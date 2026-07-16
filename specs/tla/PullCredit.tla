------------------------------ MODULE PullCredit ------------------------------
(***************************************************************************)
(* Model of the go-ivm ABI v3 pull-credit protocol + v5 staging, as        *)
(* implemented in:                                                         *)
(*                                                                         *)
(*   go-ivm/cmd/sidecar/streamgate.go    gate: acquire/tryAcquire/grant    *)
(*   go-ivm/cmd/sidecar/rowplane.go      stage, sendOrStage, flushStage,   *)
(*                                       acquirePullCredit (:523-531)      *)
(*   mono/.../go-sidecar/go-ivm-client.ts:987-1104                         *)
(*                                       consumer: lowWater top-up rule    *)
(*                                                                         *)
(* One pull-mode addQueriesStream RPC: NLanes producer goroutines (one     *)
(* per query, engine.go AddQueriesStreamPull) share ONE streamGate.  Each  *)
(* lane produces RowsPerLane row records, then one terminal Final frame.   *)
(* Row-bearing deliveries consume one credit; terminal frames ride free    *)
(* iff GateFinal = FALSE (the D3 rule, streamgate.go:6-10).                *)
(*                                                                         *)
(* Deliveries enter a bounded TSFN queue (QCap slots).  When the queue is  *)
(* full a record is STAGED (v5); a whole-stage flush ships the ENTIRE      *)
(* stage as ONE kind-5 batch slot.  Producers parked on the full queue     *)
(* are woken by the drain signal (JS dequeue); producers parked on the     *)
(* CREDIT gate are woken only by grant()/cancel().  That wakeup asymmetry  *)
(* is the heart of the 2026-07-10 credit-park stalemate.                   *)
(*                                                                         *)
(* JS consumer (go-ivm-client.ts:1094-1104): each consumed row-bearing     *)
(* entry increments `consumed`; when outstanding = granted - consumed      *)
(* falls to lowWater = max(1, W div 2), it grants topUp = W - outstanding. *)
(*                                                                         *)
(* Abstractions (all sound for the bugs under test):                       *)
(*  - grant delivery is atomic (real streamCredit is an in-flight RPC; the *)
(*    delay only ADDS stall windows, it cannot remove the deadlocks).      *)
(*  - queue slot composition is aggregate counts; a dequeue may carry any  *)
(*    1..RowsInFlight rows (superset of real batch sizes).                 *)
(*  - cancel() and the 60s idle sweep are NOT modeled: they are exactly    *)
(*    the backstops whose necessity these bugs demonstrate.  A deadlock    *)
(*    here = "wedged until sweep" in production.                           *)
(*                                                                         *)
(* Knobs:                                                                  *)
(*   FlushBeforePark : acquirePullCredit's flush-stage-before-gate.acquire *)
(*                     (rowplane.go:523-531).  FALSE = pre-fix v5 design.  *)
(*   GateFinal       : TRUE makes terminal frames need credit (violates    *)
(*                     the D3 free-frame rule).                            *)
(*   ClientMayStop   : enables AppStop -- the app stops calling next()     *)
(*                     (consumer error path go-ivm-client.ts:1099 stops    *)
(*                     grants; view-syncer awaiting a stuck downstream     *)
(*                     between pulls, pipeline-driver.ts:1471-80).         *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
  NLanes,          \* producer goroutines sharing the gate
  RowsPerLane,     \* row records each lane must deliver
  W,               \* pullWindow: opening credits ride registration (streamgate.go:185-192)
  QCap,            \* TSFN queue capacity (prod 8192; model: small)
  StageCap,        \* stage hard bound stageMaxRecords (rowplane.go:471)
  FlushBeforePark, \* BOOLEAN: the 2026-07-10 fix on/off
  GateFinal,       \* BOOLEAN: TRUE = D3 violation
  ClientMayStop    \* BOOLEAN: enable the AppStop action

ASSUME NLanes >= 1 /\ RowsPerLane >= 0 /\ W >= 1 /\ QCap >= 1 /\ StageCap >= 1

LowWater == IF W \div 2 > 1 THEN W \div 2 ELSE 1  \* max(1, W/2)  go-ivm-client.ts:987

Lanes == 1..NLanes

VARIABLES
  rowsLeft,   \* [lane -> rows still to produce]                     (Go)
  lanePC,     \* [lane -> "produce"|"creditPark"|"flushPark"|"done"] (Go)
  credit,     \* gate.credit                    streamgate.go:44     (Go)
  stage,      \* staged records (count)         rowplane.go          (Go)
  queue,      \* TSFN queue: FIFO seq of slots; slot = rows carried (0 = final frame)
  buffered,   \* dequeued row entries not yet consumed by the app    (JS)
  finalsRcvd, \* terminal frames dequeued                            (JS)
  granted,    \* client ledger: W + top-ups     go-ivm-client.ts:1004(JS)
  consumed,   \* rows consumed via next()       go-ivm-client.ts:1097(JS)
  appStopped  \* app never calls next() again                        (JS)

vars == <<rowsLeft, lanePC, credit, stage, queue, buffered, finalsRcvd,
          granted, consumed, appStopped>>

RECURSIVE SumRows(_)
SumRows(S) == IF S = {} THEN 0
              ELSE LET x == CHOOSE e \in S : TRUE
                   IN rowsLeft[x] + SumRows(S \ {x})

RECURSIVE SumSeq(_)
SumSeq(s) == IF s = <<>> THEN 0 ELSE Head(s) + SumSeq(Tail(s))

queueLen     == Len(queue)
RowsProduced == NLanes * RowsPerLane - SumRows(Lanes)
DoneLanes    == Cardinality({l \in Lanes : lanePC[l] = "done"})
RowsInQueue  == SumSeq(queue)

--------------------------------------------------------------------------
Init ==
  /\ rowsLeft   = [l \in Lanes |-> RowsPerLane]
  /\ lanePC     = [l \in Lanes |-> "produce"]
  /\ credit     = W                             \* opening window rides registration
  /\ stage      = 0
  /\ queue      = <<>>
  /\ buffered   = 0
  /\ finalsRcvd = 0
  /\ granted    = W
  /\ consumed   = 0
  /\ appStopped = FALSE

--------------------------------------------------------------------------
(* ======================  Go producer actions  ========================= *)

(* sendOrStageLocked (rowplane.go:457-475): stage empty & queue has room  *)
(* -> direct deliver; otherwise stage the record.  One credit per row     *)
(* (tryAcquire fast path, rowplane.go:524).                               *)

RowDirect(l) ==
  /\ lanePC[l] = "produce" /\ rowsLeft[l] > 0
  /\ credit > 0 /\ stage = 0 /\ queueLen < QCap
  /\ credit'   = credit - 1
  /\ queue'    = Append(queue, 1)               \* one direct row slot
  /\ rowsLeft' = [rowsLeft EXCEPT ![l] = @ - 1]
  /\ UNCHANGED <<lanePC, stage, buffered, finalsRcvd, granted, consumed, appStopped>>

RowStaged(l) ==
  /\ lanePC[l] = "produce" /\ rowsLeft[l] > 0
  /\ credit > 0 /\ stage < StageCap
  /\ (stage > 0 \/ queueLen = QCap)             \* why direct wasn't possible
  /\ credit'   = credit - 1
  /\ stage'    = stage + 1
  /\ rowsLeft' = [rowsLeft EXCEPT ![l] = @ - 1]
  /\ UNCHANGED <<lanePC, queue, buffered, finalsRcvd, granted, consumed, appStopped>>

(* Stage hard bound: the producer block-flushes (rowplane.go:471-473),    *)
(* parking drain-woken until a slot frees.                                *)
StageHardBound(l) ==
  /\ lanePC[l] = "produce" /\ stage = StageCap
  /\ lanePC' = [lanePC EXCEPT ![l] = "flushPark"]
  /\ UNCHANGED <<rowsLeft, credit, stage, queue, buffered, finalsRcvd,
                 granted, consumed, appStopped>>

(* Whole-stage flush: entire stage -> ONE kind-5 batch slot               *)
(* (rowplane.go:375-388, 418-423).  Enabled for producing or flush-parked *)
(* lanes once the queue has room -- the drain wakeup.  NOTE: a lane in    *)
(* "creditPark" can NEVER take this action; that asymmetry is the bug.    *)
FlushStage(l) ==
  /\ lanePC[l] \in {"produce", "flushPark"}
  /\ stage > 0 /\ queueLen < QCap
  /\ stage'    = 0
  /\ queue'    = Append(queue, stage)           \* whole stage -> ONE batch slot
  /\ lanePC'   = [lanePC EXCEPT ![l] = "produce"]
  /\ UNCHANGED <<rowsLeft, credit, buffered, finalsRcvd, granted, consumed, appStopped>>

(* Multi-parker wake re-check (rowplane.go:405-416): a flush-parked lane  *)
(* whose stage was shipped by a SIBLING finds len(stage)==0 on wake and   *)
(* returns -- "this parker's work is done".  Without this transition a    *)
(* sibling flush strands the parker forever (TLC found exactly that hang  *)
(* on the first draft of this spec, independently re-deriving why the     *)
(* re-check loop exists).                                                 *)
FlushParkDone(l) ==
  /\ lanePC[l] = "flushPark" /\ stage = 0
  /\ lanePC' = [lanePC EXCEPT ![l] = "produce"]
  /\ UNCHANGED <<rowsLeft, credit, stage, queue, buffered, finalsRcvd,
                 granted, consumed, appStopped>>

(* Out of credit -> acquirePullCredit (rowplane.go:523-531).              *)
(* Fixed: flush the stage BEFORE parking on the gate (if the flush must   *)
(* itself wait for queue room, the lane waits as a FLUSH parker -- drain- *)
(* woken -- and only parks on the gate once the stage has shipped).       *)
CreditParkFixed(l) ==
  /\ FlushBeforePark
  /\ lanePC[l] = "produce" /\ rowsLeft[l] > 0 /\ credit = 0
  /\ IF stage = 0
       THEN /\ lanePC' = [lanePC EXCEPT ![l] = "creditPark"]  \* gate.acquire
            /\ UNCHANGED <<stage, queue>>
       ELSE IF queueLen < QCap
       THEN /\ stage' = 0 /\ queue' = Append(queue, stage)    \* flushStage()
            /\ lanePC' = [lanePC EXCEPT ![l] = "creditPark"]  \* then gate.acquire
       ELSE /\ lanePC' = [lanePC EXCEPT ![l] = "flushPark"]   \* flush parks first
            /\ UNCHANGED <<stage, queue>>
  /\ UNCHANGED <<rowsLeft, credit, buffered, finalsRcvd, granted, consumed, appStopped>>

(* Pre-fix: gate.acquire() directly -- the stage stays behind, invisible  *)
(* to the client's ledger ("granted-but-undelivered").                    *)
CreditParkPreFix(l) ==
  /\ ~FlushBeforePark
  /\ lanePC[l] = "produce" /\ rowsLeft[l] > 0 /\ credit = 0
  /\ lanePC' = [lanePC EXCEPT ![l] = "creditPark"]
  /\ UNCHANGED <<rowsLeft, credit, stage, queue, buffered, finalsRcvd,
                 granted, consumed, appStopped>>

GateWake(l) ==                                  \* grant() broadcast, streamgate.go:116
  /\ lanePC[l] = "creditPark" /\ credit > 0
  /\ lanePC' = [lanePC EXCEPT ![l] = "produce"]
  /\ UNCHANGED <<rowsLeft, credit, stage, queue, buffered, finalsRcvd,
                 granted, consumed, appStopped>>

(* Terminal frame.  Ordering: never overtakes staged rows (deliverFrame's *)
(* flush-first rule) -> requires stage = 0; a stage>0 lane flushes first  *)
(* via FlushStage.  Free-frame rule: no credit needed unless GateFinal.   *)
(* Transport: one queue slot (park drain-woken while full -- implicit:    *)
(* the action stays disabled until JSDequeue frees a slot).               *)
EmitFinal(l) ==
  /\ lanePC[l] = "produce" /\ rowsLeft[l] = 0
  /\ stage = 0 /\ queueLen < QCap
  /\ IF GateFinal THEN credit > 0 ELSE TRUE
  /\ credit'   = IF GateFinal THEN credit - 1 ELSE credit
  /\ queue'    = Append(queue, 0)               \* final frame slot (0 rows)
  /\ lanePC'   = [lanePC EXCEPT ![l] = "done"]
  /\ UNCHANGED <<rowsLeft, stage, buffered, finalsRcvd, granted, consumed, appStopped>>

(* Under GateFinal, a lane out of rows with no credit parks on the GATE   *)
(* -- the D3 violation shape: it now needs client DEMAND to terminate.    *)
GatedFinalPark(l) ==
  /\ GateFinal
  /\ lanePC[l] = "produce" /\ rowsLeft[l] = 0 /\ credit = 0
  /\ lanePC' = [lanePC EXCEPT ![l] = "creditPark"]
  /\ UNCHANGED <<rowsLeft, credit, stage, queue, buffered, finalsRcvd,
                 granted, consumed, appStopped>>

(* A credit-parked lane whose rows are done emits its final on wake.      *)
GatedFinalWake(l) ==
  /\ GateFinal
  /\ lanePC[l] = "creditPark" /\ rowsLeft[l] = 0
  /\ credit > 0 /\ stage = 0 /\ queueLen < QCap
  /\ credit'   = credit - 1
  /\ queue'    = Append(queue, 0)
  /\ lanePC'   = [lanePC EXCEPT ![l] = "done"]
  /\ UNCHANGED <<rowsLeft, stage, buffered, finalsRcvd, granted, consumed, appStopped>>

--------------------------------------------------------------------------
(* ========================  JS-side actions  =========================== *)

(* TSFN dequeue runs on the JS event loop INDEPENDENTLY of the app's      *)
(* next() loop (go-ivm-client #handleDelivery): the HEAD slot drains in   *)
(* FIFO order (the rowplane ordering invariant); its rows land in         *)
(* `buffered` (a kind-5 batch fans out into per-row entries,              *)
(* go-ivm-client iterateBatch), finals are recorded, and the drain        *)
(* signal re-enables flush-parked producers.                              *)

JSDequeue ==
  /\ queue # <<>>
  /\ LET slot == Head(queue) IN
       /\ queue' = Tail(queue)
       /\ IF slot = 0
            THEN /\ finalsRcvd' = finalsRcvd + 1
                 /\ UNCHANGED buffered
            ELSE /\ buffered' = buffered + slot
                 /\ UNCHANGED finalsRcvd
  /\ UNCHANGED <<rowsLeft, lanePC, credit, stage, granted, consumed, appStopped>>

(* App consumes one row entry -- THE top-up rule, verbatim from           *)
(* go-ivm-client.ts:1096-1103: consumed++; outstanding=granted-consumed;  *)
(* if outstanding <= lowWater && !done && error==null:                    *)
(*   topUp = window - outstanding; granted += topUp; streamCredit(topUp). *)
AppConsume ==
  /\ ~appStopped /\ buffered > 0
  /\ consumed' = consumed + 1
  /\ buffered' = buffered - 1
  /\ LET outstanding == granted - consumed'
     IN IF outstanding <= LowWater /\ finalsRcvd < NLanes
        THEN /\ granted' = granted + (W - outstanding)
             /\ credit'  = credit  + (W - outstanding)  \* streamCredit -> gate.grant
        ELSE UNCHANGED <<granted, credit>>
  /\ UNCHANGED <<rowsLeft, lanePC, stage, queue, finalsRcvd, appStopped>>

(* The app stops calling next() forever.  Dequeue continues (event loop   *)
(* alive); only consumption -- and therefore GRANTING -- stops.           *)
AppStop ==
  /\ ClientMayStop /\ ~appStopped
  /\ appStopped' = TRUE
  /\ UNCHANGED <<rowsLeft, lanePC, credit, stage, queue, buffered,
                 finalsRcvd, granted, consumed>>

--------------------------------------------------------------------------
(* Legitimate end state: every lane delivered its final, everything       *)
(* drained and consumed.  Explicit stutter so TLC's deadlock check only   *)
(* fires on REAL wedges.                                                  *)
AllDone ==
  /\ \A l \in Lanes : lanePC[l] = "done"
  /\ finalsRcvd = NLanes /\ queue = <<>> /\ stage = 0
  /\ (buffered = 0 \/ appStopped)

Next ==
  \/ \E l \in Lanes :
       RowDirect(l) \/ RowStaged(l) \/ StageHardBound(l) \/ FlushStage(l)
       \/ FlushParkDone(l)
       \/ CreditParkFixed(l) \/ CreditParkPreFix(l) \/ GateWake(l)
       \/ EmitFinal(l) \/ GatedFinalPark(l) \/ GatedFinalWake(l)
  \/ JSDequeue \/ AppConsume \/ AppStop
  \/ (AllDone /\ UNCHANGED vars)

(* Weak fairness on every system action (the event loop dequeues, a       *)
(* healthy app consumes, producers run).  AppStop is deliberately UNFAIR: *)
(* it may or may not ever happen.                                         *)
Fairness ==
  /\ WF_vars(JSDequeue) /\ WF_vars(AppConsume)
  /\ \A l \in Lanes :
       /\ WF_vars(RowDirect(l)) /\ WF_vars(RowStaged(l))
       /\ WF_vars(StageHardBound(l)) /\ WF_vars(FlushStage(l))
       /\ WF_vars(FlushParkDone(l))
       /\ WF_vars(CreditParkFixed(l)) /\ WF_vars(CreditParkPreFix(l))
       /\ WF_vars(GateWake(l)) /\ WF_vars(EmitFinal(l))
       /\ WF_vars(GatedFinalPark(l)) /\ WF_vars(GatedFinalWake(l))

Spec == Init /\ [][Next]_vars /\ Fairness

--------------------------------------------------------------------------
(* ==========================  Properties  ============================== *)

TypeOK ==
  /\ credit >= 0
  /\ stage \in 0..StageCap
  /\ Len(queue) \in 0..QCap
  /\ \A i \in 1..Len(queue) : queue[i] \in 0..StageCap
  /\ buffered >= 0 /\ consumed >= 0
  /\ granted >= W
  /\ finalsRcvd \in 0..NLanes

(* Conservation: every produced row is in exactly one place.  (The        *)
(* aggregate-count first draft of this spec violated this silently --     *)
(* a dequeue could "lose" rows -- producing a fake deadlock; the FIFO     *)
(* queue makes it a checkable invariant.)                                 *)
RowConservation ==
  RowsProduced = stage + RowsInQueue + buffered + consumed

(* The pull contract (go-ivm-client-pull-unit.test.ts:28): Go never       *)
(* delivers more rows than the client granted.                            *)
NeverOverProduce == RowsProduced <= granted

(* Ledger consistency: gate credit == client grants - credits spent.      *)
CreditAccounting ==
  credit = granted - RowsProduced - (IF GateFinal THEN DoneLanes ELSE 0)

(* Liveness (healthy client): every lane eventually delivers its Final.   *)
AllLanesTerminate == <>(\A l \in Lanes : lanePC[l] = "done")

(* The D3 rationale, checkable: a lane that has produced all its rows     *)
(* reaches done WITHOUT further app demand -- even if the app never       *)
(* calls next() again.                                                    *)
TerminalNeedsNoDemand ==
  \A l \in Lanes : (rowsLeft[l] = 0) ~> (lanePC[l] = "done")

================================================================================
