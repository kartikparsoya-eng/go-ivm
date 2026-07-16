------------------------------ MODULE PullAdvance -----------------------------
(***************************************************************************)
(* Design-time model of the PROPOSED credit-gated / streaming advance --    *)
(* the "split-off" task from the 2026-07-09 decode-fusion review.  It does  *)
(* NOT model shipped code (advance is push+buffered today); it evaluates a  *)
(* DESIGN SPACE before the wire contract is frozen, so the surviving knob   *)
(* combination becomes the contract with machine-checked liveness/safety.   *)
(*                                                                         *)
(* Grounded in the current shipped facts it would change:                  *)
(*   - Version rides the FINAL frame today (advance_to_head.go:517-519):   *)
(*     "Final-frame-only metadata".  VersionCommitOnOpen models moving it.  *)
(*   - Reset is PRE-rows today (advance_to_head.go:506-509): "no           *)
(*     RowChanges".  This model allows MID-stream abort (the proposal's     *)
(*     reset-as-mid-iteration-throw).                                       *)
(*   - Budget is PRODUCE-clock today (rowplane.go:95-97): "budget checks    *)
(*     are pre-emit only".  BudgetClock models the produce-vs-wall choice.  *)
(*                                                                         *)
(* Transport mechanics (credit gate, v5 stage, FIFO TSFN queue, lowWater    *)
(* top-up) are the SAME as MODULE PullCredit, which TLC already proved.     *)
(*                                                                         *)
(* One advance = one diff stream for one CG: a SINGLE producer emitting     *)
(* NumChanges serial RowChanges (IVM order), carrying a Version, that may   *)
(* ABORT mid-stream (the economic advance budget) into a reset the client   *)
(* re-hydrates from.                                                        *)
(*                                                                         *)
(*                       ===  DESIGN KNOBS  ===                             *)
(*   BudgetClock \in {"produce","wall"}                                     *)
(*       produce: the abort clock runs only while ACTIVELY producing;       *)
(*                parking on the client pauses it (today's pre-emit rule).  *)
(*       wall   : the abort clock runs in wall time, INCLUDING while parked *)
(*                waiting on a slow client -- couples abort to drain speed. *)
(*   GateResetFrame \in BOOLEAN                                             *)
(*       TRUE : the mid-stream reset frame needs a credit (D3 violation).   *)
(*       FALSE: reset rides free (streamgate.go:6-10 free-frame rule).      *)
(*   VersionCommitOnOpen \in BOOLEAN                                        *)
(*       TRUE : client advances baseCookie on the OPENING frame (naive      *)
(*              early-version design).                                      *)
(*       FALSE: baseCookie commits only after Final + all rows (staged).    *)
(*   AdvanceStages / FlushBeforePark \in BOOLEAN                            *)
(*       whether gated advance reuses v5 staging, and whether it ports      *)
(*       acquirePullCredit's flush-before-park rule (rowplane.go:523-531).  *)
(*                                                                         *)
(*                       ===  PROPERTIES  ===                               *)
(*   RowConservation, TypeOK                       (safety, all configs)    *)
(*   VersionRowAtomicity : baseCookie committed => every row applied.       *)
(*                         The corruption guard.  Fails for commit-on-open. *)
(*   NoClientInducedReset: a reset is never triggered merely because the    *)
(*                         client is slow (the E4 reset-loop seed).         *)
(*                         Fails for wall-clock.                            *)
(*   ResetReaches        : once aborting, the client always gets the reset  *)
(*                         -- even if it stopped calling next().  D3-for-   *)
(*                         advance.  Fails for gated reset + stopped client. *)
(*   AdvanceTerminates   : the advance always reaches commit or reset.      *)
(*                         Fails for staged+gated without flush-before-park *)
(*                         (the credit-park stalemate, transferred).        *)
(***************************************************************************)
EXTENDS Naturals, Integers, Sequences, TLC

CONSTANTS
  NumChanges,          \* RowChanges in this advance's diff
  W,                   \* pull credit window
  QCap,                \* TSFN queue slots
  StageCap,            \* v5 stage hard bound
  BudgetClock,         \* "produce" | "wall"
  GateResetFrame,      \* BOOLEAN
  VersionCommitOnOpen, \* BOOLEAN
  AdvanceStages,       \* BOOLEAN
  FlushBeforePark,     \* BOOLEAN
  ClientMayStop,       \* BOOLEAN: app may stop calling next()
  MayAbort             \* BOOLEAN: the diff may hit the advance budget

ASSUME NumChanges >= 1 /\ W >= 1 /\ QCap >= 1 /\ StageCap >= 1
ASSUME BudgetClock \in {"produce", "wall"}

FINAL == 0                                       \* terminal Final frame (commit path)
RESET == -1                                      \* terminal reset frame (re-hydrate path)
LowWater == IF W \div 2 > 1 THEN W \div 2 ELSE 1

VARIABLES
  changesLeft,       \* RowChanges still to produce                 (Go)
  pc,                \* "producing"|"creditPark"|"flushPark"|"queuePark"|"aborting"|"done"
  credit,            \* gate credit                                 (Go)
  stage,             \* staged records                              (Go)
  queue,             \* FIFO of slots: Nat (row count) | FINAL | RESET
  buffered,          \* dequeued rows not yet consumed              (JS)
  consumed,          \* rows applied to the view via next()         (JS)
  granted,           \* client credit ledger                        (JS)
  versionCommitted,  \* client advanced baseCookie to this version  (JS)  <-- corruption axis
  resetEmitted,      \* Go put the reset frame on the queue         (Go)
  resetReceived,     \* client dequeued the reset frame             (JS)
  finalReceived,     \* client dequeued the Final frame             (JS)
  appStopped,        \* app never calls next() again                (JS)
  clientInducedReset \* ghost: an abort fired while parked on the client

vars == <<changesLeft, pc, credit, stage, queue, buffered, consumed, granted,
          versionCommitted, resetEmitted, resetReceived, finalReceived,
          appStopped, clientInducedReset>>

IsRow(s)     == s > 0                            \* positive = row count; 0 = Final; -1 = Reset
RECURSIVE SumQ(_)
SumQ(s) == IF s = <<>> THEN 0
           ELSE (IF Head(s) > 0 THEN Head(s) ELSE 0) + SumQ(Tail(s))
RowsProduced == NumChanges - changesLeft
RowsInQueue  == SumQ(queue)
streamOpened == RowsProduced > 0 \/ finalReceived \/ resetEmitted

--------------------------------------------------------------------------
Init ==
  /\ changesLeft       = NumChanges
  /\ pc                = "producing"
  /\ credit            = W
  /\ stage             = 0
  /\ queue             = <<>>
  /\ buffered          = 0
  /\ consumed          = 0
  /\ granted           = W
  /\ versionCommitted  = FALSE
  /\ resetEmitted      = FALSE
  /\ resetReceived     = FALSE
  /\ finalReceived     = FALSE
  /\ appStopped        = FALSE
  /\ clientInducedReset = FALSE

--------------------------------------------------------------------------
(* ======================  Go producer  ================================= *)

ProduceDirect ==
  /\ pc = "producing" /\ changesLeft > 0
  /\ credit > 0 /\ stage = 0 /\ Len(queue) < QCap
  /\ credit'       = credit - 1
  /\ queue'        = Append(queue, 1)
  /\ changesLeft'  = changesLeft - 1
  /\ UNCHANGED <<pc, stage, buffered, consumed, granted, versionCommitted,
                 resetEmitted, resetReceived, finalReceived, appStopped, clientInducedReset>>

ProduceStaged ==
  /\ AdvanceStages
  /\ pc = "producing" /\ changesLeft > 0
  /\ credit > 0 /\ stage < StageCap
  /\ (stage > 0 \/ Len(queue) = QCap)
  /\ credit'      = credit - 1
  /\ stage'       = stage + 1
  /\ changesLeft' = changesLeft - 1
  /\ UNCHANGED <<pc, queue, buffered, consumed, granted, versionCommitted,
                 resetEmitted, resetReceived, finalReceived, appStopped, clientInducedReset>>

(* No-stage transport: a full queue blocks the direct send; the producer   *)
(* parks drain-woken (NOT a credit issue -- it still holds credit).        *)
QueuePark ==
  /\ ~AdvanceStages
  /\ pc = "producing" /\ changesLeft > 0
  /\ credit > 0 /\ stage = 0 /\ Len(queue) = QCap
  /\ pc' = "queuePark"
  /\ UNCHANGED <<changesLeft, credit, stage, queue, buffered, consumed, granted,
                 versionCommitted, resetEmitted, resetReceived, finalReceived,
                 appStopped, clientInducedReset>>

QueueParkWake ==
  /\ pc = "queuePark" /\ Len(queue) < QCap
  /\ pc' = "producing"
  /\ UNCHANGED <<changesLeft, credit, stage, queue, buffered, consumed, granted,
                 versionCommitted, resetEmitted, resetReceived, finalReceived,
                 appStopped, clientInducedReset>>

FlushStage ==
  /\ AdvanceStages /\ pc \in {"producing", "flushPark"}
  /\ stage > 0 /\ Len(queue) < QCap
  /\ stage' = 0 /\ queue' = Append(queue, stage)
  /\ pc'    = "producing"
  /\ UNCHANGED <<changesLeft, credit, buffered, consumed, granted, versionCommitted,
                 resetEmitted, resetReceived, finalReceived, appStopped, clientInducedReset>>

StageHardBound ==
  /\ AdvanceStages /\ pc = "producing" /\ stage = StageCap
  /\ pc' = "flushPark"
  /\ UNCHANGED <<changesLeft, credit, stage, queue, buffered, consumed, granted,
                 versionCommitted, resetEmitted, resetReceived, finalReceived,
                 appStopped, clientInducedReset>>

FlushParkDone ==
  /\ pc = "flushPark" /\ stage = 0
  /\ pc' = "producing"
  /\ UNCHANGED <<changesLeft, credit, stage, queue, buffered, consumed, granted,
                 versionCommitted, resetEmitted, resetReceived, finalReceived,
                 appStopped, clientInducedReset>>

(* Out of credit -> park.  With staging + FlushBeforePark, flush first     *)
(* (the ported acquirePullCredit rule); otherwise park with the stage      *)
(* intact -- the stalemate shape when staged, harmless when unstaged       *)
(* (stage is always 0 without AdvanceStages).                              *)
CreditPark ==
  /\ pc = "producing" /\ changesLeft > 0 /\ credit = 0
  /\ IF AdvanceStages /\ FlushBeforePark /\ stage > 0
       THEN IF Len(queue) < QCap
              THEN /\ stage' = 0 /\ queue' = Append(queue, stage)
                   /\ pc' = "creditPark"
              ELSE /\ pc' = "flushPark" /\ UNCHANGED <<stage, queue>>
       ELSE /\ pc' = "creditPark" /\ UNCHANGED <<stage, queue>>
  /\ UNCHANGED <<changesLeft, credit, buffered, consumed, granted, versionCommitted,
                 resetEmitted, resetReceived, finalReceived, appStopped, clientInducedReset>>

GateWake ==
  /\ pc = "creditPark" /\ credit > 0
  /\ pc' = "producing"
  /\ UNCHANGED <<changesLeft, credit, stage, queue, buffered, consumed, granted,
                 versionCommitted, resetEmitted, resetReceived, finalReceived,
                 appStopped, clientInducedReset>>

(* Final frame: always rides free (baseline D3, streamgate.go:6-10).       *)
(* Stage must be flushed first (ordering) -- forced by stage = 0.          *)
EmitFinal ==
  /\ pc = "producing" /\ changesLeft = 0 /\ stage = 0 /\ Len(queue) < QCap
  /\ ~resetEmitted
  /\ queue' = Append(queue, FINAL)
  /\ pc'    = "done"
  /\ UNCHANGED <<changesLeft, credit, stage, buffered, consumed, granted,
                 versionCommitted, resetEmitted, resetReceived, finalReceived,
                 appStopped, clientInducedReset>>

(* Mid-stream abort (the advance budget).  Enabling depends on BudgetClock:*)
(*   produce: only while actively producing -> never fires while parked on  *)
(*            the client -> can never be client-induced.                    *)
(*   wall   : also while parked (credit/flush/queue) -> a slow-but-alive    *)
(*            client can trigger it -> clientInducedReset ghost trips.      *)
AbortAdvance ==
  /\ MayAbort
  /\ ~resetEmitted /\ pc # "done" /\ pc # "aborting"
  /\ IF BudgetClock = "produce" THEN pc = "producing" ELSE TRUE
  /\ pc' = "aborting"
  /\ clientInducedReset' = (clientInducedReset \/ pc \in {"creditPark","flushPark","queuePark"})
  /\ UNCHANGED <<changesLeft, credit, stage, queue, buffered, consumed, granted,
                 versionCommitted, resetEmitted, resetReceived, finalReceived, appStopped>>

(* Reset frame: free unless GateResetFrame.  Needs a queue slot.           *)
EmitReset ==
  /\ pc = "aborting" /\ ~resetEmitted
  /\ (GateResetFrame => credit > 0)
  /\ Len(queue) < QCap
  /\ credit'       = IF GateResetFrame THEN credit - 1 ELSE credit
  /\ queue'        = Append(queue, RESET)
  /\ resetEmitted' = TRUE
  /\ pc'           = "done"
  /\ UNCHANGED <<changesLeft, stage, buffered, consumed, granted, versionCommitted,
                 resetReceived, finalReceived, appStopped, clientInducedReset>>

--------------------------------------------------------------------------
(* ==========================  JS client  =============================== *)

JSDequeue ==
  /\ queue # <<>>
  /\ LET s == Head(queue) IN
       /\ queue' = Tail(queue)
       /\ IF s = FINAL
            THEN /\ finalReceived' = TRUE /\ UNCHANGED <<buffered, resetReceived>>
            ELSE IF s = RESET
            THEN /\ resetReceived' = TRUE /\ UNCHANGED <<buffered, finalReceived>>
            ELSE /\ buffered' = buffered + s /\ UNCHANGED <<finalReceived, resetReceived>>
  /\ UNCHANGED <<changesLeft, pc, credit, stage, consumed, granted,
                 versionCommitted, resetEmitted, appStopped, clientInducedReset>>

AppConsume ==
  /\ ~appStopped /\ buffered > 0
  /\ consumed' = consumed + 1
  /\ buffered' = buffered - 1
  /\ LET outstanding == granted - consumed'
     IN IF outstanding <= LowWater /\ ~finalReceived /\ ~resetReceived
        THEN /\ granted' = granted + (W - outstanding)
             /\ credit'  = credit  + (W - outstanding)
        ELSE UNCHANGED <<granted, credit>>
  /\ UNCHANGED <<changesLeft, pc, stage, queue, versionCommitted, resetEmitted,
                 resetReceived, finalReceived, appStopped, clientInducedReset>>

(* Naive early-version: baseCookie advances as soon as the stream opens,   *)
(* BEFORE the rows are drained.  The corruption the atomicity check bans.   *)
CommitVersionEarly ==
  /\ VersionCommitOnOpen
  /\ ~versionCommitted /\ ~resetEmitted /\ streamOpened
  /\ versionCommitted' = TRUE
  /\ UNCHANGED <<changesLeft, pc, credit, stage, queue, buffered, consumed, granted,
                 resetEmitted, resetReceived, finalReceived, appStopped, clientInducedReset>>

(* Staged: baseCookie advances only after Final AND all rows applied.      *)
CommitVersionFinal ==
  /\ ~VersionCommitOnOpen
  /\ ~versionCommitted /\ finalReceived /\ consumed = NumChanges /\ ~resetReceived
  /\ versionCommitted' = TRUE
  /\ UNCHANGED <<changesLeft, pc, credit, stage, queue, buffered, consumed, granted,
                 resetEmitted, resetReceived, finalReceived, appStopped, clientInducedReset>>

AppStop ==
  /\ ClientMayStop /\ ~appStopped
  /\ appStopped' = TRUE
  /\ UNCHANGED <<changesLeft, pc, credit, stage, queue, buffered, consumed, granted,
                 versionCommitted, resetEmitted, resetReceived, finalReceived, clientInducedReset>>

--------------------------------------------------------------------------
AllDone ==
  /\ pc = "done" /\ queue = <<>>
  /\ ( (versionCommitted /\ consumed = NumChanges /\ ~resetReceived)  \* clean commit
       \/ resetReceived )                                             \* reset -> re-hydrate

Next ==
  \/ ProduceDirect \/ ProduceStaged \/ QueuePark \/ QueueParkWake
  \/ FlushStage \/ StageHardBound \/ FlushParkDone
  \/ CreditPark \/ GateWake \/ EmitFinal \/ AbortAdvance \/ EmitReset
  \/ JSDequeue \/ AppConsume \/ CommitVersionEarly \/ CommitVersionFinal \/ AppStop
  \/ (AllDone /\ UNCHANGED vars)

(* Every system action is weakly fair; AbortAdvance and AppStop are NOT     *)
(* (they may or may not happen -- the environment's choice).               *)
Fairness ==
  /\ WF_vars(ProduceDirect) /\ WF_vars(ProduceStaged)
  /\ WF_vars(QueuePark) /\ WF_vars(QueueParkWake)
  /\ WF_vars(FlushStage) /\ WF_vars(StageHardBound) /\ WF_vars(FlushParkDone)
  /\ WF_vars(CreditPark) /\ WF_vars(GateWake) /\ WF_vars(EmitFinal) /\ WF_vars(EmitReset)
  /\ WF_vars(JSDequeue) /\ WF_vars(AppConsume)
  /\ WF_vars(CommitVersionEarly) /\ WF_vars(CommitVersionFinal)

Spec == Init /\ [][Next]_vars /\ Fairness

--------------------------------------------------------------------------
(* ==========================  Properties  ============================== *)

TypeOK ==
  /\ changesLeft \in 0..NumChanges
  /\ credit >= 0 /\ stage \in 0..StageCap /\ Len(queue) \in 0..QCap
  /\ buffered >= 0 /\ consumed \in 0..NumChanges /\ granted >= W
  /\ pc \in {"producing","creditPark","flushPark","queuePark","aborting","done"}
  /\ \A i \in 1..Len(queue) : queue[i] \in (-1)..StageCap

RowConservation == RowsProduced = stage + RowsInQueue + buffered + consumed

(* THE corruption guard: if the client advanced its baseCookie to this      *)
(* advance's version, it must have applied every row of the advance.        *)
VersionRowAtomicity == versionCommitted => (consumed = NumChanges)

(* THE reset-loop guard (E4): an abort is never attributable purely to a    *)
(* slow client -- only to genuine produce-side cost.                        *)
NoClientInducedReset == clientInducedReset = FALSE

(* D3-for-advance: deciding to abort MUST lead to the client receiving the  *)
(* reset -- even with the app stopped -- or the CG wedges until the sweep.  *)
ResetReaches == (pc = "aborting") ~> resetReceived

(* The advance always settles: commit or reset -- UNLESS the app stopped   *)
(* (the credit-park wedge is fundamental to pull-credit: no grants arrive    *)
(* when the consumer is gone; the 60s idle sweep is the prod backstop).      *)
(* With ClientMayStop=FALSE this reduces to strong liveness.                *)
AdvanceTerminates == (pc # "done") ~> ((pc = "done") \/ appStopped)

================================================================================
