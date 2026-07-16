/-!
A small model of Go IVM parallel fanout serialization.

The production shape this models is:

* each query group has an ordered local program of row/chunk emissions;
* the parallel scheduler may choose any group with remaining work;
* choosing a group emits only that group's next item;
* a lock/transport boundary records the chosen emissions as one serial trace.

The theorem is intentionally query-local. It proves that every complete
parallel trace has the same per-group projection as the original program, and
therefore any query-local consumer sees the same state as it would under any
other complete execution, including a strict serial fallback.

It does not prove that the global wire order equals the TS serial order. The Go
implementation deliberately permits cross-query interleaving; downstream
correctness relies on demultiplexing by queryID and preserving order within a
query.
-/

namespace FanoutSerializability

structure Event (Group Op : Type) where
  group : Group
  op : Op
deriving Repr

variable {Group Op : Type} [DecidableEq Group]

abbrev Program (Group Op : Type) := Group → List Op

def update (p : Program Group Op) (g : Group) (rest : List Op) :
    Program Group Op :=
  fun h => if h = g then rest else p h

def project (g : Group) : List (Event Group Op) → List Op
  | [] => []
  | e :: es =>
      if e.group = g then
        e.op :: project g es
      else
        project g es

inductive TraceFrom :
    Program Group Op → Program Group Op → List (Event Group Op) → Prop where
  | done (p : Program Group Op) : TraceFrom p p []
  | step
      {p q : Program Group Op}
      {tr : List (Event Group Op)}
      {g : Group}
      {op : Op}
      {rest : List Op}
      (head : p g = op :: rest)
      (tail : TraceFrom (update p g rest) q tr) :
      TraceFrom p q ({ group := g, op := op } :: tr)

def Done (p : Program Group Op) : Prop :=
  ∀ g, p g = []

def ObservationallyEquivalent
    (left right : List (Event Group Op)) : Prop :=
  ∀ g, project g left = project g right

theorem trace_preserves_group_order
    {p q : Program Group Op}
    {tr : List (Event Group Op)}
    (h : TraceFrom p q tr) :
    ∀ g, project g tr ++ q g = p g := by
  induction h with
  | done p =>
      intro g
      rfl
  | step head _tail ih =>
      intro g
      rename_i p q tr g0 op rest
      by_cases same : g0 = g
      · subst g
        have htail : project g0 tr ++ q g0 = rest := by
          have hgroup := ih g0
          simpa [update] using hgroup
        calc
          project g0 ({ group := g0, op := op } :: tr) ++ q g0
              = op :: (project g0 tr ++ q g0) := by
                simp [project]
          _ = op :: rest := by
                rw [htail]
          _ = p g0 := by
                exact head.symm
      · have htail : project g tr ++ q g = p g := by
          have hgroup := ih g
          have notSame : ¬ g = g0 := by
            intro h
            exact same h.symm
          simpa [update, notSame] using hgroup
        calc
          project g ({ group := g0, op := op } :: tr) ++ q g
              = project g tr ++ q g := by
                simp [project, same]
          _ = p g := htail

theorem complete_trace_matches_program
    {p q : Program Group Op}
    {tr : List (Event Group Op)}
    (h : TraceFrom p q tr)
    (done : Done q) :
    ∀ g, project g tr = p g := by
  intro g
  have horder := trace_preserves_group_order h g
  rw [done g, List.append_nil] at horder
  exact horder

theorem complete_traces_observationally_equivalent
    {p q₁ q₂ : Program Group Op}
    {tr₁ tr₂ : List (Event Group Op)}
    (h₁ : TraceFrom p q₁ tr₁)
    (done₁ : Done q₁)
    (h₂ : TraceFrom p q₂ tr₂)
    (done₂ : Done q₂) :
    ObservationallyEquivalent tr₁ tr₂ := by
  intro g
  rw [complete_trace_matches_program h₁ done₁ g,
      complete_trace_matches_program h₂ done₂ g]

theorem query_local_consumer_same
    {State : Type}
    (consume : List Op → State)
    {p q₁ q₂ : Program Group Op}
    {parallelTrace serialTrace : List (Event Group Op)}
    (hParallel : TraceFrom p q₁ parallelTrace)
    (doneParallel : Done q₁)
    (hSerial : TraceFrom p q₂ serialTrace)
    (doneSerial : Done q₂) :
    ∀ g,
      consume (project g parallelTrace) =
        consume (project g serialTrace) := by
  intro g
  rw [complete_trace_matches_program hParallel doneParallel g,
      complete_trace_matches_program hSerial doneSerial g]

theorem contention_restriction_preserves_group_order
    (AllowedByContention : List (Event Group Op) → Prop)
    {p q : Program Group Op}
    {tr : List (Event Group Op)}
    (_allowed : AllowedByContention tr)
    (h : TraceFrom p q tr)
    (done : Done q) :
    ∀ g, project g tr = p g :=
  complete_trace_matches_program h done

end FanoutSerializability
