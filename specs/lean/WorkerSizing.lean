/-!
A small capacity model for sizing Zero sync workers.

This is deliberately a *measurement-parameterized* model. Lean proves the
worker-count inequalities and monotonicity. It does not invent the constants:
`residentPerWorker`, `activePerWorker`, `drainCostPerActive`, and
`loopCapacity` must come from production metrics or ladder experiments.

The model captures the distinction that mattered in E5:

* resident CGs create memory/state ownership pressure;
* active CGs create JS-loop drain pressure;
* workers buy inter-CG parallelism by splitting CGs across independent loops;
* if many resident CGs become active at once, the active bound dominates.
-/

namespace WorkerSizing

structure Demand where
  resident : Nat
  active : Nat
deriving Repr, DecidableEq

structure Comfort where
  residentPerWorker : Nat
  activePerWorker : Nat
deriving Repr, DecidableEq

structure DrainBudget where
  drainCostPerActive : Nat
  loopCapacity : Nat
deriving Repr, DecidableEq

structure WorkerLoad where
  resident : Nat
  active : Nat
deriving Repr, DecidableEq

/-- Count-based comfort envelope: enough workers for resident and active CGs. -/
def Comfortable (d : Demand) (workers : Nat) (c : Comfort) : Prop :=
  d.resident ≤ workers * c.residentPerWorker ∧
    d.active ≤ workers * c.activePerWorker

/--
Global active drain capacity. This is a necessary aggregate condition; the
per-worker theorem below adds the balance requirement.
-/
def ActiveCapacityOK (d : Demand) (workers : Nat) (b : DrainBudget) : Prop :=
  d.active * b.drainCostPerActive ≤ workers * b.loopCapacity

/-- A single worker is safe when both count bounds and drain budget hold. -/
def WorkerSafe (load : WorkerLoad) (c : Comfort) (b : DrainBudget) : Prop :=
  load.resident ≤ c.residentPerWorker ∧
    load.active ≤ c.activePerWorker ∧
      load.active * b.drainCostPerActive ≤ b.loopCapacity

def AllWorkersSafe
    (loads : List WorkerLoad)
    (c : Comfort)
    (b : DrainBudget) : Prop :=
  ∀ load ∈ loads, WorkerSafe load c b

theorem comfortable_of_bounds
    {d : Demand}
    {workers : Nat}
    {c : Comfort}
    (residentBound : d.resident ≤ workers * c.residentPerWorker)
    (activeBound : d.active ≤ workers * c.activePerWorker) :
    Comfortable d workers c := by
  exact And.intro residentBound activeBound

theorem comfortable_resident_bound
    {d : Demand}
    {workers : Nat}
    {c : Comfort}
    (h : Comfortable d workers c) :
    d.resident ≤ workers * c.residentPerWorker :=
  h.left

theorem comfortable_active_bound
    {d : Demand}
    {workers : Nat}
    {c : Comfort}
    (h : Comfortable d workers c) :
    d.active ≤ workers * c.activePerWorker :=
  h.right

/--
Adding workers cannot make a previously comfortable configuration
uncomfortable, assuming the same per-worker limits.
-/
theorem comfortable_mono_workers
    {d : Demand}
    {workers₁ workers₂ : Nat}
    {c : Comfort}
    (h : Comfortable d workers₁ c)
    (moreWorkers : workers₁ ≤ workers₂) :
    Comfortable d workers₂ c := by
  constructor
  · exact Nat.le_trans h.left
      (Nat.mul_le_mul_right c.residentPerWorker moreWorkers)
  · exact Nat.le_trans h.right
      (Nat.mul_le_mul_right c.activePerWorker moreWorkers)

theorem active_capacity_mono_workers
    {d : Demand}
    {workers₁ workers₂ : Nat}
    {b : DrainBudget}
    (h : ActiveCapacityOK d workers₁ b)
    (moreWorkers : workers₁ ≤ workers₂) :
    ActiveCapacityOK d workers₂ b := by
  exact Nat.le_trans h (Nat.mul_le_mul_right b.loopCapacity moreWorkers)

/--
If active demand exceeds `workers * activePerWorker`, the configuration is
outside the comfort envelope. This is the formal version of: if all resident
CGs become hot, the active side can dominate and require more workers.
-/
theorem not_comfortable_if_active_over
    {d : Demand}
    {workers : Nat}
    {c : Comfort}
    (over : workers * c.activePerWorker < d.active) :
    ¬ Comfortable d workers c := by
  intro h
  exact Nat.not_lt_of_ge h.right over

theorem not_comfortable_if_resident_over
    {d : Demand}
    {workers : Nat}
    {c : Comfort}
    (over : workers * c.residentPerWorker < d.resident) :
    ¬ Comfortable d workers c := by
  intro h
  exact Nat.not_lt_of_ge h.left over

/--
If aggregate active demand exceeds aggregate loop capacity, some worker must
be over budget in any balanced implementation. This theorem states the
necessary aggregate failure condition; the actual assignment/balance policy is
modeled by `AllWorkersSafe` below.
-/
theorem not_active_capacity_if_total_over
    {d : Demand}
    {workers : Nat}
    {b : DrainBudget}
    (over : workers * b.loopCapacity < d.active * b.drainCostPerActive) :
    ¬ ActiveCapacityOK d workers b := by
  intro h
  exact Nat.not_lt_of_ge h over

/--
Per-worker safety from local bounds. This is the theorem that corresponds to
"route CGs across workers so no loop owns more than the measured comfortable
resident/active limit".
-/
theorem worker_safe_of_local_bounds
    {load : WorkerLoad}
    {c : Comfort}
    {b : DrainBudget}
    (residentBound : load.resident ≤ c.residentPerWorker)
    (activeBound : load.active ≤ c.activePerWorker)
    (activeLimitWithinLoop :
      c.activePerWorker * b.drainCostPerActive ≤ b.loopCapacity) :
    WorkerSafe load c b := by
  constructor
  · exact residentBound
  · constructor
    · exact activeBound
    · exact Nat.le_trans
        (Nat.mul_le_mul_right b.drainCostPerActive activeBound)
        activeLimitWithinLoop

theorem all_workers_safe_of_each_bound
    {loads : List WorkerLoad}
    {c : Comfort}
    {b : DrainBudget}
    (residentBound :
      ∀ load ∈ loads, load.resident ≤ c.residentPerWorker)
    (activeBound :
      ∀ load ∈ loads, load.active ≤ c.activePerWorker)
    (activeLimitWithinLoop :
      c.activePerWorker * b.drainCostPerActive ≤ b.loopCapacity) :
    AllWorkersSafe loads c b := by
  intro load inLoads
  exact worker_safe_of_local_bounds
    (residentBound load inLoads)
    (activeBound load inLoads)
    activeLimitWithinLoop

/-! Concrete checks using the current measured envelope.

These are not universal constants. They pin the arithmetic behind the current
prod recommendation:

* about 100 resident CGs per worker was comfortable in E5;
* about 4 active CGs per worker is the conservative active-load envelope from
  the observed prod-shaped trace;
* W=4 covers R=350, A=11;
* W=4 does not cover the hypothetical "all 350 resident CGs are hot" case
  under that same active envelope.
-/

def currentProdHigh : Demand :=
  { resident := 350, active := 11 }

def measuredComfort : Comfort :=
  { residentPerWorker := 100, activePerWorker := 4 }

example : Comfortable currentProdHigh 4 measuredComfort := by
  unfold Comfortable currentProdHigh measuredComfort
  decide

example : ¬ Comfortable { resident := 350, active := 350 } 4 measuredComfort := by
  unfold Comfortable measuredComfort
  decide

example : Comfortable { resident := 301, active := 11 } 3
    { residentPerWorker := 101, activePerWorker := 4 } := by
  unfold Comfortable
  decide

end WorkerSizing
