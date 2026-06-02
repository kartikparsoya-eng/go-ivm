# H18 follow-up: channelStats over-emission with no flip

## Status

Open. Distinct from the FlippedJoin port shipped in commit
`fix(builder): wire flipped CSQs through FlippedJoin + Union path (H18)`.
That fix correctly handles `flip:true` ASTs but the production over-
emission affects a query whose AST has neither `flip` nor `scalar`.

## Observed mismatch

Shadow-mode soak on 2026-06-02, query `channelStats`:

```
[shadow] MISMATCH in batch-hydrate (k65pxbouoo4e):
  TS produced 2 changes, Go produced 3 changes
[shadow] batch-hydrate (k65pxbouoo4e): 1 changes in Go only (first 5):
  [{"type":0,"queryID":"k65pxbouoo4e","table":"channel_participants",
    "rowKey":"{\"id\":\"cp-soak-cmpmblj2z002c10zqxpdasvfd\"}"}]
```

Wire AST sent to Go (no `flip`, no `scalar`):

```json
{
  "table": "channel_stats",
  "where": {"type": "and", "conditions": [
    {"type": "simple", "op": "=",
     "left": {"type": "column", "name": "channelId"},
     "right": {"type": "literal", "value": "<CHID>"}},
    {"type": "correlatedSubquery", "op": "EXISTS",
     "related": {
       "correlation": {"parentField": ["channelId"], "childField": ["id"]},
       "subquery": {
         "table": "channels", "alias": "zsubq_channel",
         "where": {"type": "or", "conditions": [
           {"type": "simple", "op": "=",
            "left": {"type": "column", "name": "visibility"},
            "right": {"type": "literal", "value": "PUBLIC"}},
           {"type": "correlatedSubquery", "op": "EXISTS",
            "related": {
              "correlation": {"parentField": ["id"], "childField": ["channelId"]},
              "subquery": {
                "table": "channel_participants",
                "alias": "zsubq_participants",
                "where": {"type": "simple", "op": "=",
                          "left": {"type": "column", "name": "userId"},
                          "right": {"type": "literal", "value": "<UID>"}}}}}]}}}}]},
  "limit": 1
}
```

## Per-channel breakdown (single soak)

Same AST shape, varying channelId, varying outputs:

| channelId                       | TS  | Go  | Match? |
|---------------------------------|-----|-----|--------|
| `cmpmblj2z002c10zqxpdasvfd`     | 2   | 3   | ✗      |
| `cmpmjrgn900bc10zqzcsdjxs8`     | 3   | 3   | ✓      |
| `cmpmjimyo00a110zqzd2u18dp`     | 2   | 3   | ✗      |
| `cmp2cqlq900f7iphvij992i5e`     | 3   | 3   | ✓      |

TS's emission of `channel_participants` is **data-dependent** —
specifically conditional on which OR branch satisfied the inner
`channels` WHERE. Go always emits the participant whenever the inner
EXISTS-join attached the relationship, regardless of which OR branch
matched.

## Working hypothesis

TS's filter chain (`Filter` + `FanOut` + branch `Exists` + `FanIn` +
`FilterEnd`) preserves branch identity through the merge so the
downstream streamer only sees the relationship from the branch that
actually selected the row. When the `visibility=PUBLIC` branch alone
selects the channel, the `Exists(participants)` branch never
contributes to the output, so its attached relationship is dropped.

Go's `FanIn` (regular variant, separate from `UnionFanIn`) currently
forwards the node with whatever relationships the upstream Join
attached. There's no branch-awareness in the merge.

This is *not* the same as the `UnionFanIn` merge-with-dedup path that
the FlippedJoin port wired up — that one operates on `Input` outputs
for the flip case. The bug here is in the `Filter`-chain `FanIn`
inside the regular EXISTS-OR path.

## Reproducer status

`engine/channelstats_overemit_repro_test.go` mirrors the shape exactly
(with `Flip:true` on both CSQs, matching the FlippedJoin fix's
intent). The test **passes** because:

- With `Flip:true`: routes through the new FlippedJoin path → 2 ✓
- Without `Flip:true` (matching production): Go's normal path
  produces 2 for the minimal test data ✓

Neither variant reproduces the production over-emit. The bug is
data-dependent and the test data is too uniform — likely needs:

- A channel that is *both* PUBLIC *and* has a matching participant
  (the over-emit case)
- vs. a channel that is *only* PUBLIC or *only* has a participant
  (which TS and Go agree on)

## Investigation outline

1. Trace TS's filter chain end-to-end for the OR-EXISTS case. Where
   does branch identity get preserved through `FanIn` →
   `FilterEnd` → streamer?
2. Compare to Go's `ivm/fan_in.go`. Is there a missing branch-tag or
   per-branch relationship masking?
3. Construct a reproducer with the data shape above (one PUBLIC+has-
   participant channel) and add it to `engine/`.
4. Port whatever TS does (branch-mask, per-branch override, or
   alternative operator) to Go.

## Related code (Go side)

- `go-ivm/builder/builder.go:applyOr` — builds FanOut + branches +
  FanIn for OR with CSQ.
- `go-ivm/ivm/fan_in.go` — `FanIn.Push` / `Fetch`.
- `go-ivm/engine/streamer.go:streamNodes` — recursion through
  `node.Relationships`. Currently unconditional except for
  `IsScalar` / `system=permissions`.

## Related code (TS side)

- `mono/packages/zql/src/builder/builder.ts:applyOr` (line ~524).
- `mono/packages/zql/src/ivm/fan-in.ts` — `FanIn` (FilterOperator).
- `mono/packages/zql/src/ivm/exists.ts` — `Exists.filter`.
- `mono/packages/zero-cache/src/services/view-syncer/pipeline-driver.ts:#streamNodes` (line ~3170).
