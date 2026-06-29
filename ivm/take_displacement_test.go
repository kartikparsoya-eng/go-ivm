package ivm

// Bug 26: Take pushAddChange missing displacement on message INSERT
// with LIMIT + DESC sort after sustained mutations.
//
// The test exercises:
// 1. DESC sort with LIMIT 10
// 2. Sustained edit mutations changing the sort key (createdAt)
// 3. INSERT (ADD) mutations that should trigger displacement
// 4. Both MemoryTakeStorage and simulated JSON round-trip storage

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"
)

// jsonRoundTripStorage wraps MemoryTakeStorage but JSON-serializes/deserializes
// all state, simulating the SQLiteTakeStorage behavior.
type jsonRoundTripStorage struct {
	inner *MemoryTakeStorage
}

func newJSONRoundTripStorage() *jsonRoundTripStorage {
	return &jsonRoundTripStorage{inner: NewMemoryTakeStorage()}
}

func (s *jsonRoundTripStorage) GetTakeState(key string) *TakeState {
	st := s.inner.GetTakeState(key)
	if st == nil {
		return nil
	}
	// JSON round-trip
	data, err := json.Marshal(st)
	if err != nil {
		panic(err)
	}
	var result TakeState
	if err := json.Unmarshal(data, &result); err != nil {
		panic(err)
	}
	return &result
}

func (s *jsonRoundTripStorage) SetTakeState(key string, state TakeState) {
	// JSON round-trip before storing
	data, err := json.Marshal(state)
	if err != nil {
		panic(err)
	}
	var rt TakeState
	if err := json.Unmarshal(data, &rt); err != nil {
		panic(err)
	}
	s.inner.SetTakeState(key, rt)
}

func (s *jsonRoundTripStorage) GetMaxBound() Row {
	row := s.inner.GetMaxBound()
	if row == nil {
		return nil
	}
	data, err := json.Marshal(row)
	if err != nil {
		panic(err)
	}
	var result Row
	if err := json.Unmarshal(data, &result); err != nil {
		panic(err)
	}
	return result
}

func (s *jsonRoundTripStorage) SetMaxBound(bound Row) {
	data, err := json.Marshal(bound)
	if err != nil {
		panic(err)
	}
	var rt Row
	if err := json.Unmarshal(data, &rt); err != nil {
		panic(err)
	}
	s.inner.SetMaxBound(rt)
}

func (s *jsonRoundTripStorage) Del(key string) {
	s.inner.Del(key)
}

// changeCollector captures output changes.
type changeCollector struct {
	Changes []Change
	schema  *SourceSchema
}

func (c *changeCollector) Push(change Change, pusher InputBase) []Change {
	c.Changes = append(c.Changes, change)
	return nil
}

func (c *changeCollector) Reset() {
	c.Changes = nil
}

// setupTakeTest creates a MemorySource with messages, a filter, and a Take operator.
// Returns the source, the collector, and the Take operator.
// The Take is hydrated via initial Fetch after setup.
func setupTakeTest(t *testing.T, storage TakeStorage, limit int, rows []Row) (*MemorySource, *changeCollector, *Take) {
	t.Helper()

	columns := map[string]string{
		"id":        "string",
		"senderId":  "string",
		"createdAt": "number",
		"content":   "string",
	}
	pk := []string{"id"}
	ms := NewMemorySource("messages", columns, pk)

	// Connection sort: createdAt DESC, id ASC (complete ordering)
	connSort := Ordering{
		{"createdAt", "desc"},
		{"id", "asc"},
	}

	// Filter: senderId = 'user-7'
	predicate := func(row Row) bool {
		return row["senderId"] == "user-7"
	}

	si := ms.Connect(connSort, predicate, nil)

	take := NewTake(si, storage, limit, nil)

	collector := &changeCollector{schema: si.GetSchema()}
	take.SetOutput(collector)

	// Insert rows before hydration
	ms.BulkInsert(rows)

	// Hydrate: initial Fetch through Take to populate TakeState
	hydrated := slices.Collect(take.Fetch(FetchRequest{}))
	t.Logf("Hydrated %d rows", len(hydrated))
	for i, n := range hydrated {
		if i < 5 || i >= len(hydrated)-2 {
			t.Logf("  hydrated[%d]: id=%v createdAt=%v", i, n.Row["id"], n.Row["createdAt"])
		} else if i == 5 {
			t.Logf("  ...")
		}
	}

	collector.Reset()
	return ms, collector, take
}

func makeMsg(id string, senderId string, createdAt float64) Row {
	return Row{
		"id":        id,
		"senderId":  senderId,
		"createdAt": createdAt,
		"content":   "msg-" + id,
	}
}

func TestTakeDisplacement_DescSort_AddIntoFullWindow(t *testing.T) {
	for _, storageType := range []string{"memory", "json-roundtrip"} {
		t.Run(storageType, func(t *testing.T) {
			var storage TakeStorage
			if storageType == "memory" {
				storage = NewMemoryTakeStorage()
			} else {
				storage = newJSONRoundTripStorage()
			}

			// Build initial rows: 15 for user-7, 5 for user-3
			var rows []Row
			for i := 0; i < 15; i++ {
				rows = append(rows, makeMsg(fmt.Sprintf("msg-%d", i), "user-7", float64(1000+i*100)))
			}
			for i := 0; i < 5; i++ {
				rows = append(rows, makeMsg(fmt.Sprintf("other-%d", i), "user-3", float64(5000+i*100)))
			}

			ms, collector, _ := setupTakeTest(t, storage, 10, rows)

			// Window should be: msg-14(2400), msg-13(2300), ..., msg-5(1500)
			// Bound should be msg-5(1500)
			// New message with createdAt=3000 should enter window and displace msg-5
			newMsg := makeMsg("msg-new-1", "user-7", float64(3000))
			collector.Reset()
			changes := ms.Push(MakeSourceChangeAdd(newMsg))

			// Count ADD and REMOVE changes
			addCount := 0
			removeCount := 0
			for _, c := range collector.Changes {
				switch c.Type {
				case ChangeTypeAdd:
					addCount++
				case ChangeTypeRemove:
					removeCount++
				}
			}

			t.Logf("Storage=%s Push ADD: collector=%d changes (add=%d, remove=%d), source_returned=%d",
				storageType, len(collector.Changes), addCount, removeCount, len(changes))

			// We expect 2 changes: ADD(new msg) + REMOVE(displaced bound)
			if addCount != 1 || removeCount != 1 {
				t.Errorf("Expected 1 ADD + 1 REMOVE, got %d ADD + %d REMOVE", addCount, removeCount)
				for i, c := range collector.Changes {
					t.Logf("  change[%d]: type=%d row=%v", i, c.Type, c.Node.Row)
				}
			}
		})
	}
}

func TestTakeDisplacement_DescSort_SustainedEdits_ThenAdd(t *testing.T) {
	for _, storageType := range []string{"memory", "json-roundtrip"} {
		t.Run(storageType, func(t *testing.T) {
			var storage TakeStorage
			if storageType == "memory" {
				storage = NewMemoryTakeStorage()
			} else {
				storage = newJSONRoundTripStorage()
			}

			// Build initial rows: 20 messages for user-7
			var rows []Row
			for i := 0; i < 20; i++ {
				rows = append(rows, makeMsg(fmt.Sprintf("msg-%d", i), "user-7", float64(1000+i*100)))
			}

			ms, collector, _ := setupTakeTest(t, storage, 10, rows)

			// Window should be: msg-19(2900), msg-18(2800), ..., msg-10(2000)
			// Bound = msg-10(2000)

			// Now do 50 sustained edits changing createdAt on various messages
			// This simulates the soak test's UPDATE messages SET createdAt = NOW()
			nextTs := float64(3000)
			for i := 0; i < 50; i++ {
				msgIdx := i % 20 // cycle through all messages
				msgId := fmt.Sprintf("msg-%d", msgIdx)
				oldTs := float64(1000 + msgIdx*100)

				// Find the current row in the source data
				var oldRow Row
				for _, r := range ms.Data() {
					if r["id"] == msgId {
						oldRow = Row{
							"id":        r["id"],
							"senderId":  r["senderId"],
							"createdAt": r["createdAt"],
							"content":   r["content"],
						}
						break
					}
				}
				if oldRow == nil {
					// Row might have been removed; use the expected values
					oldRow = makeMsg(msgId, "user-7", oldTs)
					// check if it exists
					if !ms.has(oldRow) {
						t.Logf("Skipping edit for %s (not found), oldTs was %f", msgId, oldTs)
						continue
					}
				}

				newRow := Row{
					"id":        msgId,
					"senderId":  "user-7",
					"createdAt": nextTs,
					"content":   oldRow["content"],
				}
				nextTs += 100

				collector.Reset()
				ms.Push(MakeSourceChangeEdit(newRow, oldRow))
			}

			// After sustained edits, the window should contain the 10 most recent messages.
			// Now insert a brand new message with a very high createdAt.
			// This should ALWAYS produce a displacement pair.
			newMsg := makeMsg("msg-final-insert", "user-7", nextTs+10000)
			collector.Reset()
			result := ms.Push(MakeSourceChangeAdd(newMsg))

			addCount := 0
			removeCount := 0
			for _, c := range collector.Changes {
				switch c.Type {
				case ChangeTypeAdd:
					addCount++
				case ChangeTypeRemove:
					removeCount++
				}
			}

			t.Logf("Storage=%s After 50 edits, INSERT: collector=%d changes (add=%d, remove=%d), source_returned=%d",
				storageType, len(collector.Changes), addCount, removeCount, len(result))

			if addCount != 1 || removeCount != 1 {
				t.Errorf("Expected 1 ADD + 1 REMOVE after sustained edits, got %d ADD + %d REMOVE",
					addCount, removeCount)
				for i, c := range collector.Changes {
					t.Logf("  change[%d]: type=%d row_id=%v createdAt=%v", i, c.Type, c.Node.Row["id"], c.Node.Row["createdAt"])
				}
			}
		})
	}
}

func TestTakeDisplacement_DescSort_EditBoundRowUp_ThenAdd(t *testing.T) {
	// Specific scenario: edit the BOUND row's sort key to move it up in the window,
	// then insert a new row. The displacement should still work correctly.
	for _, storageType := range []string{"memory", "json-roundtrip"} {
		t.Run(storageType, func(t *testing.T) {
			var storage TakeStorage
			if storageType == "memory" {
				storage = NewMemoryTakeStorage()
			} else {
				storage = newJSONRoundTripStorage()
			}

			messages := []Row{
				makeMsg("a", "user-7", float64(800)),
				makeMsg("b", "user-7", float64(700)),
				makeMsg("c", "user-7", float64(600)),
				makeMsg("d", "user-7", float64(500)),
				makeMsg("e", "user-7", float64(400)),
				makeMsg("f", "user-7", float64(300)),
				makeMsg("g", "user-7", float64(200)),
				makeMsg("h", "user-7", float64(100)),
			}

			ms, collector, _ := setupTakeTest(t, storage, 5, messages)
			// Window = [a(800), b(700), c(600), d(500), e(400)], bound = e(400)

			// Edit bound row (e, createdAt=400) → createdAt=900 (moves to top)
			oldE := makeMsg("e", "user-7", float64(400))
			newE := makeMsg("e", "user-7", float64(900))
			collector.Reset()
			ms.Push(MakeSourceChangeEdit(newE, oldE))

			// After edit: window = [e(900), a(800), b(700), c(600), d(500)]
			// New bound should be d(500)
			t.Logf("After edit of bound row: %d changes", len(collector.Changes))

			// Now insert a message with createdAt=450 (between d=500 and f=300)
			// This should be INSIDE the window (450 < 500 = bound? No, 450 is AFTER 500 in DESC)
			// Actually in DESC: 500 > 450, so 450 sorts after 500. So 450 is OUTSIDE the window.
			// Let's insert createdAt=550 instead (between c=600 and d=500)
			// In DESC: 550 < 600 but 550 > 500, so 550 sorts between c and d. Inside the window.
			newMsg := makeMsg("x", "user-7", float64(550))
			collector.Reset()
			changes := ms.Push(MakeSourceChangeAdd(newMsg))

			addCount := 0
			removeCount := 0
			for _, c := range collector.Changes {
				switch c.Type {
				case ChangeTypeAdd:
					addCount++
				case ChangeTypeRemove:
					removeCount++
				}
			}

			t.Logf("Storage=%s INSERT after bound edit: collector=%d (add=%d, remove=%d), returned=%d",
				storageType, len(collector.Changes), addCount, removeCount, len(changes))

			// x(550) enters window, d(500) = current bound should be displaced
			if addCount != 1 || removeCount != 1 {
				t.Errorf("Expected displacement: 1 ADD + 1 REMOVE, got %d ADD + %d REMOVE",
					addCount, removeCount)
				for i, c := range collector.Changes {
					t.Logf("  change[%d]: type=%d row_id=%v createdAt=%v", i, c.Type, c.Node.Row["id"], c.Node.Row["createdAt"])
				}
			}
		})
	}
}

func TestTakeDisplacement_DescSort_EditOutsideRowIn_Displacement(t *testing.T) {
	// An edit moves a row from OUTSIDE the window to INSIDE, displacing the bound.
	for _, storageType := range []string{"memory", "json-roundtrip"} {
		t.Run(storageType, func(t *testing.T) {
			var storage TakeStorage
			if storageType == "memory" {
				storage = NewMemoryTakeStorage()
			} else {
				storage = newJSONRoundTripStorage()
			}

			messages := []Row{
				makeMsg("a", "user-7", float64(800)),
				makeMsg("b", "user-7", float64(700)),
				makeMsg("c", "user-7", float64(600)),
				makeMsg("d", "user-7", float64(500)),
				makeMsg("e", "user-7", float64(400)),
				makeMsg("f", "user-7", float64(300)), // outside window
				makeMsg("g", "user-7", float64(200)), // outside window
			}

			ms, collector, _ := setupTakeTest(t, storage, 5, messages)
			// Window = [a(800), b(700), c(600), d(500), e(400)], bound = e(400)

			// Edit f (outside, createdAt=300) → createdAt=900 (inside, moves to top)
			oldF := makeMsg("f", "user-7", float64(300))
			newF := makeMsg("f", "user-7", float64(900))
			collector.Reset()
			result := ms.Push(MakeSourceChangeEdit(newF, oldF))

			// This should produce: REMOVE(e=bound) + ADD(f with new createdAt)
			addCount := 0
			removeCount := 0
			editCount := 0
			for _, c := range collector.Changes {
				switch c.Type {
				case ChangeTypeAdd:
					addCount++
				case ChangeTypeRemove:
					removeCount++
				case ChangeTypeEdit:
					editCount++
				}
			}

			t.Logf("Storage=%s Edit outside→inside: collector=%d (add=%d, remove=%d, edit=%d), returned=%d",
				storageType, len(collector.Changes), addCount, removeCount, editCount, len(result))

			// Expected: 1 REMOVE (old bound) + 1 ADD (edited row entering window)
			if addCount != 1 || removeCount != 1 {
				t.Errorf("Expected 1 ADD + 1 REMOVE for edit displacement, got add=%d remove=%d edit=%d",
					addCount, removeCount, editCount)
				for i, c := range collector.Changes {
					nodeId := c.Node.Row["id"]
					nodeTs := c.Node.Row["createdAt"]
					t.Logf("  change[%d]: type=%d id=%v createdAt=%v", i, c.Type, nodeId, nodeTs)
				}
			}
		})
	}
}

// TestTakeDisplacement_StressVerify runs hundreds of random edits on a
// simple Source -> Take pipeline and verifies the Take window contents
// after EVERY mutation by comparing Fetch results against a brute-force
// reference (sort all rows, take first N).
func TestTakeDisplacement_StressVerify(t *testing.T) {
	for _, storageType := range []string{"memory", "json-roundtrip"} {
		t.Run(storageType, func(t *testing.T) {
			var storage TakeStorage
			if storageType == "memory" {
				storage = NewMemoryTakeStorage()
			} else {
				storage = newJSONRoundTripStorage()
			}

			const numTickets = 20
			const limit = 15
			const numMutations = 500

			var rows []Row
			for i := 1; i <= numTickets; i++ {
				rows = append(rows, makeMsg(
					fmt.Sprintf("ticket-%d", i), "user-7",
					float64(1000+i*100),
				))
			}

			ms, collector, take := setupTakeTest(t, storage, limit, rows)
			_ = collector

			verifyWindow(t, take, ms, limit, 0)

			nextTs := float64(5000)
			for mut := 1; mut <= numMutations; mut++ {
				ticketIdx := (mut * 7) % numTickets
				ticketID := fmt.Sprintf("ticket-%d", ticketIdx+1)

				var oldRow Row
				for _, r := range ms.Data() {
					if r["id"] == ticketID {
						oldRow = Row{}
						for k, v := range r {
							oldRow[k] = v
						}
						break
					}
				}
				if oldRow == nil {
					t.Fatalf("mutation %d: ticket %s not found", mut, ticketID)
				}

				newRow := Row{}
				for k, v := range oldRow {
					newRow[k] = v
				}
				newRow["createdAt"] = nextTs
				nextTs += 100

				collector.Reset()
				ms.Push(MakeSourceChangeEdit(newRow, oldRow))

				if !verifyWindow(t, take, ms, limit, mut) {
					t.Fatalf("State divergence at mutation %d (edit %s createdAt=%v->%v)",
						mut, ticketID, oldRow["createdAt"], newRow["createdAt"])
				}
			}

			t.Logf("All %d mutations passed", numMutations)
		})
	}
}

func verifyWindow(t *testing.T, take *Take, ms *MemorySource, limit int, mutNum int) bool {
	t.Helper()

	takeNodes := slices.Collect(take.Fetch(FetchRequest{}))

	var matching []Row
	for _, r := range ms.Data() {
		if r["senderId"] == "user-7" {
			matching = append(matching, r)
		}
	}

	cmp := MakeComparator(Ordering{{"createdAt", "desc"}, {"id", "asc"}}, false)
	for i := 0; i < len(matching); i++ {
		for j := i + 1; j < len(matching); j++ {
			if cmp(matching[i], matching[j]) > 0 {
				matching[i], matching[j] = matching[j], matching[i]
			}
		}
	}
	if len(matching) > limit {
		matching = matching[:limit]
	}

	if len(takeNodes) != len(matching) {
		t.Errorf("mut %d: Take=%d rows, expected=%d", mutNum, len(takeNodes), len(matching))
		logWindowDiff(t, takeNodes, matching)
		return false
	}

	for i := range takeNodes {
		if takeNodes[i].Row["id"] != matching[i]["id"] ||
			takeNodes[i].Row["createdAt"] != matching[i]["createdAt"] {
			t.Errorf("mut %d: row %d mismatch Take={%v,%v} want={%v,%v}",
				mutNum, i,
				takeNodes[i].Row["id"], takeNodes[i].Row["createdAt"],
				matching[i]["id"], matching[i]["createdAt"])
			logWindowDiff(t, takeNodes, matching)
			return false
		}
	}
	return true
}

func logWindowDiff(t *testing.T, takeNodes []Node, expected []Row) {
	t.Helper()
	t.Logf("  Take window:")
	for i, n := range takeNodes {
		t.Logf("    [%d] id=%v createdAt=%v", i, n.Row["id"], n.Row["createdAt"])
	}
	t.Logf("  Expected:")
	for i, r := range expected {
		t.Logf("    [%d] id=%v createdAt=%v", i, r["id"], r["createdAt"])
	}
}

// TestTakeDisplacement_MultiConnection_SplitEdit tests the scenario where
// multiple connections share a source, one connection has splitEditKeys,
// causing edits to be split into REMOVE+ADD for ALL connections.
// This matches the soak test topology where simple limit queries coexist
// with join/exists queries on the same table.
func TestTakeDisplacement_MultiConnection_SplitEdit(t *testing.T) {
	const numTickets = 20
	const limit = 15
	const numMutations = 500

	columns := map[string]string{
		"id":        "string",
		"senderId":  "string",
		"createdAt": "number",
		"content":   "string",
	}
	pk := []string{"id"}
	ms := NewMemorySource("tickets", columns, pk)

	connSort := Ordering{{"createdAt", "desc"}, {"id", "asc"}}

	// Connection 1: simple limit=15 query (no filter, no split keys)
	si1 := ms.Connect(connSort, nil, nil)
	take1 := NewTake(si1, NewMemoryTakeStorage(), limit, nil)
	col1 := &changeCollector{}
	take1.SetOutput(col1)

	// Connection 2: query with splitEditKeys (simulates a JOIN correlation on "senderId")
	splitKeys := map[string]bool{"senderId": true}
	si2 := ms.Connect(connSort, nil, splitKeys)
	take2 := NewTake(si2, NewMemoryTakeStorage(), 10, nil)
	col2 := &changeCollector{}
	take2.SetOutput(col2)

	// Insert rows
	var rows []Row
	for i := 1; i <= numTickets; i++ {
		rows = append(rows, Row{
			"id":        fmt.Sprintf("ticket-%d", i),
			"senderId":  "user-7",
			"createdAt": float64(1000 + i*100),
			"content":   fmt.Sprintf("ticket-%d", i),
		})
	}
	ms.BulkInsert(rows)

	// Hydrate both Takes
	h1 := slices.Collect(take1.Fetch(FetchRequest{}))
	h2 := slices.Collect(take2.Fetch(FetchRequest{}))
	t.Logf("Take1 hydrated %d rows, Take2 hydrated %d rows", len(h1), len(h2))

	// Run mutations that change createdAt (sort key)
	// These will NOT trigger split because senderId doesn't change
	nextTs := float64(5000)
	for mut := 1; mut <= numMutations; mut++ {
		ticketIdx := (mut * 7) % numTickets
		ticketID := fmt.Sprintf("ticket-%d", ticketIdx+1)

		var oldRow Row
		for _, r := range ms.Data() {
			if r["id"] == ticketID {
				oldRow = Row{}
				for k, v := range r {
					oldRow[k] = v
				}
				break
			}
		}
		if oldRow == nil {
			t.Fatalf("mut %d: %s not found", mut, ticketID)
		}

		newRow := Row{}
		for k, v := range oldRow {
			newRow[k] = v
		}
		newRow["createdAt"] = nextTs
		nextTs += 100

		col1.Reset()
		col2.Reset()
		ms.Push(MakeSourceChangeEdit(newRow, oldRow))

		if !verifyWindowDirect(t, take1, ms, limit, mut, nil) {
			t.Fatalf("Take1 diverged at mut %d (edit %s)", mut, ticketID)
		}
	}

	// Now run mutations that ALSO change senderId (triggers split!)
	t.Logf("Phase 2: mutations that trigger split-edit (senderId changes)")
	senders := []string{"user-7", "user-8", "user-9", "user-7"}
	for mut := 1; mut <= 200; mut++ {
		ticketIdx := (mut * 3) % numTickets
		ticketID := fmt.Sprintf("ticket-%d", ticketIdx+1)

		var oldRow Row
		for _, r := range ms.Data() {
			if r["id"] == ticketID {
				oldRow = Row{}
				for k, v := range r {
					oldRow[k] = v
				}
				break
			}
		}
		if oldRow == nil {
			t.Fatalf("phase2 mut %d: %s not found", mut, ticketID)
		}

		newRow := Row{}
		for k, v := range oldRow {
			newRow[k] = v
		}
		newRow["createdAt"] = nextTs
		newRow["senderId"] = senders[mut%len(senders)]
		nextTs += 100

		col1.Reset()
		col2.Reset()
		ms.Push(MakeSourceChangeEdit(newRow, oldRow))

		// Verify Take1 (the simple limit=15 query with NO filter)
		if !verifyWindowDirect(t, take1, ms, limit, 500+mut, nil) {
			t.Fatalf("Take1 diverged at phase2 mut %d (edit %s senderId=%v->%v)",
				mut, ticketID, oldRow["senderId"], newRow["senderId"])
		}
	}

	t.Logf("All mutations passed (500 non-split + 200 split)")
}

// verifyWindowDirect verifies Take against brute-force without filter dependency.
func verifyWindowDirect(t *testing.T, take *Take, ms *MemorySource, limit int, mutNum int, filterPred func(Row) bool) bool {
	t.Helper()

	takeNodes := slices.Collect(take.Fetch(FetchRequest{}))

	var matching []Row
	for _, r := range ms.Data() {
		if filterPred == nil || filterPred(r) {
			matching = append(matching, r)
		}
	}

	cmp := MakeComparator(Ordering{{"createdAt", "desc"}, {"id", "asc"}}, false)
	for i := 0; i < len(matching); i++ {
		for j := i + 1; j < len(matching); j++ {
			if cmp(matching[i], matching[j]) > 0 {
				matching[i], matching[j] = matching[j], matching[i]
			}
		}
	}
	if len(matching) > limit {
		matching = matching[:limit]
	}

	if len(takeNodes) != len(matching) {
		t.Errorf("mut %d: Take=%d rows, expected=%d", mutNum, len(takeNodes), len(matching))
		logWindowDiff(t, takeNodes, matching)
		return false
	}

	for i := range takeNodes {
		if takeNodes[i].Row["id"] != matching[i]["id"] ||
			takeNodes[i].Row["createdAt"] != matching[i]["createdAt"] {
			t.Errorf("mut %d: row %d mismatch Take={%v,%v} want={%v,%v}",
				mutNum, i,
				takeNodes[i].Row["id"], takeNodes[i].Row["createdAt"],
				matching[i]["id"], matching[i]["createdAt"])
			logWindowDiff(t, takeNodes, matching)
			return false
		}
	}
	return true
}
