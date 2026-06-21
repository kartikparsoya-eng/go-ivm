package main

// Standalone repro for the channels EXISTS channel_participants hydrate bug.
// Opens the real replica.db copy + runs the same Fetch the engine would issue
// inside the Join. Logs what TableSource returns at each layer so we can
// pinpoint whether the missing rows are at the SQL scan, the post-filter, or
// the snapshot pin.

import (
	"fmt"
	"os"

	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

func main() {
	replica := "/tmp/replica-debug.db"
	if len(os.Args) > 1 {
		replica = os.Args[1]
	}

	db, err := tablesource.Open(replica, tablesource.OpenOptions{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Open: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	writableDB, err := tablesource.OpenWritable(replica, tablesource.OpenOptions{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "OpenWritable: %v\n", err)
		os.Exit(1)
	}
	defer writableDB.Close()

	cpCols := map[string]sqlite.ColumnSchema{
		"id":        {Type: "string"},
		"channelId": {Type: "string"},
		"userId":    {Type: "string"},
		"joinedAt":  {Type: "number"},
		"role":      {Type: "string"},
	}
	cpSrc, err := tablesource.New(db, writableDB, "channel_participants", cpCols, []string{"id"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "channel_participants New: %v\n", err)
		os.Exit(1)
	}

	chanCols := map[string]sqlite.ColumnSchema{
		"id":         {Type: "string"},
		"name":       {Type: "string"},
		"visibility": {Type: "string"},
	}
	chanSrc, err := tablesource.New(db, writableDB, "channels", chanCols, []string{"id"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "channels New: %v\n", err)
		os.Exit(1)
	}

	// Probe 1: full channel scan (no filter, no constraint). Should return 4.
	chanIn := chanSrc.Connect(ivm.Ordering{{"id", "asc"}}, nil, nil, nil)
	chanNodes := chanIn.Fetch(ivm.FetchRequest{})
	fmt.Printf("Probe 1: channel_count=%d\n", len(chanNodes))
	for _, n := range chanNodes {
		fmt.Printf("  channel: id=%v name=%v visibility=%v\n",
			n.Row["id"], n.Row["name"], n.Row["visibility"])
	}

	// Probe 2: full channel_participants scan with the EXISTS-side filter
	// (userId='cmp2cccy9005uyifciswws01m'). The closure mirrors what the
	// builder.BuildPredicate would produce.
	targetUser := "cmp2cccy9005uyifciswws01m"
	filter := func(row ivm.Row) bool {
		v, _ := row["userId"].(string)
		return v == targetUser
	}
	cpIn := cpSrc.Connect(ivm.Ordering{{"id", "asc"}}, nil, filter, map[string]bool{"channelId": true})
	cpNodes := cpIn.Fetch(ivm.FetchRequest{})
	fmt.Printf("\nProbe 2: channel_participants_count_after_filter=%d (filter: userId=%s)\n", len(cpNodes), targetUser)
	for _, n := range cpNodes {
		fmt.Printf("  cp: id=%v channelId=%v userId=%v\n",
			n.Row["id"], n.Row["channelId"], n.Row["userId"])
	}

	// Probe 3: child fetch with constraint (what the Join issues for each parent).
	// Constraint = {channelId: <seeded channel id>}. Should return the
	// cp-test-sandbox-1 row if present.
	seededChannel := "cmp2cqlq900f7iphvij992i5e"
	constrained := cpIn.Fetch(ivm.FetchRequest{
		Constraint: &ivm.Constraint{"channelId": seededChannel},
	})
	fmt.Printf("\nProbe 3: cp_count_for_channel(%s)=%d\n", seededChannel, len(constrained))
	for _, n := range constrained {
		fmt.Printf("  cp: id=%v channelId=%v userId=%v\n",
			n.Row["id"], n.Row["channelId"], n.Row["userId"])
	}

	// Probe 4: raw SQL fallback so we can compare against the SQLite truth.
	rows, err := db.Query(
		`SELECT id, "channelId", "userId" FROM channel_participants WHERE "channelId"=? AND "userId"=?`,
		seededChannel, targetUser,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "raw query: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()
	fmt.Printf("\nProbe 4: raw SQL — same constraint:\n")
	for rows.Next() {
		var id, channelID, userID string
		if err := rows.Scan(&id, &channelID, &userID); err != nil {
			fmt.Fprintf(os.Stderr, "scan: %v\n", err)
			continue
		}
		fmt.Printf("  row: id=%v channelId=%v userId=%v\n", id, channelID, userID)
	}
}
