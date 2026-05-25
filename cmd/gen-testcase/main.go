package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/kartikparsoya-eng/go-ivm/testharness"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: gen-testcase <seed> <optsJSON>\n")
		os.Exit(1)
	}

	seed, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid seed: %v\n", err)
		os.Exit(1)
	}

	var opts testharness.FuzzOpts
	if err := json.Unmarshal([]byte(os.Args[2]), &opts); err != nil {
		fmt.Fprintf(os.Stderr, "invalid opts: %v\n", err)
		os.Exit(1)
	}

	fuzzer := testharness.NewFuzzer(seed)
	data := fuzzer.GenerateTestCase(opts)
	fmt.Println(string(data))
}
