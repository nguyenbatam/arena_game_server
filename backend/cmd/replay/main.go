// Command replay re-simulates a recorded match and checks that this build still
// reproduces it.
//
//	go run ./cmd/replay replays/r-gs-local-1234.arnr
//	go run ./cmd/replay replays/          # every recording in a directory
//
// A recording holds the seed and the inputs, not the outcome, so verification
// is a real re-simulation: the world is rebuilt from scratch and its checksum
// compared with the one the server stamped when the match ended. A mismatch
// means this build no longer produces the match that was played — a desync bug,
// or a gameplay change that was not meant to be one.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/nguyenbatam/arena_game_server/internal/replay"
)

func main() {
	verbose := flag.Bool("v", false, "print each frame")
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: replay [-v] <file.arnr | dir>")
		os.Exit(2)
	}

	paths, err := expand(flag.Args())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(paths) == 0 {
		fmt.Fprintln(os.Stderr, "no .arnr files found")
		os.Exit(1)
	}

	bad := 0
	for _, path := range paths {
		if !verify(path, *verbose) {
			bad++
		}
	}
	fmt.Printf("\n%d replay(s), %d mismatched\n", len(paths), bad)
	if bad > 0 {
		os.Exit(1)
	}
}

func verify(path string, verbose bool) bool {
	rec, err := replay.Load(path)
	if err != nil {
		fmt.Printf("%-40s ERROR  %v\n", filepath.Base(path), err)
		return false
	}
	if verbose {
		for _, f := range rec.Frames {
			fmt.Printf("  tick %5d  %d input(s)\n", f.Tick, len(f.Inputs))
		}
	}
	got, ok := rec.Verify()
	status := "OK"
	if !ok {
		status = "DESYNC"
	}
	fmt.Printf("%-40s %-7s seed=%d tick_rate=%d players=%d ticks=%d frames=%d checksum=%#016x\n",
		filepath.Base(path), status, rec.Header.Seed, rec.Header.TickRate,
		len(rec.Header.Roster), rec.FinalTick, len(rec.Frames), got)
	if !ok {
		fmt.Printf("%-40s         recorded=%#016x replayed=%#016x\n", "", rec.Checksum, got)
	}
	return ok
}

func expand(args []string) ([]string, error) {
	var out []string
	for _, arg := range args {
		info, err := os.Stat(arg)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			out = append(out, arg)
			continue
		}
		entries, err := os.ReadDir(arg)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".arnr") {
				out = append(out, filepath.Join(arg, e.Name()))
			}
		}
	}
	sort.Strings(out)
	return out, nil
}
