// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main // import "robpike.io/ivy"

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"strings"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/glycerine/vprint"
	"github.com/gomem/gomem/pkg/dataframe"
	"robpike.io/ivy/config"
	"robpike.io/ivy/exec"
	"robpike.io/ivy/parse"
	"robpike.io/ivy/run"
	"robpike.io/ivy/scan"
	"robpike.io/ivy/value"
)

var (
	execute         = flag.String("e", "", "execute `argument` and quit")
	executeContinue = flag.String("i", "", "execute `argument` and continue")
	file            = flag.String("f", "", "execute `file` before input")
	format          = flag.String("format", "", "use `fmt` as format for printing numbers; empty sets default format")
	gformat         = flag.Bool("g", false, `shorthand for -format="%.12g"`)
	maxbits         = flag.Uint("maxbits", 1e9, "maximum size of an integer, in bits; 0 means no limit")
	maxdigits       = flag.Uint("maxdigits", 1e4, "above this many `digits`, integers print as floating point; 0 disables")
	maxstack        = flag.Uint("stack", 100000, "maximum call stack `depth` allowed")
	origin          = flag.Int("origin", 1, "set index origin to `n` (must be >=0)")
	prompt          = flag.String("prompt", "", "command `prompt`")
	debugFlag       = flag.String("debug", "", "comma-separated `names` of debug settings to enable")
	parquet         = flag.String("parquet", "", "execute with arrow table")
)

var (
	conf    config.Config
	context value.Context
)

func main() {
	flag.Usage = usage
	flag.Parse()

	f, _ := os.Create("/tmp/ivy.prof")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	if *origin < 0 {
		fmt.Fprintf(os.Stderr, "ivy: illegal origin value %d\n", *origin)
		os.Exit(2)
	}

	if *gformat {
		*format = "%.12g"
	}

	conf.SetFormat(*format)
	conf.SetMaxBits(*maxbits)
	conf.SetMaxDigits(*maxdigits)
	conf.SetMaxStack(*maxstack)
	conf.SetOrigin(*origin)
	conf.SetPrompt(*prompt)

	if len(*debugFlag) > 0 {
		for _, debug := range strings.Split(*debugFlag, ",") {
			if !conf.SetDebug(debug, true) {
				fmt.Fprintf(os.Stderr, "ivy: unknown debug flag %q\n", debug)
				os.Exit(2)
			}
		}
	}

	context = exec.NewContext(&conf)

	if *file != "" {
		if !runFile(context, *file) {
			os.Exit(1)
		}
	}

	if *executeContinue != "" {
		if !runString(context, *executeContinue) {
			os.Exit(1)
		}
	}

	if *execute != "" {
		if !runString(context, *execute) {
			os.Exit(1)
		}
		return
	}

	if flag.NArg() > 0 {
		for i := 0; i < flag.NArg(); i++ {
			if !runFile(context, flag.Arg(i)) {
				os.Exit(1)
			}
		}
		return
	}

	scanner := scan.New(context, "<stdin>", bufio.NewReader(os.Stdin))
	/*
		pool := memory.NewGoAllocator()
		col := NewArrowIntColumn([]int64{1, 2, 3}, pool)
		defer col.Release()
		context.AssignGlobal("df1", value.NewArrowVector(col))
	*/
	if *parquet != "" {
		vprint.VV("initializing %v", *parquet)
		err := context.(*exec.Context).LoadGlobalsFromParquet(*parquet, conf)
		if err != nil {
			vprint.VV("error %v", err)
			os.Exit(1)
		}
	}
	parser := parse.NewParser("<stdin>", scanner, context)
	for !run.Run(parser, context, true) {
	}
}

// TWG(twg) testing column needes to be released
func NewArrowIntColumn(v []int64, pool memory.Allocator) *arrow.Column {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "INT", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
	bld := array.NewRecordBuilder(pool, schema)
	defer bld.Release()

	bld.Field(0).(*array.Int64Builder).AppendValues(v, nil)

	rc := bld.NewRecord()
	defer rc.Release()

	table := array.NewTableFromRecords(schema, []arrow.Record{rc})
	//	defer table.Release()
	r := dataframe.NewChunkResolver(table.Column(0))
	vprint.VV("check: %v", r.NumRows)
	return table.Column(0)
}

// runFile executes the contents of the file as an ivy program.
func runFile(context value.Context, file string) bool {
	var fd io.Reader
	var err error
	interactive := false
	if file == "-" {
		interactive = true
		fd = os.Stdin
	} else {
		interactive = false
		fd, err = os.Open(file)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "ivy: %s\n", err)
		os.Exit(1)
	}
	scanner := scan.New(context, file, bufio.NewReader(fd))
	parser := parse.NewParser(file, scanner, context)
	return run.Run(parser, context, interactive)
}

// runString executes the string, typically a command-line argument, as an ivy program.
func runString(context value.Context, str string) bool {
	scanner := scan.New(context, "<args>", strings.NewReader(str))
	parser := parse.NewParser("<args>", scanner, context)
	return run.Run(parser, context, false)
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: ivy [options] [file ...]\n")
	fmt.Fprintf(os.Stderr, "Flags:\n")
	flag.PrintDefaults()
	os.Exit(2)
}
