// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package value // import "robpike.io/ivy/value"

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/glycerine/vprint"
	"robpike.io/ivy/config"
)

var debugConf = &config.Config{} // For debugging, e.g. to call a String method.

type Value interface {
	// String is for internal debugging only. It uses default configuration
	// and puts parentheses around every value so it's clear when it is used.
	// All user output should call Sprint instead.
	String() string
	Sprint(*config.Config) string
	Eval(Context) Value

	// Inner retrieves the value, without evaluation. But for Assignments,
	// it returns the right-hand side.
	Inner() Value

	// Rank returns the rank of the value: 0 for scalar, 1 for vector, etc.
	Rank() int

	// shrink returns a simpler form of the value, such as an
	// integer for an integral BigFloat. For some types it is
	// the identity. It does not modify the receiver.
	shrink() Value

	// ProgString is like String, but suitable for program listing.
	// For instance, it ignores the user format for numbers and
	// puts quotes on chars, guaranteeing a correct representation.
	ProgString() string

	toType(string, *config.Config, valueType) Value
}

// Error is the type we recognize as a recoverable run-time error.
type Error string

func (err Error) Error() string {
	return string(err)
}

// Errorf panics with the formatted string, with type Error.
func Errorf(format string, args ...interface{}) {
	panic(Error(fmt.Sprintf(format, args...)))
}

func parseTwo(conf *config.Config, s string) (Value, Value, string, error) {
	var elems []string
	var sep string
	var typ string
	if strings.ContainsRune(s, 'j') {
		sep = "j"
		typ = "complex"
	} else if strings.ContainsRune(s, '/') {
		sep = "/"
		typ = "rational"
	} else {
		return Int(0), Int(0), "", nil
	}
	elems = strings.Split(s, sep)
	if len(elems) != 2 || elems[0] == "" || elems[1] == "" {
		Errorf("bad %s number syntax: %q", typ, s)
	}
	v1, err := Parse(conf, elems[0])
	if err != nil {
		return nil, nil, "", err
	}
	v2, err := Parse(conf, elems[1])
	if err != nil {
		return nil, nil, "", err
	}
	return v1, v2, sep, err
}

func Parse(conf *config.Config, s string) (Value, error) {
	// Is it a complex or rational?
	v1, v2, sep, err := parseTwo(conf, s)
	if err != nil {
		return nil, err
	}
	switch sep {
	case "j":
		// A complex.
		return newComplex(v1, v2), nil
	case "/":
		// A rational. It's tricky.
		// Common simple case.
		if whichType(v1) == intType && whichType(v2) == intType {
			return bigRatTwoInt64s(int64(v1.(Int)), int64(v2.(Int))).shrink(), nil
		}
		// General mix-em-up.
		rden := v2.toType("rat", conf, bigRatType)
		if rden.(BigRat).Sign() == 0 {
			Errorf("zero denominator in rational")
		}
		return binaryBigRatOp(v1.toType("rat", conf, bigRatType), (*big.Rat).Quo, rden), nil
	}
	// Not a rational, but might be something like 1.3e-2 and therefore
	// become a rational.
	i, err := setIntString(conf, s)
	if err == nil {
		return i, nil
	}
	b, err := setBigIntString(conf, s)
	if err == nil {
		return b.shrink(), nil
	}
	r, err := setBigRatFromFloatString(s) // We know there is no slash.
	if err == nil {
		return r.shrink(), nil
	}
	return nil, err
}

func bigInt64(x int64) BigInt {
	return BigInt{big.NewInt(x)}
}

func bigRatInt64(x int64) BigRat {
	return bigRatTwoInt64s(x, 1)
}

func bigFloatInt64(conf *config.Config, x int64) BigFloat {
	return BigFloat{new(big.Float).SetPrec(conf.FloatPrec()).SetInt64(x)}
}

func bigRatTwoInt64s(x, y int64) BigRat {
	if y == 0 {
		Errorf("zero denominator in rational")
	}
	return BigRat{big.NewRat(x, y)}
}

func ToArrowColumn(value Value, mem memory.Allocator) *arrow.Column {
	switch v := value.(type) {
	case Vector:
		vprint.VV(("convert to Arrow.Column"))
		return v.ToArrowCol(mem)
	case ArrowVector:
		return v.col
	case Int:
		return IntToArrowIntCol(v, mem)
	case BigFloat:
		return FloatToArrowFloatCol(v, mem)
	case *Matrix:
		vprint.VV(("convert to Arrow.Column"))
	default:
		vprint.VV("What Type %T", v)
	}
	return nil
}

func IntToArrowIntCol(v Int, mem memory.Allocator) *arrow.Column {
	vprint.VV("singe int %v", v)
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "I", Type: arrow.PrimitiveTypes.Int64},
			//{Name: "C", Type: arrow.BinaryTypes.String},
		},
		nil, // no metadata
	)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{int64(v)}, nil)

	table := array.NewTableFromRecords(schema, []arrow.Record{b.NewRecord()})
	return table.Column(0)
}

func FloatToArrowFloatCol(v BigFloat, mem memory.Allocator) *arrow.Column {
	vprint.VV("singe Float %v", v)
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "F", Type: arrow.PrimitiveTypes.Float64},
			//{Name: "C", Type: arrow.BinaryTypes.String},
		},
		nil, // no metadata
	)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	f, _ := v.Float64()
	b.Field(0).(*array.Float64Builder).AppendValues([]float64{f}, nil)
	table := array.NewTableFromRecords(schema, []arrow.Record{b.NewRecord()})
	return table.Column(0)
}
