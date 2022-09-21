// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package value

import (
	"bytes"
	"fmt"
	"math/big"
	"sort"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/glycerine/vprint"
	"github.com/gomem/gomem/pkg/dataframe"
	"robpike.io/ivy/config"
)

type ValueGetter interface {
	Get(i int) Value
	Len() int
}

type ArrowVector struct {
	col      *arrow.Column
	resolver dataframe.ChunkResolver
	config   *config.Config
}

func (v ArrowVector) String() string {
	return "(" + v.Sprint(debugConf) + ")"
}

func (v ArrowVector) Sprint(conf *config.Config) string {
	return v.makeString(conf, !v.AllChars())
}

func (v ArrowVector) Rank() int {
	return 1
}

// TODO(twg) 2022/09/06 untested just a sketch for now
func (v ArrowVector) Slice(beg, end int64) (ArrowVector, error) {
	if end > int64(v.resolver.NumRows) || beg > end {
		return ArrowVector{}, fmt.Errorf("mutation: index out of range")
	}

	sliceCol := *array.NewColumnSlice(v.col, beg, end)
	/*
		defer func() {
			sliceCol.Release()
		}()
	*/

	//	rows := end - beg
	return NewArrowVector(&sliceCol, v.config), nil
}

func (v ArrowVector) ProgString() string {
	// There is no such thing as a vector in program listings; they
	// are represented as a sliceExpr.
	panic("arrowvector.ProgString - cannot happen")
}

// makeString is like String but takes a flag specifying
// whether to put spaces between the elements. By
// default (that is, by calling String) spaces are suppressed
// if all the elements of the ArrowVector are Chars.
func (v ArrowVector) makeString(conf *config.Config, spaces bool) string {
	var b bytes.Buffer
	for i := 0; i < v.resolver.NumRows; i++ {
		if spaces && i > 0 {
			fmt.Fprint(&b, " ")
		}
		fmt.Fprintf(&b, "%s", v.Get(i).Sprint(conf))
	}
	return b.String()
}

// AllChars reports whether the vector contains only Chars.
// TODO(twg) possibly only float/int support
func (v ArrowVector) AllChars() bool {
	return false
	/*
		for _, c := range v {
			if _, ok := c.Inner().(Char); !ok {
				return false
			}
		}
		return true
	*/
}

// AllInts reports whether the vector contains only Ints.
func (v ArrowVector) AllInts() bool {
	return true
	for i := 0; i < v.resolver.NumRows; i++ {
		if _, ok := v.Get(i).(Int); !ok {
			return false
		}
	}
	return true
}

// func NewArrowVector(elems []Value) ArrowVector {
func NewArrowVector(col *arrow.Column, config *config.Config) ArrowVector {
	return ArrowVector{
		col:      col,
		resolver: dataframe.NewChunkResolver(col),
		config:   config,
	}
}

/*
func NewIntArrowVector(elems []int) Vector { // TODO (twg) Needed?
	vec := make([]Value, len(elems))
	for i, elem := range elems {
		vec[i] = Int(elem)
	}
	return Vector(vec)
}
*/

func (v ArrowVector) Get(i int) Value {
	c, offset := v.resolver.Resolve((i))
	switch v.col.DataType() {
	case arrow.PrimitiveTypes.Int8:
		x := v.col.Data().Chunk(c).(*array.Int8).Int8Values()
		return Int(x[offset])
	case arrow.PrimitiveTypes.Int16:
		x := v.col.Data().Chunk(c).(*array.Int16).Int16Values()
		return Int(x[offset])
	case arrow.PrimitiveTypes.Int32:
		x := v.col.Data().Chunk(c).(*array.Int32).Int32Values()
		return Int(x[offset])
	case arrow.PrimitiveTypes.Int64:
		x := v.col.Data().Chunk(c).(*array.Int64).Int64Values()
		return Int(x[offset])
	case arrow.PrimitiveTypes.Uint8:
		x := v.col.Data().Chunk(c).(*array.Uint8).Uint8Values()
		return Int(x[offset])
	case arrow.PrimitiveTypes.Uint16:
		x := v.col.Data().Chunk(c).(*array.Uint16).Uint16Values()
		return Int(x[offset])
	case arrow.PrimitiveTypes.Uint32:
		x := v.col.Data().Chunk(c).(*array.Uint32).Uint32Values()
		return Int(x[offset])
	case arrow.PrimitiveTypes.Uint64:
		x := v.col.Data().Chunk(c).(*array.Uint64).Uint64Values()
		return Int(x[offset])
	case arrow.PrimitiveTypes.Float32:
		x := v.col.Data().Chunk(c).(*array.Float32).Float32Values()
		return BigFloat{new(big.Float).SetPrec(v.config.FloatPrec()).SetFloat64(float64(x[offset]))}
	case arrow.PrimitiveTypes.Float64:
		x := v.col.Data().Chunk(c).(*array.Float64).Float64Values()
		return BigFloat{new(big.Float).SetPrec(v.config.FloatPrec()).SetFloat64(x[offset])}
	}
	vprint.VV("Get value not supported returning nil %v", v.col.DataType())
	return nil
}

func (v ArrowVector) Eval(Context) Value {
	return v
}

func (v ArrowVector) Inner() Value {
	return v
}

// Arrow Vectors are read only so copying converts to a regular veco
func (v ArrowVector) Copy() Vector {
	return v.ToVector()
}

func (v ArrowVector) ToVector() Vector {
	elem := make([]Value, v.Len())
	for i := 0; i < len(elem); i++ {
		elem[i] = v.Get(i)
	}
	return NewVector(elem)
}

func (v ArrowVector) toType(op string, conf *config.Config, which valueType) Value {
	switch which {
	case arrowVectorType:
		return v.ToVector()
	case vectorType:
		return v.ToVector()
	case matrixType:
		return NewMatrix([]int{v.Len()}, v.ToVector())
	}
	Errorf("%s: cannot convert arrowVector to %s", op, which)
	return nil
}

/*
func (v ArrowVector) sameLength(x ArrowVector) {
	if len(v) != len(x) {
		Errorf("length mismatch: %d %d", len(v), len(x))
	}
}
*/

// n := copy(dst, src[j:])
// copy(dst[n:n+j], src[:j])
// rotate returns a copy of v with elements rotated left by n.
func (v ArrowVector) Len() int {
	return v.resolver.NumRows
}

func (v ArrowVector) rotate(n int) Value {
	if v.Len() == 0 {
		return v
	}
	if v.Len() == 1 {
		return v.Get(0)
	}
	n %= v.Len()
	if n < 0 {
		n += v.Len()
	}
	elems := make([]Value, v.Len())
	// doRotate
	j := n % len(elems)
	z := 0
	for i := j; i < v.Len(); i++ { //, v := range v[j:] {
		elems[z] = v.Get(i)
		z++
	}
	for i := 0; i < j; i++ { //, v := range v[j:] {
		elems[z] = v.Get(i)
		z++
	}

	// doRotate(elems, v, n%len(elems))
	return NewVector(elems)
}

// grade returns as a ArrowVector the indexes that sort the vector into increasing order
func (v ArrowVector) grade(c Context) Vector {
	x := make([]int, v.Len())
	for i := range x {
		x[i] = i
	}
	sort.SliceStable(x, func(i, j int) bool {
		return toBool(c.EvalBinary(v.Get(x[i]), "<", v.Get(x[j])))
	})
	origin := c.Config().Origin()
	for i := range x {
		x[i] += origin
	}
	return NewIntVector(x)
}

// reverse returns the reversal of a vector.
func (v ArrowVector) reverse() Vector {
	r := v.Copy()
	for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return r
}

// membership creates a vector of size len(u) reporting
// whether each element is an element of v.
// Algorithm is O(nV log nV + nU log nV) where nU==len(u) and nV==len(V).
/*
func membershipArrow(c Context, u, v ArrowVector) []Value {
	values := make([]Value, len(u))
	sortedV := v.sortedCopy(c)
	work := 2 * (1 + int(math.Log2(float64(len(v)))))
	pfor(true, work, len(values), func(lo, hi int) {
		for i := lo; i < hi; i++ {
			values[i] = toInt(sortedV.contains(c, u[i]))
		}
	})
	return values
}
*/

// sortedCopy returns a copy of v, in ascending sorted order.
func (v ArrowVector) sortedCopy(c Context) Vector {
	sortedV := v.Copy()
	sort.Slice(sortedV, func(i, j int) bool {
		return c.EvalBinary(sortedV[i], "<", sortedV[j]) == Int(1)
	})
	return sortedV
}

// contains reports whether x is in v, which must be already in ascending
// sorted order.
func (v ArrowVector) contains(c Context, x Value) bool {
	pos := sort.Search(v.Len(), func(j int) bool {
		return c.EvalBinary(v.Get(j), ">=", x) == Int(1)
	})
	return pos < v.Len() && c.EvalBinary(v.Get(pos), "==", x) == Int(1)
}

func (v ArrowVector) shrink() Value {
	if v.Len() == 1 {
		return v.Get(0) // TODO(twg) need to figure out floats
	}
	return v
}
