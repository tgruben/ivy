// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package value

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/gomem/gomem/pkg/dataframe"
	"robpike.io/ivy/config"
)

type ValueGetter interface {
	Get(i int) Value
	Len() int
}

type ArrowIntVector struct {
	col      *arrow.Column
	resolver dataframe.ChunkResolver
}

func (v ArrowIntVector) String() string {
	return "(" + v.Sprint(debugConf) + ")"
}

func (v ArrowIntVector) Sprint(conf *config.Config) string {
	return v.makeString(conf, !v.AllChars())
}

func (v ArrowIntVector) Rank() int {
	return 1
}

func (v ArrowIntVector) ProgString() string {
	// There is no such thing as a vector in program listings; they
	// are represented as a sliceExpr.
	panic("arrowvector.ProgString - cannot happen")
}

// makeString is like String but takes a flag specifying
// whether to put spaces between the elements. By
// default (that is, by calling String) spaces are suppressed
// if all the elements of the ArrowVector are Chars.
func (v ArrowIntVector) makeString(conf *config.Config, spaces bool) string {
	var b bytes.Buffer
	for i := 0; i < v.resolver.NumRows; i++ {
		if spaces && i > 0 {
			fmt.Fprint(&b, " ")
		}
		// fmt.Fprintf(&b, "%s", elem.Sprint(conf))
		fmt.Fprintf(&b, "%d", v.Get(i))
	}
	return b.String()
}

// AllChars reports whether the vector contains only Chars.
// TODO(twg) possibly only float/int support
func (v ArrowIntVector) AllChars() bool {
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
func (v ArrowIntVector) AllInts() bool {
	return true
	/*
		for _, c := range v {
			if _, ok := c.Inner().(Int); !ok {
				return false
			}
		}
		return true
	*/
}

// func NewArrowVector(elems []Value) ArrowVector {
func NewArrowVector(col *arrow.Column) ArrowIntVector {
	return ArrowIntVector{
		col:      col,
		resolver: dataframe.NewChunkResolver(col),
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

func (v ArrowIntVector) Get(i int) Value {
	c, offset := v.resolver.Resolve((i))
	x := v.col.Data().Chunk(c).(*array.Int64).Int64Values()
	return Int(x[offset])
}

func (v ArrowIntVector) Eval(Context) Value {
	return v
}

func (v ArrowIntVector) Inner() Value {
	return v
}

// Arrow Vectors are read only so copying converts to a regular veco
func (v ArrowIntVector) Copy() Vector {
	return v.ToVector()
}

func (v ArrowIntVector) ToVector() Vector {
	elem := make([]Value, v.Len())
	for i := 0; i < len(elem); i++ {
		elem[i] = v.Get(i)
	}
	return NewVector(elem)
}

func (v ArrowIntVector) toType(op string, conf *config.Config, which valueType) Value {
	switch which {
	case arrowVectorType:
		return v
	case vectorType:
		return v.ToVector()
	case matrixType:
		return NewMatrix([]int{v.Len()}, v.ToVector())
	}
	Errorf("%s: cannot convert arrowVector to %s", op, which)
	return nil
}

/*
func (v ArrowIntVector) sameLength(x ArrowIntVector) {
	if len(v) != len(x) {
		Errorf("length mismatch: %d %d", len(v), len(x))
	}
}
*/

// n := copy(dst, src[j:])
// copy(dst[n:n+j], src[:j])
// rotate returns a copy of v with elements rotated left by n.
func (v ArrowIntVector) Len() int {
	return v.resolver.NumRows
}

func (v ArrowIntVector) rotate(n int) Value {
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
func (v ArrowIntVector) grade(c Context) Vector {
	panic("GRADE")
	x := make([]int, v.Len())
	/*
		for i := range x {
			x[i] = i
		}
		sort.SliceStable(x, func(i, j int) bool {
			return toBool(c.EvalBinary(Int(v[x[i]]), "<", Int(v[x[j]])))
		})
		origin := c.Config().Origin()
		for i := range x {
			x[i] += origin
		}
	*/
	return NewIntVector(x)
}

// reverse returns the reversal of a vector.
func (v ArrowIntVector) reverse() Vector {
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
func (v ArrowIntVector) sortedCopy(c Context) Vector {
	sortedV := v.Copy()
	sort.Slice(sortedV, func(i, j int) bool {
		return c.EvalBinary(sortedV[i], "<", sortedV[j]) == Int(1)
	})
	return sortedV
}

// contains reports whether x is in v, which must be already in ascending
// sorted order.
func (v ArrowIntVector) contains(c Context, x Value) bool {
	pos := sort.Search(v.Len(), func(j int) bool {
		return c.EvalBinary(v.Get(j), ">=", x) == Int(1)
	})
	return pos < v.Len() && c.EvalBinary(v.Get(pos), "==", x) == Int(1)
}

func (v ArrowIntVector) shrink() Value {
	if v.Len() == 1 {
		return v.Get(0) // TODO(twg) need to figure out floats
	}
	return v
}
