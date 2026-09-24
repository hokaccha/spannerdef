package spannerdef

import (
	"github.com/cloudspannerecosystem/memefish"
	"reflect"
)

// Compare default expressions structurally, preserving literal values while
// ignoring source positions, redundant parentheses and built-in function case.
func sameDefaultSQL(a, b string) bool {
	if a == b {
		return true
	}
	if a == "" || b == "" {
		return false
	}
	left, err := memefish.ParseExpr("", a)
	if err != nil {
		return false
	}
	right, err := memefish.ParseExpr("", b)
	if err != nil {
		return false
	}
	normalizeGenerationExpression(left, nil)
	normalizeGenerationExpression(right, nil)
	return equalGenerationAST(reflect.ValueOf(left), reflect.ValueOf(right))
}
