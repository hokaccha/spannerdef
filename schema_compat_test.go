package spannerdef_test

import (
	"testing"

	spannerdef "github.com/hokaccha/spannerdef"
	"github.com/stretchr/testify/require"
)

func TestSchemaPositionalConstructionCompatibility(t *testing.T) {
	// A local defined type permits a positional literal without vet's warning
	// about unkeyed literals of imported types. Its fields still come from Schema.
	type positionalSchema spannerdef.Schema
	schema := spannerdef.Schema(positionalSchema{
		nil,
		nil,
		map[string]*spannerdef.Table{},
		map[string]*spannerdef.Index{},
	})
	ddls, err := spannerdef.GenerateDDLsChecked(&schema, &schema)
	require.NoError(t, err)
	require.Empty(t, ddls)
}
