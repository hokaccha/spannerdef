package spannerdef

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestInterleaveEnforcementRoundTrip(t *testing.T) {
	for _, clause := range []string{"INTERLEAVE IN P", "INTERLEAVE IN PARENT P", "INTERLEAVE IN PARENT P ON DELETE CASCADE"} {
		t.Run(clause, func(t *testing.T) {
			desired := "CREATE TABLE P (Id INT64 NOT NULL) PRIMARY KEY (Id); CREATE TABLE C (Id INT64 NOT NULL, N INT64 NOT NULL) PRIMARY KEY (Id, N), " + clause
			ddls, err := GenerateIdempotentDDLs(desired, "", GeneratorConfig{})
			require.NoError(t, err)
			require.Len(t, ddls, 2)
			require.True(t, strings.HasSuffix(ddls[1], clause), ddls[1])
			diff, err := GenerateIdempotentDDLs(desired, strings.Join(ddls, ";"), GeneratorConfig{})
			require.NoError(t, err)
			require.Empty(t, diff)
		})
	}
}

func TestInterleaveEnforcementChangesAreNotIgnored(t *testing.T) {
	base := "CREATE TABLE C (Id INT64 NOT NULL) PRIMARY KEY (Id), INTERLEAVE IN "
	for _, pair := range [][2]string{{"P", "PARENT P"}, {"PARENT P", "P"}, {"P", "Q"}} {
		ddls, err := GenerateIdempotentDDLs(base+pair[1], base+pair[0], GeneratorConfig{})
		require.ErrorContains(t, err, "unsupported interleave change for table C")
		require.Empty(t, ddls)
	}
}
