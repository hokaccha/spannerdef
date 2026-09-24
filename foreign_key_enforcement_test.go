package spannerdef

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestForeignKeyEnforcementRoundTrip(t *testing.T) {
	base := "CREATE TABLE P (Id INT64 NOT NULL) PRIMARY KEY (Id); CREATE TABLE C (Id INT64 NOT NULL, PId INT64, CONSTRAINT FK FOREIGN KEY (PId) REFERENCES P(Id) %s) PRIMARY KEY (Id)"
	for _, enforcement := range []string{"", "ENFORCED", "NOT ENFORCED"} {
		t.Run(enforcement, func(t *testing.T) {
			desired := strings.Replace(base, "%s", enforcement, 1)
			ddls, err := GenerateIdempotentDDLs(desired, "", GeneratorConfig{})
			require.NoError(t, err)
			require.Len(t, ddls, 2)
			require.Equal(t, enforcement == "NOT ENFORCED", strings.Contains(ddls[1], "NOT ENFORCED"))
			diff, err := GenerateIdempotentDDLs(desired, strings.Join(ddls, ";"), GeneratorConfig{})
			require.NoError(t, err)
			require.Empty(t, diff)
		})
	}
}

func TestForeignKeyEnforcementAlterAndDiff(t *testing.T) {
	tables := "CREATE TABLE P (Id INT64 NOT NULL) PRIMARY KEY (Id); CREATE TABLE C (Id INT64 NOT NULL, PId INT64) PRIMARY KEY (Id);"
	fk := "ALTER TABLE C ADD CONSTRAINT FK FOREIGN KEY (PId) REFERENCES P(Id)"
	for _, pair := range [][2]string{{"", " NOT ENFORCED"}, {" NOT ENFORCED", ""}} {
		current := tables + fk + pair[0]
		desired := tables + fk + pair[1]
		ddls, err := GenerateIdempotentDDLs(desired, current, GeneratorConfig{})
		require.NoError(t, err)
		require.Equal(t, []string{"ALTER TABLE C DROP CONSTRAINT FK", "ALTER TABLE C ADD CONSTRAINT FK FOREIGN KEY (PId) REFERENCES P (Id)" + pair[1]}, ddls)
	}
	ddls, err := GenerateIdempotentDDLs(tables+fk+" ENFORCED", tables+fk, GeneratorConfig{})
	require.NoError(t, err)
	require.Empty(t, ddls)
}
