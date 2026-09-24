package spannerdef

import (
	"context"
	"fmt"
	"testing"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDDLBatchesPreserveOrderAndNewTableGroups(t *testing.T) {
	var indexes []string
	for i := 0; i < 21; i++ {
		indexes = append(indexes, fmt.Sprintf("CREATE INDEX I%d ON T(Id)", i))
	}
	batches := ddlBatches(indexes)
	require.Len(t, batches, 3)
	require.Len(t, batches[0], 10)
	require.Len(t, batches[1], 10)
	require.Len(t, batches[2], 1)
	var joined []string
	for _, batch := range batches {
		joined = append(joined, batch...)
	}
	require.Equal(t, indexes, joined)
	creation := append([]string{"CREATE TABLE T(Id INT64) PRIMARY KEY(Id)"}, indexes...)
	require.Equal(t, [][]string{creation}, ddlBatches(creation))
	// IF NOT EXISTS does not guarantee an empty, newly created table.
	conditional := append([]string{"CREATE TABLE IF NOT EXISTS T(Id INT64) PRIMARY KEY(Id)"}, indexes...)
	require.Len(t, ddlBatches(conditional), 3)
	constrained := append([]string{"CREATE TABLE T(Id INT64, CONSTRAINT FK FOREIGN KEY(Id) REFERENCES Other(Id)) PRIMARY KEY(Id)"}, indexes...)
	require.Len(t, ddlBatches(constrained), 3)
	// Existing-table work between CREATE TABLE and its indexes is a barrier.
	barrier := append([]string{"CREATE TABLE T(Id INT64) PRIMARY KEY(Id)", "CREATE INDEX Existing ON Other(Id)"}, indexes...)
	batches = ddlBatches(barrier)
	require.Len(t, batches, 3)
	// Unknown/new server syntax must retain pass-through support.
	require.Equal(t, [][]string{{"NEW SERVER DDL"}}, ddlBatches([]string{"NEW SERVER DDL"}))
	require.Empty(t, ddlBatches(nil))
}
func TestBatchesStopAtFirstFailure(t *testing.T) {
	var sizes []int
	service := &transportServer{update: func(_ context.Context, r *databasepb.UpdateDatabaseDdlRequest) (*longrunningpb.Operation, error) {
		sizes = append(sizes, len(r.Statements))
		if len(sizes) == 2 {
			return nil, status.Error(codes.InvalidArgument, "invalid second batch")
		}
		return completedOperation(r.Database+"/operations/"+r.OperationId, r.Database, r.Statements), nil
	}}
	db := transportDB(t, service)
	var statements []string
	for i := 0; i < 21; i++ {
		statements = append(statements, fmt.Sprintf("CREATE INDEX I%d ON T(Id)", i))
	}
	err := RunDDLsContext(context.Background(), db, statements, false, true)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, []int{10, 10}, sizes)
}
func TestBatchesWaitBeforeSubmittingNext(t *testing.T) {
	var events []string
	var lastRequest *databasepb.UpdateDatabaseDdlRequest
	service := &transportServer{}
	service.update = func(_ context.Context, r *databasepb.UpdateDatabaseDdlRequest) (*longrunningpb.Operation, error) {
		events = append(events, fmt.Sprintf("submit:%d", len(r.Statements)))
		lastRequest = r
		return &longrunningpb.Operation{Name: r.Database + "/operations/" + r.OperationId}, nil
	}
	service.get = func(_ context.Context, r *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error) {
		events = append(events, "wait")
		return completedOperation(r.Name, lastRequest.Database, lastRequest.Statements), nil
	}
	db := transportDB(t, service)
	var statements []string
	for i := 0; i < 11; i++ {
		statements = append(statements, fmt.Sprintf("CREATE INDEX I%d ON T(Id)", i))
	}
	require.NoError(t, db.ExecDDLs(statements))
	require.Equal(t, []string{"submit:10", "wait", "submit:1", "wait"}, events)
}
func TestWholePlanValidatedBeforeAnyBatch(t *testing.T) {
	db := &legacyRuntimeDB{}
	statements := []string{"CREATE TABLE T(Id INT64) PRIMARY KEY(Id)", "DROP TABLE Old", "ALTER TABLE Existing ALTER COLUMN V STRING(10)"}
	require.Error(t, RunDDLs(db, statements, false, true))
	require.Zero(t, db.calls)
}
