package spannerdef

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	dbadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	gax "github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type transportServer struct {
	databasepb.UnimplementedDatabaseAdminServer
	longrunningpb.UnimplementedOperationsServer
	dump   func(context.Context, *databasepb.GetDatabaseDdlRequest) (*databasepb.GetDatabaseDdlResponse, error)
	update func(context.Context, *databasepb.UpdateDatabaseDdlRequest) (*longrunningpb.Operation, error)
	get    func(context.Context, *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error)
}

func (s *transportServer) GetDatabaseDdl(ctx context.Context, r *databasepb.GetDatabaseDdlRequest) (*databasepb.GetDatabaseDdlResponse, error) {
	return s.dump(ctx, r)
}
func (s *transportServer) UpdateDatabaseDdl(ctx context.Context, r *databasepb.UpdateDatabaseDdlRequest) (*longrunningpb.Operation, error) {
	return s.update(ctx, r)
}
func (s *transportServer) GetOperation(ctx context.Context, r *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error) {
	return s.get(ctx, r)
}
func transportDB(t *testing.T, s *transportServer) *SpannerDatabase {
	t.Helper()
	t.Setenv("SPANNER_EMULATOR_HOST", "")
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	databasepb.RegisterDatabaseAdminServer(server, s)
	longrunningpb.RegisterOperationsServer(server, s)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	admin, err := dbadmin.NewDatabaseAdminClient(context.Background(), option.WithEndpoint("passthrough:///test"), option.WithoutAuthentication(), option.WithGRPCDialOption(grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() })), option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })
	admin.CallOptions.UpdateDatabaseDdl = []gax.CallOption{gax.WithRetry(func() gax.Retryer {
		return gax.OnCodes([]codes.Code{codes.Unavailable}, gax.Backoff{Initial: time.Millisecond, Max: time.Millisecond})
	})}
	return &SpannerDatabase{adminClient: admin, databasePath: "projects/p/instances/i/databases/d"}
}
func completedOperation(name, db string, statements []string) *longrunningpb.Operation {
	metadata, _ := anypb.New(&databasepb.UpdateDatabaseDdlMetadata{Database: db, Statements: statements})
	response, _ := anypb.New(&emptypb.Empty{})
	return &longrunningpb.Operation{Name: name, Metadata: metadata, Done: true, Result: &longrunningpb.Operation_Response{Response: response}}
}
func TestOperationRetriesRetainIdentityAndResume(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		t.Run(fmt.Sprint(mismatch), func(t *testing.T) {
			calls := 0
			var id, name string
			statements := []string{"CREATE TABLE T(Id INT64) PRIMARY KEY(Id)"}
			service := &transportServer{}
			service.update = func(_ context.Context, r *databasepb.UpdateDatabaseDdlRequest) (*longrunningpb.Operation, error) {
				calls++
				if calls == 1 {
					id = r.OperationId
					name = r.Database + "/operations/" + id
					return nil, status.Error(codes.Unavailable, "lost response")
				}
				if id == "" || r.OperationId != id {
					return nil, status.Error(codes.InvalidArgument, "operation ID changed")
				}
				return nil, status.Error(codes.AlreadyExists, "accepted before disconnect")
			}
			service.get = func(_ context.Context, r *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error) {
				actual := statements
				if mismatch {
					actual = []string{"DROP TABLE Other"}
				}
				return completedOperation(r.Name, "projects/p/instances/i/databases/d", actual), nil
			}
			db := transportDB(t, service)
			var observed string
			db.operationObserver = func(n string) { observed = n }
			err := db.ExecDDLsContext(context.Background(), statements)
			require.Equal(t, 2, calls)
			require.Regexp(t, `^spannerdef_[a-f0-9]{32}$`, id)
			require.Equal(t, name, observed)
			if mismatch {
				require.ErrorContains(t, err, "statements do not match")
				var operationErr *DDLOperationError
				require.ErrorAs(t, err, &operationErr)
				require.Equal(t, name, operationErr.OperationName)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
func TestOperationErrorsKeepStatusAndName(t *testing.T) {
	service := &transportServer{update: func(context.Context, *databasepb.UpdateDatabaseDdlRequest) (*longrunningpb.Operation, error) {
		return nil, status.Error(codes.PermissionDenied, "denied")
	}}
	db := transportDB(t, service)
	err := db.ExecDDLs([]string{"DROP TABLE T"})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	var operationErr *DDLOperationError
	require.ErrorAs(t, err, &operationErr)
	require.Contains(t, operationErr.OperationName, db.databasePath+"/operations/spannerdef_")
	service.dump = func(context.Context, *databasepb.GetDatabaseDdlRequest) (*databasepb.GetDatabaseDdlResponse, error) {
		return nil, status.Error(codes.PermissionDenied, "denied")
	}
	_, err = db.DumpDDLs()
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}
func TestResumeDoesNotSubmitDDL(t *testing.T) {
	service := &transportServer{get: func(_ context.Context, r *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error) {
		return completedOperation(r.Name, "projects/p/instances/i/databases/d", []string{"CREATE TABLE T(Id INT64) PRIMARY KEY(Id)"}), nil
	}}
	db := transportDB(t, service)
	require.NoError(t, db.WaitDDLOperation(context.Background(), db.databasePath+"/operations/resume"))
	require.ErrorContains(t, db.WaitDDLOperation(context.Background(), "projects/other/instances/i/databases/d/operations/resume"), "must belong")
}
func TestTransportCancellation(t *testing.T) {
	service := &transportServer{dump: func(ctx context.Context, _ *databasepb.GetDatabaseDdlRequest) (*databasepb.GetDatabaseDdlResponse, error) {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}}
	db := transportDB(t, service)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := db.DumpDDLsContext(ctx)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded, "%v", err)
}
func TestFreshSubmissionsHaveDifferentIDs(t *testing.T) {
	var ids []string
	service := &transportServer{update: func(_ context.Context, r *databasepb.UpdateDatabaseDdlRequest) (*longrunningpb.Operation, error) {
		ids = append(ids, r.OperationId)
		return completedOperation(r.Database+"/operations/"+r.OperationId, r.Database, r.Statements), nil
	}}
	db := transportDB(t, service)
	for range 2 {
		require.NoError(t, db.ExecDDLs([]string{"CREATE TABLE T(Id INT64) PRIMARY KEY(Id)"}))
	}
	require.Len(t, ids, 2)
	require.NotEqual(t, ids[0], ids[1])
	require.False(t, strings.Contains(ids[0], "-"))
}

func TestExportPreservesServerOrder(t *testing.T) {
	statements := []string{
		"CREATE TABLE T(Id INT64) PRIMARY KEY(Id)",
		"CREATE INDEX I ON T(Id)",
		"ALTER TABLE T ADD CONSTRAINT C CHECK(Id > 0)",
	}
	service := &transportServer{dump: func(context.Context, *databasepb.GetDatabaseDdlRequest) (*databasepb.GetDatabaseDdlResponse, error) {
		return &databasepb.GetDatabaseDdlResponse{Statements: statements}, nil
	}}
	db := transportDB(t, service)
	dump, err := db.DumpDDLs()
	require.NoError(t, err)
	require.Equal(t, strings.Join(statements, ";\n\n")+";", dump)
	ddls, err := GenerateIdempotentDDLs(dump, dump, GeneratorConfig{})
	require.NoError(t, err)
	require.Empty(t, ddls)
	statements = nil
	dump, err = db.DumpDDLs()
	require.NoError(t, err)
	require.Equal(t, ";", dump)
}
