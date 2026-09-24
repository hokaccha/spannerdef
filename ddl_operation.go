package spannerdef

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DatabaseOption configures clients created with NewDatabaseContext.
type DatabaseOption func(*SpannerDatabase)

// WithDDLOperationObserver reports each full operation name before submission.
// The callback runs synchronously and should return promptly. Its name can be
// used to inspect a request even when the submission response is lost.
func WithDDLOperationObserver(observer func(string)) DatabaseOption {
	return func(db *SpannerDatabase) { db.operationObserver = observer }
}

// DDLOperationError retains the operation name and original error. A failed or
// cancelled client wait does not imply server-side rollback or cancellation.
type DDLOperationError struct {
	OperationName string
	Err           error
}

func (e *DDLOperationError) Error() string {
	return fmt.Sprintf("DDL operation %s: %v", e.OperationName, e.Err)
}
func (e *DDLOperationError) Unwrap() error { return e.Err }

func (db *SpannerDatabase) executeDDLBatch(ctx context.Context, statements []string) error {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return fmt.Errorf("create DDL operation ID: %w", err)
	}
	id := "spannerdef_" + hex.EncodeToString(random[:])
	name := db.databasePath + "/operations/" + id
	if db.operationObserver != nil {
		db.operationObserver(name)
	}
	op, err := db.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: db.databasePath, Statements: statements, OperationId: id,
	})
	if status.Code(err) == codes.AlreadyExists {
		// An SDK retry can see ALREADY_EXISTS after the first response was lost.
		// Do not assume an existing operation belongs to this exact submission.
		if err = db.checkDDLOperation(ctx, name, statements); err == nil {
			op = db.adminClient.UpdateDatabaseDdlOperation(name)
		}
	}
	if err == nil {
		err = op.Wait(ctx)
	}
	if err != nil {
		return &DDLOperationError{OperationName: name, Err: err}
	}
	return nil
}

func (db *SpannerDatabase) checkDDLOperation(ctx context.Context, name string, statements []string) error {
	prefix := db.databasePath + "/operations/"
	id, ok := strings.CutPrefix(name, prefix)
	if !ok || id == "" || strings.Contains(id, "/") {
		return fmt.Errorf("operation must belong to database %s", db.databasePath)
	}
	op, err := db.adminClient.LROClient.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: name})
	if err != nil {
		return err
	}
	if op.Metadata == nil {
		return fmt.Errorf("cannot verify DDL operation: metadata is missing")
	}
	var metadata databasepb.UpdateDatabaseDdlMetadata
	if err := op.Metadata.UnmarshalTo(&metadata); err != nil {
		return fmt.Errorf("not a verifiable DDL operation: %w", err)
	}
	if metadata.Database != db.databasePath {
		return fmt.Errorf("DDL operation database does not match %s", db.databasePath)
	}
	if statements != nil && !slices.Equal(metadata.Statements, statements) {
		return fmt.Errorf("existing DDL operation statements do not match this submission")
	}
	return nil
}

// WaitDDLOperation resumes waiting for an existing DDL operation in this
// database. It never resubmits DDL or starts any later batches.
func (db *SpannerDatabase) WaitDDLOperation(ctx context.Context, name string) error {
	err := db.checkDDLOperation(ctx, name, nil)
	if err == nil {
		err = db.adminClient.UpdateDatabaseDdlOperation(name).Wait(ctx)
	}
	if err != nil {
		return &DDLOperationError{OperationName: name, Err: err}
	}
	return nil
}
