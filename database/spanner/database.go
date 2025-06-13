package spanner

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"cloud.google.com/go/spanner"
	dbadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/ubie-sandbox/spannerdef/database"
)

type SpannerDatabase struct {
	client       *spanner.Client
	adminClient  *dbadmin.DatabaseAdminClient
	projectID    string
	instanceID   string
	databaseID   string
	databasePath string
}

func NewDatabase(config database.Config) (*SpannerDatabase, error) {
	ctx := context.Background()

	// Create Spanner client
	databasePath := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		config.ProjectID, config.InstanceID, config.DatabaseID)

	client, err := spanner.NewClient(ctx, databasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create Spanner client: %v", err)
	}

	// Create admin client for DDL operations
	adminClient, err := dbadmin.NewDatabaseAdminClient(ctx)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create admin client: %v", err)
	}

	return &SpannerDatabase{
		client:       client,
		adminClient:  adminClient,
		projectID:    config.ProjectID,
		instanceID:   config.InstanceID,
		databaseID:   config.DatabaseID,
		databasePath: databasePath,
	}, nil
}

func (db *SpannerDatabase) DumpDDLs() (string, error) {
	ctx := context.Background()

	// Get database schema
	req := &databasepb.GetDatabaseDdlRequest{
		Database: db.databasePath,
	}

	resp, err := db.adminClient.GetDatabaseDdl(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to get database DDL: %v", err)
	}

	// Sort statements for consistent output
	statements := make([]string, len(resp.Statements))
	copy(statements, resp.Statements)
	sort.Strings(statements)

	return strings.Join(statements, ";\n\n") + ";", nil
}

func (db *SpannerDatabase) ExecDDL(ddl string) error {
	return db.ExecDDLs([]string{ddl})
}

func (db *SpannerDatabase) ExecDDLs(ddls []string) error {
	ctx := context.Background()

	req := &databasepb.UpdateDatabaseDdlRequest{
		Database:   db.databasePath,
		Statements: ddls,
	}

	op, err := db.adminClient.UpdateDatabaseDdl(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to execute DDLs: %v", err)
	}

	// Wait for the operation to complete
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("DDL operation failed: %v", err)
	}

	return nil
}

func (db *SpannerDatabase) Close() error {
	db.client.Close()
	return db.adminClient.Close()
}
