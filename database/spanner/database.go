package spanner

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"cloud.google.com/go/spanner"
	dbadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instanceadmin "cloud.google.com/go/spanner/admin/instance/apiv1"
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

// SpannerAdminDatabase handles database lifecycle operations
type SpannerAdminDatabase struct {
	adminClient         *dbadmin.DatabaseAdminClient
	instanceAdminClient *instanceadmin.InstanceAdminClient
	projectID           string
	instanceID          string
	databaseID          string
	databasePath        string
	instancePath        string
}

func NewAdminDatabase(config database.Config) (*SpannerAdminDatabase, error) {
	ctx := context.Background()

	// Create admin client for database operations
	adminClient, err := dbadmin.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create database admin client: %v", err)
	}

	// Create instance admin client for instance operations
	instanceAdminClient, err := instanceadmin.NewInstanceAdminClient(ctx)
	if err != nil {
		adminClient.Close()
		return nil, fmt.Errorf("failed to create instance admin client: %v", err)
	}

	databasePath := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		config.ProjectID, config.InstanceID, config.DatabaseID)
	instancePath := fmt.Sprintf("projects/%s/instances/%s",
		config.ProjectID, config.InstanceID)

	return &SpannerAdminDatabase{
		adminClient:         adminClient,
		instanceAdminClient: instanceAdminClient,
		projectID:           config.ProjectID,
		instanceID:          config.InstanceID,
		databaseID:          config.DatabaseID,
		databasePath:        databasePath,
		instancePath:        instancePath,
	}, nil
}

func (db *SpannerAdminDatabase) CreateDatabase(ctx context.Context) error {
	req := &databasepb.CreateDatabaseRequest{
		Parent:          db.instancePath,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", db.databaseID),
	}

	op, err := db.adminClient.CreateDatabase(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to create database: %v", err)
	}

	// Wait for the operation to complete
	_, err = op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("database creation failed: %v", err)
	}

	return nil
}

func (db *SpannerAdminDatabase) DropDatabase(ctx context.Context) error {
	req := &databasepb.DropDatabaseRequest{
		Database: db.databasePath,
	}

	err := db.adminClient.DropDatabase(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to drop database: %v", err)
	}

	return nil
}

func (db *SpannerAdminDatabase) Close() error {
	if err := db.adminClient.Close(); err != nil {
		return err
	}
	return db.instanceAdminClient.Close()
}

// InstanceAdminClient provides access to the instance admin client
func (db *SpannerAdminDatabase) InstanceAdminClient() *instanceadmin.InstanceAdminClient {
	return db.instanceAdminClient
}

// DatabaseAdminClient provides access to the database admin client
func (db *SpannerAdminDatabase) DatabaseAdminClient() *dbadmin.DatabaseAdminClient {
	return db.adminClient
}
