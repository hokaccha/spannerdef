package spannerdef

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"cloud.google.com/go/spanner"
	dbadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"
)

// clientOptions returns the client options shared by the data and admin
// clients. Without impersonation it returns nil, so the libraries fall back to
// their defaults (Application Default Credentials, or the emulator when
// SPANNER_EMULATOR_HOST is set). With impersonation it mints short-lived
// credentials for the target service account through the IAM Credentials API
// and hands them to the clients as a token source, which both the classic and
// the new auth transports honour. The emulator ignores credentials, so
// impersonation is skipped there instead of failing to configure.
//
// base options configure the IAM Credentials client used for minting; tests
// pass a static token source, production passes nothing (ADC).
func clientOptions(ctx context.Context, config Config, base ...option.ClientOption) ([]option.ClientOption, error) {
	if config.ImpersonateServiceAccount == "" || os.Getenv("SPANNER_EMULATOR_HOST") != "" {
		return nil, nil
	}
	tokenSource, err := impersonate.CredentialsTokenSource(ctx, impersonate.CredentialsConfig{
		TargetPrincipal: config.ImpersonateServiceAccount,
		Scopes:          []string{"https://www.googleapis.com/auth/cloud-platform"},
	}, base...)
	if err != nil {
		return nil, fmt.Errorf("failed to impersonate %s: %w", config.ImpersonateServiceAccount, err)
	}
	return []option.ClientOption{option.WithTokenSource(tokenSource)}, nil
}

type SpannerDatabase struct {
	client       *spanner.Client
	adminClient  *dbadmin.DatabaseAdminClient
	projectID    string
	instanceID   string
	databaseID   string
	databasePath string
}

func NewDatabase(config Config) (*SpannerDatabase, error) {
	ctx := context.Background()

	// Create Spanner client
	databasePath := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		config.ProjectID, config.InstanceID, config.DatabaseID)

	opts, err := clientOptions(ctx, config)
	if err != nil {
		return nil, err
	}

	client, err := spanner.NewClient(ctx, databasePath, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Spanner client: %v", err)
	}

	// Create admin client for DDL operations
	adminClient, err := dbadmin.NewDatabaseAdminClient(ctx, opts...)
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
	adminClient  *dbadmin.DatabaseAdminClient
	projectID    string
	instanceID   string
	databaseID   string
	databasePath string
	instancePath string
}

func NewAdminDatabase(config Config) (*SpannerAdminDatabase, error) {
	ctx := context.Background()

	opts, err := clientOptions(ctx, config)
	if err != nil {
		return nil, err
	}

	// Create admin client for database operations
	adminClient, err := dbadmin.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create database admin client: %v", err)
	}

	databasePath := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		config.ProjectID, config.InstanceID, config.DatabaseID)
	instancePath := fmt.Sprintf("projects/%s/instances/%s",
		config.ProjectID, config.InstanceID)

	return &SpannerAdminDatabase{
		adminClient:  adminClient,
		projectID:    config.ProjectID,
		instanceID:   config.InstanceID,
		databaseID:   config.DatabaseID,
		databasePath: databasePath,
		instancePath: instancePath,
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
	return db.adminClient.Close()
}

// DatabaseAdminClient returns the database admin client for testing
func (db *SpannerAdminDatabase) DatabaseAdminClient() *dbadmin.DatabaseAdminClient {
	return db.adminClient
}
