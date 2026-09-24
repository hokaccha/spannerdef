package spannerdef

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/auth/credentials/impersonate"
	dbadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/option"
)

// clientOptions returns authentication options for admin clients.
// Without impersonation it returns nil, so the libraries fall back to
// their defaults (Application Default Credentials, or the emulator when
// SPANNER_EMULATOR_HOST is set). With impersonation it mints short-lived
// credentials for the target service account through the IAM Credentials API
// and hands them to the clients as context-aware credentials. The emulator ignores credentials, so
// impersonation is skipped there instead of failing to configure.
func clientOptions(ctx context.Context, config Config) ([]option.ClientOption, error) {
	return clientOptionsWithHTTPClient(ctx, config, nil)
}

// A supplied client is used by local tests to serve the IAM request. In
// production, NewCredentials discovers ADC and builds its authenticated client.
func clientOptionsWithHTTPClient(ctx context.Context, config Config, client *http.Client) ([]option.ClientOption, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config.ImpersonateServiceAccount == "" || os.Getenv("SPANNER_EMULATOR_HOST") != "" {
		return nil, nil
	}
	const scope = "https://www.googleapis.com/auth/cloud-platform"
	impersonation := &impersonate.CredentialsOptions{
		TargetPrincipal: config.ImpersonateServiceAccount,
		Scopes:          []string{scope},
		Client:          client,
	}
	if client == nil {
		// Resolve ADC once, rather than rediscovering it at every refresh.
		base, err := credentials.DetectDefault(&credentials.DetectOptions{
			Scopes: []string{scope}, UseSelfSignedJWT: true,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to impersonate %s: %w", config.ImpersonateServiceAccount, err)
		}
		impersonation.Credentials = base
	}
	creds, err := impersonate.NewCredentials(impersonation)
	if err != nil {
		return nil, fmt.Errorf("failed to impersonate %s: %w", config.ImpersonateServiceAccount, err)
	}
	first := creds.TokenProvider
	creds.TokenProvider = &refreshingImpersonation{
		newProvider: func() (auth.TokenProvider, error) {
			if first != nil {
				provider := first
				first = nil
				return provider, nil
			}
			fresh, err := impersonate.NewCredentials(impersonation)
			if err != nil {
				return nil, err
			}
			return fresh.TokenProvider, nil
		},
	}
	return []option.ClientOption{option.WithAuthCredentials(creds)}, nil
}

// refreshingImpersonation caches successful tokens while keeping the network
// request outside the mutex. Waiters can stop on their own RPC deadlines. A
// failed or canceled refresh leaves the provider reusable for the next call.
type refreshingImpersonation struct {
	mu          sync.Mutex
	token       *auth.Token
	refreshDone chan struct{}
	newProvider func() (auth.TokenProvider, error)
}

func (r *refreshingImpersonation) Token(ctx context.Context) (*auth.Token, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r.mu.Lock()
		if r.token != nil && r.token.Value != "" && (r.token.Expiry.IsZero() || time.Until(r.token.Expiry) > 30*time.Second) {
			token := r.token
			r.mu.Unlock()
			return token, nil
		}
		if done := r.refreshDone; done != nil {
			r.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				continue
			}
		}
		done := make(chan struct{})
		r.refreshDone = done
		r.mu.Unlock()
		provider, err := r.newProvider()
		var token *auth.Token
		if err == nil {
			token, err = provider.Token(ctx)
		}
		r.mu.Lock()
		if err == nil {
			r.token = token
		}
		r.refreshDone = nil
		close(done)
		r.mu.Unlock()
		return token, err
	}
}

type SpannerDatabase struct {
	operationObserver func(string)
	adminClient       *dbadmin.DatabaseAdminClient
	projectID         string
	instanceID        string
	databaseID        string
	databasePath      string
}

func NewDatabase(config Config) (*SpannerDatabase, error) {
	return NewDatabaseContext(context.Background(), config)
}

// NewDatabaseContext allows cancellation of client construction.
func NewDatabaseContext(ctx context.Context, config Config, options ...DatabaseOption) (*SpannerDatabase, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	databasePath := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		config.ProjectID, config.InstanceID, config.DatabaseID)

	// Retain the path validation previously performed by the data client,
	// without opening a session or starting its background workers.
	for _, component := range []string{config.ProjectID, config.InstanceID, config.DatabaseID} {
		if component == "" || strings.Contains(component, "/") {
			return nil, fmt.Errorf("invalid database name %q: project, instance, and database IDs must be nonempty path components", databasePath)
		}
	}

	// The credential provider outlives construction; each token request gets
	// its own RPC context from the gRPC transport.
	opts, err := clientOptions(ctx, config)
	if err != nil {
		return nil, err
	}

	// Create admin client for DDL operations
	adminClient, err := dbadmin.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create admin client: %w", err)
	}

	db := &SpannerDatabase{
		adminClient:  adminClient,
		projectID:    config.ProjectID,
		instanceID:   config.InstanceID,
		databaseID:   config.DatabaseID,
		databasePath: databasePath,
	}
	for _, option := range options {
		if option != nil {
			option(db)
		}
	}
	return db, nil
}

func (db *SpannerDatabase) DumpDDLs() (string, error) {
	return db.DumpDDLsContext(context.Background())
}

func (db *SpannerDatabase) DumpDDLsContext(ctx context.Context) (string, error) {

	// Get database schema
	req := &databasepb.GetDatabaseDdlRequest{
		Database: db.databasePath,
	}

	resp, err := db.adminClient.GetDatabaseDdl(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to get database DDL: %w", err)
	}

	// Preserve the server's statement order; alphabetical sorting can put
	// indexes and ALTER statements before the tables they depend on.
	return strings.Join(resp.Statements, ";\n\n") + ";", nil
}

func (db *SpannerDatabase) ExecDDL(ddl string) error {
	return db.ExecDDLs([]string{ddl})
}

func (db *SpannerDatabase) ExecDDLContext(ctx context.Context, ddl string) error {
	return db.ExecDDLsContext(ctx, []string{ddl})
}

func (db *SpannerDatabase) ExecDDLs(ddls []string) error {
	return db.ExecDDLsContext(context.Background(), ddls)
}

func (db *SpannerDatabase) ExecDDLsContext(ctx context.Context, ddls []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ddls) == 0 {
		return nil
	}

	for _, batch := range ddlBatches(ddls) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := db.executeDDLBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *SpannerDatabase) Close() error {
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
	return NewAdminDatabaseContext(context.Background(), config)
}

func NewAdminDatabaseContext(ctx context.Context, config Config) (*SpannerAdminDatabase, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	opts, err := clientOptions(ctx, config)
	if err != nil {
		return nil, err
	}

	// Create admin client for database operations
	adminClient, err := dbadmin.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create database admin client: %w", err)
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
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Wait for the operation to complete
	_, err = op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("database creation failed: %w", err)
	}

	return nil
}

func (db *SpannerAdminDatabase) DropDatabase(ctx context.Context) error {
	req := &databasepb.DropDatabaseRequest{
		Database: db.databasePath,
	}

	err := db.adminClient.DropDatabase(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
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
