package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hokaccha/spannerdef"
	"github.com/jessevdk/go-flags"
)

var (
	version   = "dev"
	buildDate = "unknown"
)

type commandOptions struct {
	resumeOperation string
	config          spannerdef.Config
	options         spannerdef.Options
	timeout         time.Duration
}

func parseCommand(args []string) (*commandOptions, error) {
	var opts struct {
		ResumeOperation           string        `long:"resume-operation" description:"Wait for an existing DDL operation without submitting a new plan"`
		Timeout                   time.Duration `long:"timeout" description:"Maximum time for database operations (for example 30m; 0 disables the deadline)"`
		ProjectID                 string        `short:"p" long:"project" description:"Google Cloud Project ID (or set SPANNER_PROJECT_ID)" value-name:"project_id"`
		InstanceID                string        `short:"i" long:"instance" description:"Spanner Instance ID (or set SPANNER_INSTANCE_ID)" value-name:"instance_id"`
		DatabaseID                string        `short:"d" long:"database" description:"Spanner Database ID (or set SPANNER_DATABASE_ID)" value-name:"database_id"`
		File                      []string      `long:"file" description:"Read desired SQL from the file, rather than stdin" value-name:"sql_file" default:"-"`
		DryRun                    bool          `long:"dry-run" description:"Don't run DDLs but just show them"`
		Export                    bool          `long:"export" description:"Just dump the current schema to stdout"`
		EnableDrop                bool          `long:"enable-drop" description:"Enable destructive changes such as DROP TABLE, DROP INDEX"`
		Config                    string        `long:"config" description:"YAML file to specify: target_tables, skip_tables"`
		ImpersonateServiceAccount string        `long:"impersonate-service-account" description:"Run as this service account using short-lived credentials from the IAM Credentials API (the caller needs roles/iam.serviceAccountTokenCreator on it)" value-name:"email"`
		Help                      bool          `long:"help" description:"Show this help"`
		Version                   bool          `long:"version" description:"Show this version"`
	}

	parser := flags.NewParser(&opts, flags.None)
	parser.Usage = "[OPTIONS] < desired.sql"
	_, err := parser.ParseArgs(args)
	if err != nil {
		return nil, err
	}

	if opts.Help {
		parser.WriteHelp(os.Stdout)
		return nil, nil
	}

	if opts.Version {
		fmt.Printf("spannerdef %s (built: %s)\n", version, buildDate)
		return nil, nil
	}

	// Use environment variables as defaults if CLI args are not provided
	if opts.ProjectID == "" {
		opts.ProjectID = os.Getenv("SPANNER_PROJECT_ID")
	}
	if opts.InstanceID == "" {
		opts.InstanceID = os.Getenv("SPANNER_INSTANCE_ID")
	}
	if opts.DatabaseID == "" {
		opts.DatabaseID = os.Getenv("SPANNER_DATABASE_ID")
	}

	// Validate required fields
	if opts.ProjectID == "" {
		return nil, fmt.Errorf("project ID is required; use --project or SPANNER_PROJECT_ID")
	}
	if opts.InstanceID == "" {
		return nil, fmt.Errorf("instance ID is required; use --instance or SPANNER_INSTANCE_ID")
	}
	if opts.DatabaseID == "" {
		return nil, fmt.Errorf("database ID is required; use --database or SPANNER_DATABASE_ID")
	}

	if opts.ResumeOperation != "" && (opts.Export || opts.DryRun || opts.EnableDrop || opts.Config != "" || (parser.FindOptionByLongName("file").IsSet() && !parser.FindOptionByLongName("file").IsSetDefault())) {
		return nil, fmt.Errorf("--resume-operation cannot be combined with --export, --dry-run, --enable-drop, --config, or --file")
	}
	if opts.Timeout < 0 {
		return nil, fmt.Errorf("--timeout must be nonnegative")
	}
	generatorConfig, err := spannerdef.ParseGeneratorConfigChecked(opts.Config)
	if err != nil {
		return nil, err
	}

	desiredFiles := spannerdef.ParseFiles(opts.File)

	var desiredDDLs string
	if !opts.Export && opts.ResumeOperation == "" {
		desiredDDLs, err = spannerdef.ReadFiles(desiredFiles)
		if err != nil {
			return nil, fmt.Errorf("read %v: %w", desiredFiles, err)
		}
	}

	options := spannerdef.Options{
		DesiredDDLs: desiredDDLs,
		DryRun:      opts.DryRun,
		Export:      opts.Export,
		EnableDrop:  opts.EnableDrop,
		Config:      generatorConfig,
	}

	config := spannerdef.Config{
		ProjectID:                 opts.ProjectID,
		InstanceID:                opts.InstanceID,
		DatabaseID:                opts.DatabaseID,
		ImpersonateServiceAccount: opts.ImpersonateServiceAccount,
	}

	return &commandOptions{config: config, options: options, timeout: opts.Timeout, resumeOperation: opts.ResumeOperation}, nil
}

func runCommand(ctx context.Context, args []string) error {
	command, err := parseCommand(args)
	if err != nil || command == nil {
		return err
	}
	// Keep the default signal behavior while reading potentially blocking input.
	// Once clients can be created, cancellation lets their deferred cleanup run.
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	if command.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, command.timeout)
		defer cancel()
	}
	db, err := spannerdef.NewDatabaseContext(ctx, command.config, spannerdef.WithDDLOperationObserver(func(name string) {
		fmt.Fprintf(os.Stderr, "DDL operation: %s\n", name)
	}))
	if err != nil {
		return err
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("failed to close Spanner clients: %v", err)
		}
	}()
	if command.resumeOperation != "" {
		return db.WaitDDLOperation(ctx, command.resumeOperation)
	}
	return spannerdef.RunContext(ctx, db, &command.options)
}

func main() {
	err := runCommand(context.Background(), os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
