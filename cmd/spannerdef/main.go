package main

import (
	"fmt"
	"log"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/ubie-sandbox/spannerdef"
	"github.com/ubie-sandbox/spannerdef/database"
	"github.com/ubie-sandbox/spannerdef/database/spanner"
)

var version = "dev"

// parseOptions parses command line options
func parseOptions(args []string) (database.Config, *spannerdef.Options) {
	var opts struct {
		ProjectID  string   `short:"p" long:"project" description:"Google Cloud Project ID (or set SPANNER_PROJECT_ID)" value-name:"project_id"`
		InstanceID string   `short:"i" long:"instance" description:"Spanner Instance ID (or set SPANNER_INSTANCE_ID)" value-name:"instance_id"`
		DatabaseID string   `short:"d" long:"database" description:"Spanner Database ID (or set SPANNER_DATABASE_ID)" value-name:"database_id"`
		File       []string `long:"file" description:"Read desired SQL from the file, rather than stdin" value-name:"sql_file" default:"-"`
		DryRun     bool     `long:"dry-run" description:"Don't run DDLs but just show them"`
		Export     bool     `long:"export" description:"Just dump the current schema to stdout"`
		EnableDrop bool     `long:"enable-drop" description:"Enable destructive changes such as DROP TABLE, DROP INDEX"`
		Config     string   `long:"config" description:"YAML file to specify: target_tables, skip_tables"`
		Help       bool     `long:"help" description:"Show this help"`
		Version    bool     `long:"version" description:"Show this version"`
	}

	parser := flags.NewParser(&opts, flags.None)
	parser.Usage = "[OPTIONS] < desired.sql"
	_, err := parser.ParseArgs(args)
	if err != nil {
		log.Fatal(err)
	}

	if opts.Help {
		parser.WriteHelp(os.Stdout)
		os.Exit(0)
	}

	if opts.Version {
		fmt.Println(version)
		os.Exit(0)
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
		log.Fatal("Project ID is required. Use --project or set SPANNER_PROJECT_ID environment variable.")
	}
	if opts.InstanceID == "" {
		log.Fatal("Instance ID is required. Use --instance or set SPANNER_INSTANCE_ID environment variable.")
	}
	if opts.DatabaseID == "" {
		log.Fatal("Database ID is required. Use --database or set SPANNER_DATABASE_ID environment variable.")
	}

	desiredFiles := spannerdef.ParseFiles(opts.File)

	var desiredDDLs string
	if !opts.Export {
		desiredDDLs, err = spannerdef.ReadFiles(desiredFiles)
		if err != nil {
			log.Fatalf("Failed to read '%v': %s", desiredFiles, err)
		}
	}

	options := spannerdef.Options{
		DesiredDDLs: desiredDDLs,
		DryRun:      opts.DryRun,
		Export:      opts.Export,
		EnableDrop:  opts.EnableDrop,
		Config:      database.ParseGeneratorConfig(opts.Config),
	}

	config := database.Config{
		ProjectID:  opts.ProjectID,
		InstanceID: opts.InstanceID,
		DatabaseID: opts.DatabaseID,
	}

	return config, &options
}

func main() {
	config, options := parseOptions(os.Args[1:])

	db, err := spanner.NewDatabase(config)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	spannerdef.Run(db, options)
}
