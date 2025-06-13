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
		ProjectID  string   `short:"p" long:"project" description:"Google Cloud Project ID" value-name:"project_id" required:"true"`
		InstanceID string   `short:"i" long:"instance" description:"Spanner Instance ID" value-name:"instance_id" required:"true"`
		DatabaseID string   `short:"d" long:"database" description:"Spanner Database ID" value-name:"database_id" required:"true"`
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
