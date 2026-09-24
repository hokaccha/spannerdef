package spannerdef

import (
	"fmt"
	"log"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	ProjectID  string
	InstanceID string
	DatabaseID string
	// ImpersonateServiceAccount, when set, makes every Spanner API call use
	// short-lived credentials for this service account obtained through the
	// IAM Credentials API with the caller's Application Default Credentials.
	ImpersonateServiceAccount string
}

type GeneratorConfig struct {
	TargetTables []string
	SkipTables   []string
}

// Database interface for Spanner
type Database interface {
	DumpDDLs() (string, error)
	ExecDDL(ddl string) error
	ExecDDLs(ddls []string) error
	Close() error
}

func RunDDLs(d Database, ddls []string, enableDrop bool, quiet bool) error {
	if !quiet {
		fmt.Println("-- Apply --")
	}

	plan, err := planDDLs(ddls, enableDrop)
	if err != nil {
		return err
	}
	validDDLs := make([]string, 0, len(plan))
	for _, step := range plan {
		if step.Skip {
			if !quiet {
				fmt.Printf("-- Skipped: %s;\n", step.SQL)
			}
			continue
		}
		if !quiet {
			fmt.Printf("%s;\n", step.SQL)
		}
		validDDLs = append(validDDLs, step.SQL)
	}

	if len(validDDLs) == 0 {
		return nil
	}

	// Execute all DDLs in batch
	return d.ExecDDLs(validDDLs)
}

func ParseGeneratorConfig(configFile string) GeneratorConfig {
	if configFile == "" {
		return GeneratorConfig{}
	}

	buf, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatal(err)
	}

	var config struct {
		TargetTables string `yaml:"target_tables"`
		SkipTables   string `yaml:"skip_tables"`
	}

	err = yaml.Unmarshal(buf, &config)
	if err != nil {
		log.Fatal(err)
	}

	var targetTables []string
	if config.TargetTables != "" {
		targetTables = strings.Split(strings.Trim(config.TargetTables, "\n"), "\n")
	}

	var skipTables []string
	if config.SkipTables != "" {
		skipTables = strings.Split(strings.Trim(config.SkipTables, "\n"), "\n")
	}

	return GeneratorConfig{
		TargetTables: targetTables,
		SkipTables:   skipTables,
	}
}
