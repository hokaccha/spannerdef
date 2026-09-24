package spannerdef

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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

// ContextDatabase is an optional extension; Database itself is unchanged.
type ContextDatabase interface {
	Database
	DumpDDLsContext(context.Context) (string, error)
	ExecDDLsContext(context.Context, []string) error
}

func RunDDLs(d Database, ddls []string, enableDrop bool, quiet bool) error {
	return RunDDLsContext(context.Background(), d, ddls, enableDrop, quiet)
}

// RunDDLsContext validates the complete drop-filtered plan before execution.
func RunDDLsContext(ctx context.Context, d Database, ddls []string, enableDrop bool, quiet bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !quiet {
		if _, err := fmt.Fprintln(os.Stdout, "-- Apply --"); err != nil {
			return fmt.Errorf("write apply plan: %w", err)
		}
	}

	plan, err := planDDLs(ddls, enableDrop)
	if err != nil {
		return err
	}
	validDDLs := make([]string, 0, len(plan))
	for _, step := range plan {
		if step.Skip {
			if !quiet {
				if _, err := fmt.Fprintf(os.Stdout, "-- Skipped: %s;\n", step.SQL); err != nil {
					return fmt.Errorf("write apply plan: %w", err)
				}
			}
			continue
		}
		if !quiet {
			if _, err := fmt.Fprintf(os.Stdout, "%s;\n", step.SQL); err != nil {
				return fmt.Errorf("write apply plan: %w", err)
			}
		}
		validDDLs = append(validDDLs, step.SQL)
	}

	if len(validDDLs) == 0 {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	if contextual, ok := d.(interface {
		ExecDDLsContext(context.Context, []string) error
	}); ok {
		return contextual.ExecDDLsContext(ctx, validDDLs)
	}
	return d.ExecDDLs(validDDLs)
}

// ParseGeneratorConfig retains the legacy exit-on-error behavior.
// Deprecated: use ParseGeneratorConfigChecked to handle configuration errors.
func ParseGeneratorConfig(configFile string) GeneratorConfig {
	config, err := ParseGeneratorConfigChecked(configFile)
	if err != nil {
		log.Fatal(err)
	}
	return config
}

// ParseGeneratorConfigChecked rejects unknown fields and extra YAML documents
// before any schema can be planned. Empty files retain their previous meaning.
func ParseGeneratorConfigChecked(configFile string) (GeneratorConfig, error) {
	if configFile == "" {
		return GeneratorConfig{}, nil
	}
	buf, err := os.ReadFile(configFile)
	if err != nil {
		return GeneratorConfig{}, fmt.Errorf("read config %s: %w", configFile, err)
	}
	var config struct {
		TargetTables string `yaml:"target_tables"`
		SkipTables   string `yaml:"skip_tables"`
	}
	decoder := yaml.NewDecoder(bytes.NewReader(buf))
	decoder.KnownFields(true)
	if err := decoder.Decode(&config); err != nil && !errors.Is(err, io.EOF) {
		return GeneratorConfig{}, fmt.Errorf("parse config %s: %w", configFile, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			err = fmt.Errorf("only one YAML document is allowed")
		}
		return GeneratorConfig{}, fmt.Errorf("parse config %s: %w", configFile, err)
	}
	var targetTables, skipTables []string
	if config.TargetTables != "" {
		targetTables = strings.Split(strings.Trim(config.TargetTables, "\n"), "\n")
	}
	if config.SkipTables != "" {
		skipTables = strings.Split(strings.Trim(config.SkipTables, "\n"), "\n")
	}
	return GeneratorConfig{TargetTables: targetTables, SkipTables: skipTables}, nil
}
