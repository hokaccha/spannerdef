package spannerdef_test

import s "github.com/hokaccha/spannerdef"

// Pin the legacy signatures and public struct layouts. Defined local types
// permit positional shape assertions without vet's imported-literal warning.
var (
	_ func(s.Config) (*s.SpannerDatabase, error)      = s.NewDatabase
	_ func(s.Config) (*s.SpannerAdminDatabase, error) = s.NewAdminDatabase
	_ func(s.Database, *s.Options)                    = s.Run
	_ func(s.Database, []string, bool, bool) error    = s.RunDDLs
	_ func(string) s.GeneratorConfig                  = s.ParseGeneratorConfig
	_ func([]string) (string, error)                  = s.ReadFiles
)

type configShape s.Config
type filterShape s.GeneratorConfig
type optionsShape s.Options

var (
	_ = configShape{"p", "i", "d", ""}
	_ = filterShape{nil, nil}
	_ = optionsShape{"", false, false, false, s.GeneratorConfig{}}
)
