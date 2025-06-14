package spannerdef_test

import (
	"fmt"
	"log"

	"github.com/ubie-sandbox/spannerdef"
)

func ExampleGenerateIdempotentDDLs() {
	currentDDLs := `
		CREATE TABLE Users (
			Id INT64 NOT NULL,
			Name STRING(100)
		) PRIMARY KEY (Id);
	`

	desiredDDLs := `
		CREATE TABLE Users (
			Id INT64 NOT NULL,
			Name STRING(100),
			Email STRING(255),
			CreatedAt TIMESTAMP
		) PRIMARY KEY (id);

		CREATE INDEX IdxEmail ON Users (Email);
	`

	ddls, err := spannerdef.GenerateIdempotentDDLs(desiredDDLs, currentDDLs, spannerdef.GeneratorConfig{})
	if err != nil {
		log.Fatal(err)
	}

	for _, ddl := range ddls {
		fmt.Println(ddl)
	}

	// Output:
	// ALTER TABLE Users ADD COLUMN Email STRING(255)
	// ALTER TABLE Users ADD COLUMN CreatedAt TIMESTAMP
	// CREATE INDEX IdxEmail ON Users (Email)
}
