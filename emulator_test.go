package spannerdef

import (
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"context"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if os.Getenv("SPANNER_TEST_EMULATOR") == "1" {
		if err := prepareTestEmulator(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}
func prepareTestEmulator() error {
	// This helper must never create infrastructure on a managed Spanner endpoint.
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		return fmt.Errorf("SPANNER_TEST_EMULATOR requires SPANNER_EMULATOR_HOST")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()
	project := getEnvOrDefault("SPANNER_PROJECT_ID", "default")
	id := getEnvOrDefault("SPANNER_INSTANCE_ID", "default")
	op, err := client.CreateInstance(ctx, &instancepb.CreateInstanceRequest{Parent: "projects/" + project, InstanceId: id, Instance: &instancepb.Instance{Config: "projects/" + project + "/instanceConfigs/emulator-config", DisplayName: "spannerdef tests", NodeCount: 1}})
	if status.Code(err) == codes.AlreadyExists {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = op.Wait(ctx)
	return err
}
