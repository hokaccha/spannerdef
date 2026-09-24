//go:build !windows

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type signalTestServer struct {
	databasepb.UnimplementedDatabaseAdminServer
	started  chan struct{}
	canceled chan struct{}
	blockRPC bool
}

func (s *signalTestServer) GetDatabaseDdl(ctx context.Context, _ *databasepb.GetDatabaseDdlRequest) (*databasepb.GetDatabaseDdlResponse, error) {
	s.started <- struct{}{}
	if s.blockRPC {
		<-ctx.Done()
		s.canceled <- struct{}{}
		return nil, ctx.Err()
	}
	return &databasepb.GetDatabaseDdlResponse{Statements: []string{strings.Repeat("A", 1<<20)}}, nil
}

func TestSignalProcessHelper(t *testing.T) {
	if os.Getenv("SPANNERDEF_SIGNAL_TEST_HELPER") != "1" {
		return
	}
	err := runCommand(context.Background(), []string{"--project=p", "--instance=i", "--database=d", "--export"})
	if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
		os.Exit(42)
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(43)
}

func startSignalTestServer(t *testing.T, blockRPC bool) (string, *signalTestServer) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	service := &signalTestServer{started: make(chan struct{}, 1), canceled: make(chan struct{}, 1), blockRPC: blockRPC}
	server := grpc.NewServer()
	databasepb.RegisterDatabaseAdminServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	return listener.Addr().String(), service
}

func startSignalTestProcess(t *testing.T, host string, stdout *os.File) (*exec.Cmd, <-chan error, *bytes.Buffer) {
	t.Helper()
	self, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.Command(self, "-test.run=^TestSignalProcessHelper$")
	cmd.Env = append(os.Environ(), "SPANNERDEF_SIGNAL_TEST_HELPER=1", "SPANNER_EMULATOR_HOST="+host)
	cmd.Stdout = stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Start())
	finished := make(chan error, 1)
	go func() { finished <- cmd.Wait() }()
	return cmd, finished, &stderr
}

func awaitSignalTest(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func TestSecondSignalTerminatesBlockedOutput(t *testing.T) {
	for _, second := range []os.Signal{os.Interrupt, syscall.SIGTERM} {
		t.Run(second.String(), func(t *testing.T) {
			host, service := startSignalTestServer(t, false)
			r, w, err := os.Pipe()
			require.NoError(t, err)
			defer r.Close()
			defer w.Close()
			cmd, finished, stderr := startSignalTestProcess(t, host, w)
			waited := false
			defer func() {
				if !waited {
					_ = cmd.Process.Kill()
					select {
					case <-finished:
					case <-time.After(5 * time.Second):
						t.Error("child process did not exit after kill")
					}
				}
			}()
			awaitSignalTest(t, service.started, "export RPC")
			// Reading one byte proves the export write started; the remaining
			// megabyte fills the pipe while its read end stays open.
			require.NoError(t, r.SetReadDeadline(time.Now().Add(5*time.Second)))
			var first [1]byte
			_, err = r.Read(first[:])
			require.NoError(t, err)
			require.NoError(t, cmd.Process.Signal(os.Interrupt))
			select {
			case err := <-finished:
				waited = true
				t.Fatalf("first signal terminated blocked export: %v; stderr: %s", err, stderr.String())
			case <-time.After(200 * time.Millisecond):
			}
			require.NoError(t, cmd.Process.Signal(second))
			select {
			case err := <-finished:
				waited = true
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit, "stderr: %s", stderr.String())
				status, ok := exit.Sys().(syscall.WaitStatus)
				require.True(t, ok)
				require.True(t, status.Signaled(), "expected signal termination, got %v; stderr: %s", err, stderr.String())
				require.Equal(t, second, status.Signal())
			case <-time.After(5 * time.Second):
				t.Fatal("second signal did not terminate blocked export")
			}
		})
	}
}

func TestFirstSignalCancelsRPC(t *testing.T) {
	for _, first := range []os.Signal{os.Interrupt, syscall.SIGTERM} {
		t.Run(first.String(), func(t *testing.T) {
			host, service := startSignalTestServer(t, true)
			cmd, finished, stderr := startSignalTestProcess(t, host, nil)
			waited := false
			defer func() {
				if !waited {
					_ = cmd.Process.Kill()
					select {
					case <-finished:
					case <-time.After(5 * time.Second):
						t.Error("child process did not exit after kill")
					}
				}
			}()
			awaitSignalTest(t, service.started, "blocked RPC")
			require.NoError(t, cmd.Process.Signal(first))
			awaitSignalTest(t, service.canceled, "RPC cancellation")
			select {
			case err := <-finished:
				waited = true
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit, "stderr: %s", stderr.String())
				require.Equal(t, 42, exit.ExitCode(), "stderr: %s", stderr.String())
			case <-time.After(5 * time.Second):
				t.Fatal("first signal did not end the RPC")
			}
		})
	}
}
