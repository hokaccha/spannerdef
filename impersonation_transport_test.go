package spannerdef

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	dbadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

type rewriteIAMTransport struct {
	base        http.RoundTripper
	url         *url.URL
	sawDeadline chan<- bool
}

func (r rewriteIAMTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if r.sawDeadline != nil {
		_, ok := req.Context().Deadline()
		select {
		case r.sawDeadline <- ok:
		default:
		}
	}
	req = req.Clone(req.Context())
	req.URL.Scheme = r.url.Scheme
	req.URL.Host = r.url.Host
	req.Host = r.url.Host
	return r.base.RoundTrip(req)
}

func TestImpersonationRPCDeadlineAndReuse(t *testing.T) {
	t.Setenv("SPANNER_EMULATOR_HOST", "")
	firstIAM := make(chan struct{})
	var calls atomic.Int32
	iam := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/serviceAccounts/sa@example.invalid:generateAccessToken") {
			t.Errorf("unexpected IAM path: %s", r.URL.Path)
		}
		var body struct {
			Scope []string `json:"scope"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("IAM request body: %v", err)
		}
		if len(body.Scope) != 1 || body.Scope[0] != "https://www.googleapis.com/auth/cloud-platform" {
			t.Errorf("IAM scopes: %v", body.Scope)
		}
		if calls.Add(1) == 1 {
			close(firstIAM)
			<-r.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"accessToken": "impersonated-token",
			"expireTime":  time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
		})
	}))
	defer iam.Close()
	iamURL, err := url.Parse(iam.URL)
	require.NoError(t, err)
	sawDeadline := make(chan bool, 1)
	httpClient := iam.Client()
	httpClient.Transport = rewriteIAMTransport{base: httpClient.Transport, url: iamURL, sawDeadline: sawDeadline}

	service := &transportServer{dump: func(ctx context.Context, _ *databasepb.GetDatabaseDdlRequest) (*databasepb.GetDatabaseDdlResponse, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer impersonated-token" {
			t.Errorf("RPC authorization: %v", got)
		}
		return &databasepb.GetDatabaseDdlResponse{Statements: []string{"CREATE TABLE T (Id INT64) PRIMARY KEY (Id)"}}, nil
	}}
	grpcServer := grpc.NewServer()
	databasepb.RegisterDatabaseAdminServer(grpcServer, service)
	endpoint := httptest.NewUnstartedServer(grpcServer)
	endpoint.EnableHTTP2 = true
	endpoint.StartTLS()
	defer endpoint.Close()
	roots := x509.NewCertPool()
	roots.AddCert(endpoint.Certificate())

	construction, cancelConstruction := context.WithCancel(context.Background())
	opts, err := clientOptionsWithHTTPClient(construction, Config{ImpersonateServiceAccount: "sa@example.invalid"}, httpClient)
	require.NoError(t, err)
	opts = append(opts,
		option.WithEndpoint(endpoint.Listener.Addr().String()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}))),
	)
	admin, err := dbadmin.NewDatabaseAdminClient(construction, opts...)
	require.NoError(t, err)
	defer admin.Close()
	cancelConstruction()
	db := &SpannerDatabase{adminClient: admin, databasePath: "projects/p/instances/i/databases/d"}

	firstCtx, cancelFirst := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelFirst()
	firstResult := make(chan error, 1)
	go func() { _, err := db.DumpDDLsContext(firstCtx); firstResult <- err }()
	select {
	case <-firstIAM:
	case <-time.After(3 * time.Second):
		t.Fatal("IAM request did not start")
	}
	require.True(t, <-sawDeadline, "IAM request must carry the RPC deadline")
	// A waiter has its own deadline, even while another RPC is refreshing.
	shortCtx, cancelShort := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancelShort()
	start := time.Now()
	_, err = db.DumpDDLsContext(shortCtx)
	require.ErrorContains(t, err, context.DeadlineExceeded.Error())
	require.Less(t, time.Since(start), 500*time.Millisecond)
	cancelFirst()
	select {
	case err := <-firstResult:
		require.ErrorContains(t, err, context.Canceled.Error())
	case <-time.After(time.Second):
		t.Fatal("canceled IAM request blocked the RPC")
	}

	for range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		ddl, err := db.DumpDDLsContext(ctx)
		cancel()
		require.NoError(t, err)
		require.Contains(t, ddl, "CREATE TABLE T")
	}
	require.Equal(t, int32(2), calls.Load(), "successful token should be cached")
}
