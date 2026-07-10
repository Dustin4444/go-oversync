package oversync

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	managedIntegrationPostgresImage    = "postgres:17.10"
	managedIntegrationPostgresPassword = "oversync_test_password"
)

type managedIntegrationPostgresServer struct {
	containerID string
	host        string
	port        string
}

var (
	managedIntegrationPostgresOnce     sync.Once
	managedIntegrationPostgres         *managedIntegrationPostgresServer
	managedIntegrationPostgresErr      error
	managedIntegrationDatabaseSequence atomic.Uint64
	callerManagedIntegrationDatabaseMu sync.Mutex
)

func TestMain(m *testing.M) {
	exitCode := m.Run()
	if err := stopManagedIntegrationPostgres(); err != nil {
		fmt.Fprintf(os.Stderr, "stop managed integration PostgreSQL: %v\n", err)
		if exitCode == 0 {
			exitCode = 1
		}
	}
	os.Exit(exitCode)
}

func provisionIntegrationTestDatabase(t *testing.T, ctx context.Context) (string, bool) {
	t.Helper()

	if databaseURL := os.Getenv("TEST_DATABASE_URL"); databaseURL != "" {
		callerManagedIntegrationDatabaseMu.Lock()
		t.Cleanup(callerManagedIntegrationDatabaseMu.Unlock)
		return databaseURL, false
	}

	server, err := getManagedIntegrationPostgres()
	if err != nil {
		t.Fatalf("start managed integration PostgreSQL (set TEST_DATABASE_URL to use an existing database): %v", err)
	}

	databaseName := fmt.Sprintf(
		"oversync_test_%d_%d",
		os.Getpid(),
		managedIntegrationDatabaseSequence.Add(1),
	)
	if err := server.createDatabase(ctx, databaseName); err != nil {
		t.Fatalf("create isolated integration test database %s: %v", databaseName, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.dropDatabase(cleanupCtx, databaseName); err != nil {
			t.Errorf("drop isolated integration test database %s: %v", databaseName, err)
		}
	})

	return server.databaseURL(databaseName), true
}

func getManagedIntegrationPostgres() (*managedIntegrationPostgresServer, error) {
	managedIntegrationPostgresOnce.Do(func() {
		managedIntegrationPostgres, managedIntegrationPostgresErr = startManagedIntegrationPostgres()
	})
	return managedIntegrationPostgres, managedIntegrationPostgresErr
}

func startManagedIntegrationPostgres() (*managedIntegrationPostgresServer, error) {
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, fmt.Errorf("find docker CLI: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	containerName := fmt.Sprintf("go-oversync-test-%d-%d", os.Getpid(), time.Now().UnixNano())
	containerID, err := dockerOutput(ctx,
		"run",
		"--detach",
		"--rm",
		"--name", containerName,
		"--label", "com.mobiletoly.go-oversync.test=true",
		"--publish", "127.0.0.1::5432",
		"--env", "POSTGRES_USER=postgres",
		"--env", "POSTGRES_PASSWORD="+managedIntegrationPostgresPassword,
		"--env", "POSTGRES_DB=postgres",
		"--health-cmd", "pg_isready --username postgres --dbname postgres",
		"--health-interval", "250ms",
		"--health-timeout", "5s",
		"--health-retries", "120",
		managedIntegrationPostgresImage,
	)
	if err != nil {
		return nil, err
	}
	containerID = strings.TrimSpace(containerID)

	started := false
	defer func() {
		if !started {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = dockerOutput(cleanupCtx, "rm", "--force", containerID)
		}
	}()

	portOutput, err := dockerOutput(ctx, "port", containerID, "5432/tcp")
	if err != nil {
		return nil, err
	}
	host, port, err := parseDockerPublishedPort(portOutput)
	if err != nil {
		return nil, err
	}
	server := &managedIntegrationPostgresServer{
		containerID: containerID,
		host:        host,
		port:        port,
	}
	if err := server.waitUntilReady(ctx); err != nil {
		logs, logsErr := dockerOutput(context.Background(), "logs", "--tail", "50", containerID)
		if logsErr == nil && logs != "" {
			return nil, fmt.Errorf("wait for %s: %w; container logs: %s", managedIntegrationPostgresImage, err, logs)
		}
		return nil, fmt.Errorf("wait for %s: %w", managedIntegrationPostgresImage, err)
	}

	started = true
	return server, nil
}

func stopManagedIntegrationPostgres() error {
	if managedIntegrationPostgres == nil || managedIntegrationPostgres.containerID == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := dockerOutput(ctx, "rm", "--force", managedIntegrationPostgres.containerID)
	if err != nil && !strings.Contains(err.Error(), "No such container") {
		return err
	}
	return nil
}

func parseDockerPublishedPort(output string) (string, string, error) {
	line := strings.TrimSpace(strings.SplitN(output, "\n", 2)[0])
	host, port, err := net.SplitHostPort(line)
	if err != nil {
		return "", "", fmt.Errorf("parse docker published port %q: %w", line, err)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return host, port, nil
}

func dockerOutput(ctx context.Context, args ...string) (string, error) {
	command := exec.CommandContext(ctx, "docker", args...)
	output, err := command.Output()
	if err == nil {
		return strings.TrimSpace(string(output)), nil
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		stderr := strings.TrimSpace(string(exitErr.Stderr))
		if stderr != "" {
			return "", fmt.Errorf("docker %s: %w: %s", args[0], err, stderr)
		}
	}
	return "", fmt.Errorf("docker %s: %w", args[0], err)
}

func (s *managedIntegrationPostgresServer) databaseURL(databaseName string) string {
	databaseURL := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword("postgres", managedIntegrationPostgresPassword),
		Host:     net.JoinHostPort(s.host, s.port),
		Path:     "/" + databaseName,
		RawQuery: "sslmode=disable",
	}
	return databaseURL.String()
}

func (s *managedIntegrationPostgresServer) waitUntilReady(ctx context.Context) error {
	var lastErr error
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, time.Second)
		conn, err := pgx.Connect(attemptCtx, s.databaseURL("postgres"))
		if err == nil {
			err = conn.Ping(attemptCtx)
			closeErr := conn.Close(attemptCtx)
			if err == nil {
				err = closeErr
			}
		}
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err

		select {
		case <-ctx.Done():
			return fmt.Errorf("PostgreSQL did not become ready: %w (last connection error: %v)", ctx.Err(), lastErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (s *managedIntegrationPostgresServer) createDatabase(ctx context.Context, databaseName string) error {
	conn, err := pgx.Connect(ctx, s.databaseURL("postgres"))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	_, err = conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{databaseName}.Sanitize())
	return err
}

func (s *managedIntegrationPostgresServer) dropDatabase(ctx context.Context, databaseName string) error {
	conn, err := pgx.Connect(ctx, s.databaseURL("postgres"))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	_, err = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{databaseName}.Sanitize()+" WITH (FORCE)")
	return err
}
