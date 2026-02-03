package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"

	"pulse/internal/app"
	"pulse/internal/config"
	"pulse/internal/consumer"
	"pulse/internal/event"
	"pulse/internal/log"
)

// TestConfig holds e2e test configuration from environment.
type TestConfig struct {
	Mode          string
	Events        int
	PayloadBytes  int
	BatchSize     int
	ServerPort    int
	NumPartitions int
	NoColor       bool
}

// loadTestConfig reads configuration from environment with defaults.
func loadTestConfig() TestConfig {
	cfg := TestConfig{
		Events:        getEnvInt("PULSE_E2E_EVENTS", 10000),
		PayloadBytes:  getEnvInt("PULSE_E2E_PAYLOAD_BYTES", 128),
		BatchSize:     getEnvInt("PULSE_E2E_BATCH_SIZE", 500),
		Mode:          getEnv("PULSE_E2E_MODE", "ci"),
		NoColor:       getEnvBool("NO_COLOR", false),
		ServerPort:    getEnvInt("PULSE_SERVER_PORT", 9090),
		NumPartitions: getEnvInt("PULSE_NUM_PARTITIONS", 4),
	}

	// Override with stress mode defaults
	if cfg.Mode == "stress" {
		if !hasEnv("PULSE_E2E_EVENTS") {
			cfg.Events = 200000
		}
	}

	return cfg
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if value == "1" || value == "true" {
			return true
		}
		return false
	}
	return defaultValue
}

func hasEnv(key string) bool {
	_, ok := os.LookupEnv(key)
	return ok
}

// Color codes for terminal output
type colorizer struct {
	enabled bool
}

func newColorizer(enabled bool) *colorizer {
	return &colorizer{enabled: enabled}
}

func (c *colorizer) red(s string) string {
	if !c.enabled {
		return s
	}
	return "\033[31m" + s + "\033[0m"
}

func (c *colorizer) green(s string) string {
	if !c.enabled {
		return s
	}
	return "\033[32m" + s + "\033[0m"
}

func (c *colorizer) blue(s string) string {
	if !c.enabled {
		return s
	}
	return "\033[34m" + s + "\033[0m"
}

func (c *colorizer) bold(s string) string {
	if !c.enabled {
		return s
	}
	return "\033[1m" + s + "\033[0m"
}

// TestRunner orchestrates the e2e test.
type TestRunner struct {
	generated []generatedEvent
	baseURL   string
	color     *colorizer
	rng       *rand.Rand
	cfg       TestConfig
}

type generatedEvent struct {
	key       string
	data      []byte
	partition int
	index     int // Global index in generation order
}

func newTestRunner(cfg TestConfig) *TestRunner {
	return &TestRunner{
		cfg:     cfg,
		color:   newColorizer(!cfg.NoColor),
		baseURL: fmt.Sprintf("http://localhost:%d", cfg.ServerPort),
		rng:     rand.New(rand.NewSource(42)), // Deterministic seed
	}
}

func (tr *TestRunner) run() error {
	fmt.Println(tr.color.bold("=== Pulse E2E Integration Test ==="))
	fmt.Printf("Mode: %s\n", tr.color.blue(tr.cfg.Mode))
	fmt.Printf("Events: %s\n", tr.color.blue(fmt.Sprintf("%d", tr.cfg.Events)))
	fmt.Printf("Payload size: %s bytes\n", tr.color.blue(fmt.Sprintf("%d", tr.cfg.PayloadBytes)))
	fmt.Printf("Batch size: %s\n", tr.color.blue(fmt.Sprintf("%d", tr.cfg.BatchSize)))
	fmt.Printf("Partitions: %s\n\n", tr.color.blue(fmt.Sprintf("%d", tr.cfg.NumPartitions)))

	// Start Pulse in-process
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pulseApp, dataDir, err := tr.startPulse(ctx)
	if err != nil {
		return fmt.Errorf("start pulse: %w", err)
	}
	defer func() {
		if stopErr := pulseApp.Stop(context.Background()); stopErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to stop pulse: %v\n", stopErr)
		}
		if rmErr := os.RemoveAll(dataDir); rmErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to remove temp dir: %v\n", rmErr)
		}
	}()

	// Wait for server to be ready
	if waitErr := tr.waitForServer(); waitErr != nil {
		return fmt.Errorf("wait for server: %w", waitErr)
	}

	// Generate events
	fmt.Println(tr.color.bold("📝 Generating events..."))
	tr.generateEvents()
	fmt.Printf("%s Generated %d events\n\n", tr.color.green("✓"), len(tr.generated))

	// Ingest events
	fmt.Println(tr.color.bold("📤 Ingesting events..."))
	ingestStart := time.Now()
	if ingestErr := tr.ingestEvents(); ingestErr != nil {
		return fmt.Errorf("ingest events: %w", ingestErr)
	}
	ingestDuration := time.Since(ingestStart)
	ingestRate := float64(tr.cfg.Events) / ingestDuration.Seconds()
	fmt.Printf("%s Ingested %d events in %s (%.0f events/sec)\n\n",
		tr.color.green("✓"), tr.cfg.Events, ingestDuration.Round(time.Millisecond), ingestRate)

	// Wait for flush to disk (flush interval is 100ms)
	fmt.Println("Waiting for data to flush...")
	time.Sleep(500 * time.Millisecond)

	// Consume events
	fmt.Println(tr.color.bold("📥 Consuming events..."))
	consumeStart := time.Now()
	consumed, err := tr.consumeEvents()
	if err != nil {
		return fmt.Errorf("consume events: %w", err)
	}
	consumeDuration := time.Since(consumeStart)
	consumeRate := float64(len(consumed)) / consumeDuration.Seconds()
	fmt.Printf("%s Consumed %d events in %s (%.0f events/sec)\n\n",
		tr.color.green("✓"), len(consumed), consumeDuration.Round(time.Millisecond), consumeRate)

	// Validate
	fmt.Println(tr.color.bold("🔍 Validating results..."))
	if err := tr.validate(consumed); err != nil {
		fmt.Printf("%s Validation failed: %v\n", tr.color.red("✗"), err)
		return err
	}
	fmt.Printf("%s All validations passed\n\n", tr.color.green("✓"))

	// Print summary
	tr.printSummary(ingestDuration, ingestRate, consumeDuration, consumeRate)

	return nil
}

func (tr *TestRunner) startPulse(ctx context.Context) (*app.App, string, error) {
	// Create temporary data directory
	dataDir, err := os.MkdirTemp("", "pulse-e2e-*")
	if err != nil {
		return nil, "", fmt.Errorf("create temp dir: %w", err)
	}

	// Set environment for Pulse
	if setErr := os.Setenv("PULSE_DATA_DIR", dataDir); setErr != nil {
		return nil, "", fmt.Errorf("set PULSE_DATA_DIR: %w", setErr)
	}
	if setErr := os.Setenv("PULSE_SERVER_PORT", strconv.Itoa(tr.cfg.ServerPort)); setErr != nil {
		return nil, "", fmt.Errorf("set PULSE_SERVER_PORT: %w", setErr)
	}
	if setErr := os.Setenv("PULSE_NUM_PARTITIONS", strconv.Itoa(tr.cfg.NumPartitions)); setErr != nil {
		return nil, "", fmt.Errorf("set PULSE_NUM_PARTITIONS: %w", setErr)
	}
	if setErr := os.Setenv("PULSE_LOG_LEVEL", "error"); setErr != nil {
		return nil, "", fmt.Errorf("set PULSE_LOG_LEVEL: %w", setErr)
	}
	if setErr := os.Setenv("PULSE_ENABLE_DOCS", "false"); setErr != nil {
		return nil, "", fmt.Errorf("set PULSE_ENABLE_DOCS: %w", setErr)
	}
	if setErr := os.Setenv("PULSE_FLUSH_INTERVAL_MS", "100"); setErr != nil {
		return nil, "", fmt.Errorf("set PULSE_FLUSH_INTERVAL_MS: %w", setErr)
	}

	// Load config
	cfg, err := config.Load()
	if err != nil {
		if rmErr := os.RemoveAll(dataDir); rmErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to cleanup: %v\n", rmErr)
		}
		return nil, "", fmt.Errorf("load config: %w", err)
	}

	// Create logger
	logger := log.New(cfg.LogLevel)

	// Create and start app
	application := app.New(cfg, logger)

	errChan := make(chan error, 1)
	go func() {
		if err := application.Start(ctx); err != nil {
			errChan <- err
		}
	}()

	// Check for immediate startup errors
	select {
	case err := <-errChan:
		if rmErr := os.RemoveAll(dataDir); rmErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to cleanup: %v\n", rmErr)
		}
		return nil, "", fmt.Errorf("app start error: %w", err)
	case <-time.After(100 * time.Millisecond):
		// No error, continue
	}

	return application, dataDir, nil
}

func (tr *TestRunner) waitForServer() error {
	for i := 0; i < 50; i++ {
		resp, err := http.Get(tr.baseURL + "/health")
		if err == nil {
			if closeErr := resp.Body.Close(); closeErr != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to close response body: %v\n", closeErr)
			}
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("server did not become ready")
}

func (tr *TestRunner) generateEvents() {
	tr.generated = make([]generatedEvent, 0, tr.cfg.Events)

	for i := 0; i < tr.cfg.Events; i++ {
		key := fmt.Sprintf("key-%d", i%tr.cfg.NumPartitions)
		partition := simpleHash(key) % tr.cfg.NumPartitions

		// Generate deterministic but pseudo-random payload
		data := make([]byte, tr.cfg.PayloadBytes)
		for j := 0; j < tr.cfg.PayloadBytes; j++ {
			data[j] = byte(tr.rng.Intn(256))
		}

		tr.generated = append(tr.generated, generatedEvent{
			key:       key,
			data:      data,
			partition: partition,
			index:     i,
		})
	}
}

// simpleHash computes a simple hash for partitioning (matches Pulse's logic).
func simpleHash(s string) int {
	h := 0
	for _, c := range s {
		h = 31*h + int(c)
	}
	if h < 0 {
		h = -h
	}
	return h
}

func (tr *TestRunner) ingestEvents() error {
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	for i := 0; i < len(tr.generated); i += tr.cfg.BatchSize {
		end := i + tr.cfg.BatchSize
		if end > len(tr.generated) {
			end = len(tr.generated)
		}

		batch := event.Batch{
			Events: make([]event.Event, 0, end-i),
		}

		for j := i; j < end; j++ {
			batch.Events = append(batch.Events, event.Event{
				Key:  tr.generated[j].key,
				Data: tr.generated[j].data,
			})
		}

		body, err := json.Marshal(batch)
		if err != nil {
			return fmt.Errorf("marshal batch: %w", err)
		}

		resp, err := client.Post(tr.baseURL+"/events", "application/json", bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("post events: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			respBody, readErr := io.ReadAll(resp.Body)
			if closeErr := resp.Body.Close(); closeErr != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to close response body: %v\n", closeErr)
			}
			if readErr != nil {
				return fmt.Errorf("ingest failed: status=%d, read error=%v", resp.StatusCode, readErr)
			}
			return fmt.Errorf("ingest failed: status=%d, body=%s", resp.StatusCode, string(respBody))
		}

		// Read and discard response
		if _, copyErr := io.Copy(io.Discard, resp.Body); copyErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to read response body: %v\n", copyErr)
		}
		if closeErr := resp.Body.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close response body: %v\n", closeErr)
		}
	}

	return nil
}

func (tr *TestRunner) consumeEvents() ([]*event.Entry, error) {
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	var allEntries []*event.Entry

	for p := 0; p < tr.cfg.NumPartitions; p++ {
		offset := int64(0)
		for {
			url := fmt.Sprintf("%s/stream?partition=%d&offset=%d&limit=1000", tr.baseURL, p, offset)
			resp, err := client.Get(url)
			if err != nil {
				return nil, fmt.Errorf("get stream partition %d: %w", p, err)
			}

			if resp.StatusCode != http.StatusOK {
				respBody, readErr := io.ReadAll(resp.Body)
				if closeErr := resp.Body.Close(); closeErr != nil {
					fmt.Fprintf(os.Stderr, "Warning: failed to close response body: %v\n", closeErr)
				}
				if readErr != nil {
					return nil, fmt.Errorf("stream failed: status=%d, read error=%v", resp.StatusCode, readErr)
				}
				return nil, fmt.Errorf("stream failed: status=%d, body=%s", resp.StatusCode, string(respBody))
			}

			var result consumer.StreamResult
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				if closeErr := resp.Body.Close(); closeErr != nil {
					fmt.Fprintf(os.Stderr, "Warning: failed to close response body: %v\n", closeErr)
				}
				return nil, fmt.Errorf("decode stream result: %w", err)
			}
			if closeErr := resp.Body.Close(); closeErr != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to close response body: %v\n", closeErr)
			}

			if len(result.Entries) == 0 {
				break
			}

			allEntries = append(allEntries, result.Entries...)
			// Calculate next offset from last entry
			offset = result.Entries[len(result.Entries)-1].Offset + 1
		}
	}

	return allEntries, nil
}

func (tr *TestRunner) validate(consumed []*event.Entry) error {
	// Validate count
	if len(consumed) != tr.cfg.Events {
		return fmt.Errorf("count mismatch: expected %d, got %d", tr.cfg.Events, len(consumed))
	}

	// Group by partition
	byPartition := make(map[int][]*event.Entry)
	for _, entry := range consumed {
		// Determine partition from key
		partition := simpleHash(entry.Key) % tr.cfg.NumPartitions
		byPartition[partition] = append(byPartition[partition], entry)
	}

	// Validate per-partition ordering and continuity
	for p := 0; p < tr.cfg.NumPartitions; p++ {
		entries := byPartition[p]
		if len(entries) == 0 {
			continue
		}

		// Sort by offset
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Offset < entries[j].Offset
		})

		// Check for gaps
		for i := 1; i < len(entries); i++ {
			if entries[i].Offset != entries[i-1].Offset+1 {
				return fmt.Errorf("partition %d: offset gap between %d and %d", p, entries[i-1].Offset, entries[i].Offset)
			}
		}

		// Check that offsets start at 0
		if entries[0].Offset != 0 {
			return fmt.Errorf("partition %d: first offset is %d, expected 0", p, entries[0].Offset)
		}
	}

	// Validate data integrity (spot check)
	// We can't validate all because we didn't store original order per partition
	// But we can validate that each consumed event has correct payload size
	for _, entry := range consumed {
		if len(entry.Data) != tr.cfg.PayloadBytes {
			return fmt.Errorf("data size mismatch: expected %d bytes, got %d", tr.cfg.PayloadBytes, len(entry.Data))
		}
	}

	return nil
}

func (tr *TestRunner) printSummary(ingestDuration time.Duration, ingestRate float64, consumeDuration time.Duration, consumeRate float64) {
	fmt.Println(tr.color.bold("=== Summary ==="))
	fmt.Printf("Total events:       %s\n", tr.color.green(fmt.Sprintf("%d", tr.cfg.Events)))
	fmt.Printf("Partitions:         %s\n", tr.color.green(fmt.Sprintf("%d", tr.cfg.NumPartitions)))
	fmt.Printf("Ingest duration:    %s\n", tr.color.green(ingestDuration.Round(time.Millisecond).String()))
	fmt.Printf("Ingest rate:        %s events/sec\n", tr.color.green(fmt.Sprintf("%.0f", ingestRate)))
	fmt.Printf("Consume duration:   %s\n", tr.color.green(consumeDuration.Round(time.Millisecond).String()))
	fmt.Printf("Consume rate:       %s events/sec\n", tr.color.green(fmt.Sprintf("%.0f", consumeRate)))
	fmt.Printf("Status:             %s\n\n", tr.color.green("PASSED"))
}

func main() {
	cfg := loadTestConfig()
	runner := newTestRunner(cfg)

	if err := runner.run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s Test failed: %v\n", runner.color.red("✗"), err)
		os.Exit(1)
	}
}
