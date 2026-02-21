package nntp

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/textproto"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/retry"
)

// ProviderPool manages connections for a single provider using a LIFO stack
type ProviderPool struct {
	conns  []*connectionEntry // Stack: Push/Pop from end
	mu     sync.Mutex         // Protects conns slice only
	slots  chan struct{}      // Semaphore: capacity = max connections
	max    int
	config config.UsenetProvider
}

// Client manages a pool of NNTP connections.
type Client struct {
	pools     map[string]*ProviderPool // Map Key: Provider Host
	providers []config.UsenetProvider
	logger    zerolog.Logger

	retries int // Number of retries per provider for transient errors

	closed atomic.Bool
	// Speed test results storage
	speedTestResults *xsync.Map[string, SpeedTestResult]
}

// SpeedTestResult holds the result of a provider speed test
type SpeedTestResult struct {
	Provider  string    `json:"provider"`
	SpeedMBps float64   `json:"speed_mbps"`
	LatencyMs int64     `json:"latency_ms"`
	BytesRead int64     `json:"bytes_read"`
	TestedAt  time.Time `json:"tested_at"`
	Error     string    `json:"error,omitempty"`
}

// connectionEntry tracks a connection and its provider
type connectionEntry struct {
	conn     *Connection
	provider config.UsenetProvider
	lastUsed time.Time
}

// TimeoutConfig holds all NNTP timeout settings in one place.
// This provides a single location to tune timeout behavior.
type TimeoutConfig struct {
	// Connection establishment timeout
	DialTimeout time.Duration
	// TCP keepalive interval
	KeepAlive time.Duration
	// Auth/handshake deadline after connection
	HandshakeTimeout time.Duration
	// Read deadline for streaming segment data
	StreamBodyTimeout time.Duration
	// Health check connections idle longer than this
	StaleThreshold time.Duration
	// Close connections idle longer than this
	IdleTimeout time.Duration
	// How often to check for idle connections
	ReaperInterval time.Duration
}

// DefaultTimeouts returns production-tuned timeout values.
// These are aggressive to prevent "connection reset by peer" errors
// from long-idle connections.
var DefaultTimeouts = TimeoutConfig{
	DialTimeout:       10 * time.Second,
	KeepAlive:         30 * time.Second,
	HandshakeTimeout:  10 * time.Second,
	StreamBodyTimeout: 60 * time.Second,
	StaleThreshold:    10 * time.Second,
	IdleTimeout:       20 * time.Second,
	ReaperInterval:    20 * time.Second,
}

// Package-level timeouts used by all clients
var timeouts = DefaultTimeouts

// NewClient creates a new connection manager
func NewClient(cfg *config.Config) (*Client, error) {
	providers := cfg.Usenet.Providers
	if len(providers) == 0 {
		return nil, errors.New("no NNTP providers configured")
	}

	// Sort providers by priority (lower number = higher priority)
	sort.Slice(providers, func(i, j int) bool {
		return providers[i].Priority < providers[j].Priority
	})
	pools := make(map[string]*ProviderPool)
	for _, p := range providers {
		pp := &ProviderPool{
			conns:  make([]*connectionEntry, 0, p.MaxConnections),
			slots:  make(chan struct{}, p.MaxConnections),
			max:    p.MaxConnections,
			config: p,
		}
		pools[p.Host] = pp
	}

	cm := &Client{
		pools:            pools,
		providers:        providers,
		retries:          cfg.Retries,
		logger:           logger.New("nntp-client"),
		speedTestResults: xsync.NewMap[string, SpeedTestResult](),
	}

	// Start background reaper
	go cm.reaper()
	return cm, nil
}

// put returns a connection to the pool and releases the slot.
func (c *Client) put(conn *Connection, provider config.UsenetProvider) {
	if conn == nil {
		return
	}

	pp, ok := c.pools[provider.Host]
	if !ok {
		_ = conn.Close()
		return
	}

	// Don't return closed connections to pool
	if conn.IsClosed() {
		_ = conn.Close()
		<-pp.slots // Release slot
		return
	}

	if c.closed.Load() {
		_ = conn.Close()
		<-pp.slots // Release slot
		return
	}

	entry := &connectionEntry{
		conn:     conn,
		provider: provider,
		lastUsed: time.Now(),
	}

	pp.mu.Lock()
	// Cap stack size (shouldn't happen with semaphore, but be safe)
	if len(pp.conns) >= pp.max {
		pp.mu.Unlock()
		_ = conn.Close()
		<-pp.slots // Release slot
		return
	}
	pp.conns = append(pp.conns, entry) // Push to stack
	pp.mu.Unlock()

	<-pp.slots // Release slot - connection is now available for reuse
}

// release closes a connection without returning it (for error cases)
func (c *Client) release(conn *Connection) {
	if conn != nil {
		_ = conn.Close()
		if pp, ok := c.pools[conn.address]; ok {
			<-pp.slots // Release slot
		}
	}
}

// isHealthy checks if a connection entry is still usable
func (c *Client) isHealthy(entry *connectionEntry) bool {
	if entry == nil || entry.conn == nil {
		return false
	}
	// Check if explicitly closed
	if entry.conn.IsClosed() {
		return false
	}
	// Check if already closed/expired (though normally caught by reaper)
	// Or check stale threshold
	if time.Since(entry.lastUsed) > timeouts.StaleThreshold {
		if err := entry.conn.ping(); err != nil {
			return false
		}
	}
	return true
}

// ExecuteWithFailover executes an operation with automatic provider failover and retry logic.
// Uses exclusion-based connection acquisition: gets ANY available connection,
// and on retryable errors, retries with exponential backoff before excluding the provider.
// Uses avast/retry-go for retry handling.
func (c *Client) ExecuteWithFailover(ctx context.Context, fn func(conn *Connection) error) error {
	var lastErr error
	excludedProviders := make(map[string]bool)

	for providerAttempts := 0; providerAttempts < len(c.providers); providerAttempts++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		conn, connProvider, err := c.getAnyAvailableConnection(ctx, excludedProviders)
		if err != nil {
			lastErr = err
			continue
		}

		// Use retry-go for retry logic with exponential backoff
		var currentConn = conn
		err = retry.Do(
			func() error {
				execErr := c.safeExecute(currentConn, fn)
				if execErr == nil {
					return nil
				}

				var nntpErr *Error
				if errors.As(execErr, &nntpErr) {
					switch nntpErr.Type {
					case ErrorTypeConnection, ErrorTypeTimeout, ErrorTypeServerBusy:
						// Retriable error - release the potentially dead connection
						c.release(currentConn)
						// Get a fresh connection for retry
						newConn, _, connErr := c.getAnyAvailableConnection(ctx, map[string]bool{})
						if connErr != nil {
							return retry.Unrecoverable(connErr)
						}
						// Prefer same provider for retry if possible
						if newConn.address != connProvider.Host {
							c.put(newConn, connProvider)
							newConn, _, connErr = c.getConnectionFromProvider(ctx, connProvider)
							if connErr != nil {
								return retry.Unrecoverable(connErr)
							}
						}
						currentConn = newConn
						return execErr // Retriable

					case ErrorTypeArticleNotFound:
						// Article not found - not retriable, try next provider
						return retry.Unrecoverable(execErr)

					default:
						// Non-retriable error
						return retry.Unrecoverable(execErr)
					}
				} else if customerror.IsPanicError(execErr) {
					// Panic error - release connection
					c.release(currentConn)
					return retry.Unrecoverable(execErr)
				}
				// Unknown error type - don't retry
				return retry.Unrecoverable(execErr)
			},
			retry.Context(ctx),
			retry.Attempts(uint(c.retries)+1),
			retry.Delay(config.DefaultRetryDelay),
			retry.MaxDelay(config.DefaultRetryDelayMax),
			retry.DelayType(retry.BackOffDelay),
			retry.LastErrorOnly(true),
		)

		// Success
		if err == nil {
			c.returnOrReleaseConn(currentConn, connProvider)
			return nil
		}

		// Handle failure
		c.returnOrReleaseConn(currentConn, connProvider)
		lastErr = err

		// Check if we should exclude this provider
		var nntpErr *Error
		if errors.As(err, &nntpErr) {
			if nntpErr.Type == ErrorTypeArticleNotFound ||
				nntpErr.Type == ErrorTypeConnection ||
				nntpErr.Type == ErrorTypeTimeout ||
				nntpErr.Type == ErrorTypeServerBusy {
				excludedProviders[connProvider.Host] = true
			} else {
				// Non-retriable error, return immediately
				return err
			}
		} else if customerror.IsPanicError(err) {
			excludedProviders[connProvider.Host] = true
		} else {
			// Unknown error type - return immediately
			return err
		}
	}

	if lastErr != nil {
		return lastErr
	}
	return errors.New("all providers failed")
}

// returnOrReleaseConn returns a connection to the pool or releases it if closed
func (c *Client) returnOrReleaseConn(conn *Connection, provider config.UsenetProvider) {
	if conn == nil {
		return
	}
	if conn.IsClosed() {
		c.release(conn)
	} else {
		c.put(conn, provider)
	}
}

// getConnectionFromProvider tries to get a connection from a specific provider
func (c *Client) getConnectionFromProvider(ctx context.Context, provider config.UsenetProvider) (*Connection, config.UsenetProvider, error) {
	pp, ok := c.pools[provider.Host]
	if !ok {
		return nil, provider, fmt.Errorf("provider pool not found: %s", provider.Host)
	}

	select {
	case pp.slots <- struct{}{}:
		conn, err := c.getOrCreateFromPool(ctx, pp, provider)
		if err != nil {
			<-pp.slots
			return nil, provider, err
		}
		return conn, provider, nil
	case <-ctx.Done():
		return nil, provider, ctx.Err()
	}
}

// safeExecute wraps fn execution with panic recovery
func (c *Client) safeExecute(conn *Connection, fn func(conn *Connection) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error().
				Interface("panic", r).
				Str("host", conn.address).
				Msg("Recovered from panic in NNTP operation")
			err = customerror.NewPanicError(r)
		}
	}()
	return fn(conn)
}

// getAnyAvailableConnection gets a connection from ANY provider that isn't excluded.
// Phase 1: Non-blocking scan of all eligible providers (fast path)
// Phase 2: If all busy, race goroutines to get first available slot
func (c *Client) getAnyAvailableConnection(ctx context.Context, excludedProviders map[string]bool) (*Connection, config.UsenetProvider, error) {
	// Build list of eligible providers
	var eligible []config.UsenetProvider
	for _, p := range c.providers {
		if !excludedProviders[p.Host] {
			eligible = append(eligible, p)
		}
	}

	if len(eligible) == 0 {
		return nil, config.UsenetProvider{}, errors.New("no eligible providers available")
	}

	// Phase 1: Non-blocking scan - try to get a free slot from any provider
	for _, provider := range eligible {
		pp := c.pools[provider.Host]

		select {
		case pp.slots <- struct{}{}:
			// Got a slot - try to get or create connection
			conn, err := c.getOrCreateFromPool(ctx, pp, provider)
			if err != nil {
				<-pp.slots // Release slot on error
				continue   // Try next provider
			}
			return conn, provider, nil
		default:
			// Pool at capacity, try next provider
			continue
		}
	}

	// Phase 2: All providers busy - race for first available
	return c.raceForConnection(ctx, eligible)
}

// raceForConnection spawns goroutines that race to acquire a connection slot.
// Returns as soon as any provider has availability.
func (c *Client) raceForConnection(ctx context.Context, eligible []config.UsenetProvider) (*Connection, config.UsenetProvider, error) {
	type result struct {
		conn     *Connection
		provider config.UsenetProvider
		err      error
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Buffer for all possible results to prevent goroutine leaks
	resultCh := make(chan result, len(eligible))

	for _, provider := range eligible {
		go func(p config.UsenetProvider) {
			pp := c.pools[p.Host]

			// Block waiting for slot (respects context)
			select {
			case pp.slots <- struct{}{}:
				// Got slot
			case <-ctx.Done():
				return // Context cancelled, exit cleanly
			}

			// Check if context was cancelled while we were waiting
			if ctx.Err() != nil {
				<-pp.slots // Release slot
				return
			}

			// Try to get or create connection
			conn, err := c.getOrCreateFromPool(ctx, pp, p)
			if err != nil {
				<-pp.slots // Release slot on error
				// Send error result
				select {
				case resultCh <- result{nil, p, err}:
				case <-ctx.Done():
				}
				return
			}

			// Try to send success result
			select {
			case resultCh <- result{conn, p, nil}:
				// Successfully sent
			case <-ctx.Done():
				// Context cancelled - another goroutine won, return our connection
				c.put(conn, p)
			}
		}(provider)
	}

	// Wait for first successful result or all failures
	var lastErr error
	for i := 0; i < len(eligible); i++ {
		select {
		case r := <-resultCh:
			if r.err == nil && r.conn != nil {
				return r.conn, r.provider, nil
			}
			lastErr = r.err
		case <-ctx.Done():
			return nil, config.UsenetProvider{}, ctx.Err()
		}
	}

	if lastErr != nil {
		return nil, config.UsenetProvider{}, lastErr
	}
	return nil, config.UsenetProvider{}, errors.New("failed to get connection from any provider")
}

// getOrCreateFromPool tries to get an existing connection from pool, or creates a new one.
// Caller must have already acquired a slot from pp.slots.
func (c *Client) getOrCreateFromPool(ctx context.Context, pp *ProviderPool, provider config.UsenetProvider) (*Connection, error) {
	// Try to get existing connection from pool (quick lock)
	for {
		pp.mu.Lock()
		if len(pp.conns) > 0 {
			// Pop from end (LIFO)
			n := len(pp.conns)
			entry := pp.conns[n-1]
			pp.conns[n-1] = nil // Avoid memory leak
			pp.conns = pp.conns[:n-1]
			pp.mu.Unlock()

			// Health check outside lock
			if c.isHealthy(entry) {
				return entry.conn, nil
			}
			// Unhealthy - close and try next pooled connection
			_ = entry.conn.Close()
			continue
		}
		pp.mu.Unlock()
		break
	}

	// No pooled connection available, create new one
	return c.createConnection(ctx, provider)
}

// createConnection creates a new NNTP connection to a provider
func (c *Client) createConnection(ctx context.Context, provider config.UsenetProvider) (*Connection, error) {
	address := fmt.Sprintf("%s:%d", provider.Host, provider.Port)

	var netConn net.Conn
	var err error

	dialer := &net.Dialer{
		Timeout:   timeouts.DialTimeout,
		KeepAlive: timeouts.KeepAlive,
	}

	// TLS if enabled
	if provider.SSL {
		// Dial with TLS directly if possible, or Dial then Wrap
		tlsConfig := &tls.Config{
			ServerName:         provider.Host,
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
		}
		// Use tls.Dialer for simpler timeout handling
		tlsDialer := &tls.Dialer{
			NetDialer: dialer,
			Config:    tlsConfig,
		}
		netConn, err = tlsDialer.DialContext(ctx, "tcp", address)
	} else {
		netConn, err = dialer.DialContext(ctx, "tcp", address)
	}

	if err != nil {
		return nil, NewConnectionError(fmt.Errorf("dial %s: %w", address, err))
	}

	// Optimize TCP socket
	if tcpConn, ok := netConn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
		_ = tcpConn.SetReadBuffer(1024 * 1024)
		_ = tcpConn.SetWriteBuffer(256 * 1024)
	}
	// For TLS, get underlying conn
	if tlsConn, ok := netConn.(*tls.Conn); ok {
		if tcpConn, ok := tlsConn.NetConn().(*net.TCPConn); ok {
			_ = tcpConn.SetNoDelay(true)
			_ = tcpConn.SetReadBuffer(1024 * 1024)
			_ = tcpConn.SetWriteBuffer(256 * 1024)
		}
	}

	reader := bufio.NewReaderSize(netConn, 512*1024)
	writer := bufio.NewWriterSize(netConn, 64*1024)

	conn := &Connection{
		conn:     netConn,
		reader:   reader,
		text:     textproto.NewReader(reader),
		writer:   writer,
		address:  provider.Host,
		port:     provider.Port,
		username: provider.Username,
		password: provider.Password,
		logger:   c.logger.With().Str("host", provider.Host).Logger(),
	}

	// Set deadline for handshake (greeting + auth)
	// If the server doesn't respond quickly during setup, we should abort.
	_ = netConn.SetDeadline(time.Now().Add(timeouts.HandshakeTimeout))

	// Read greeting
	line, err := reader.ReadString('\n')
	if err != nil {
		_ = netConn.Close()
		return nil, NewConnectionError(fmt.Errorf("read greeting: %w", err))
	}
	if !strings.HasPrefix(line, "200") && !strings.HasPrefix(line, "201") {
		_ = netConn.Close()
		return nil, NewConnectionError(fmt.Errorf("unexpected greeting: %s", line))
	}

	// Authenticate
	if provider.Username != "" {
		if err := conn.authenticate(); err != nil {
			_ = netConn.Close()
			return nil, fmt.Errorf("auth: %w", err)
		}
	}

	// Clear deadline for normal operation
	_ = netConn.SetDeadline(time.Time{})

	return conn, nil
}

// reaper periodically closes idle connections
func (c *Client) reaper() {
	ticker := time.NewTicker(timeouts.ReaperInterval)
	defer ticker.Stop()

	for range ticker.C {
		if c.closed.Load() {
			return
		}
		c.reapIdleConnections()
	}
}

func (c *Client) reapIdleConnections() {
	for _, pp := range c.pools {
		pp.mu.Lock()

		// LIFO Stack: Oldest (least recently used) items are at index 0.
		// Find first non-expired connection; all after it are newer and valid.
		expiredCount := 0
		for _, entry := range pp.conns {
			if time.Since(entry.lastUsed) > timeouts.IdleTimeout {
				_ = entry.conn.Close()
				expiredCount++
			} else {
				// Found a valid one - stop here
				break
			}
		}

		// Remove expired connections from the front of the slice
		if expiredCount > 0 {
			// Shift remaining items to front
			remaining := len(pp.conns) - expiredCount
			copy(pp.conns, pp.conns[expiredCount:])
			// Nil out trailing pointers to help GC
			for i := remaining; i < len(pp.conns); i++ {
				pp.conns[i] = nil
			}
			pp.conns = pp.conns[:remaining]
		}

		pp.mu.Unlock()
	}
}

// NNTPPoolStats holds aggregate pool statistics.
type NNTPPoolStats struct {
	MaxConnections int `json:"max_connections"`
	TotalCreated   int `json:"total_created"`
	Active         int `json:"active"`
	Idle           int `json:"idle"`
}

// NNTPSpeedTestStats holds speed test results for a provider.
type NNTPSpeedTestStats struct {
	LatencyMs int64   `json:"latency_ms"`
	SpeedMBps float64 `json:"speed_mbps"`
	BytesRead int64   `json:"bytes_read"`
	TestedAt  string  `json:"tested_at"`
	Error     string  `json:"error,omitempty"`
}

// NNTPProviderStats holds per-provider connection stats.
type NNTPProviderStats struct {
	Host           string              `json:"host"`
	Port           int                 `json:"port"`
	MaxConnections int                 `json:"max_connections"`
	Active         int                 `json:"active"`
	Idle           int                 `json:"idle"`
	SSL            bool                `json:"ssl"`
	SpeedTest      *NNTPSpeedTestStats `json:"speed_test,omitempty"`
}

// NNTPStats holds the complete NNTP client statistics.
type NNTPStats struct {
	Pool      NNTPPoolStats       `json:"pool"`
	Providers []NNTPProviderStats `json:"providers"`
}

// Stats returns current pool statistics as a typed struct.
func (c *Client) Stats() *NNTPStats {
	if c.closed.Load() {
		return nil
	}

	providers := make([]NNTPProviderStats, 0, len(c.providers))

	totalActive := 0
	totalIdle := 0
	totalMax := 0

	for _, p := range c.providers {
		pp, ok := c.pools[p.Host]
		if !ok {
			continue
		}

		pp.mu.Lock()
		idle := len(pp.conns)
		pp.mu.Unlock()

		active := len(pp.slots)
		maxC := pp.max

		totalActive += active
		totalIdle += idle
		totalMax += maxC

		ps := NNTPProviderStats{
			Host:           p.Host,
			Port:           p.Port,
			MaxConnections: maxC,
			Active:         active,
			Idle:           idle,
			SSL:            p.SSL,
		}

		if result, ok := c.speedTestResults.Load(p.Host); ok {
			ps.SpeedTest = &NNTPSpeedTestStats{
				LatencyMs: result.LatencyMs,
				SpeedMBps: result.SpeedMBps,
				BytesRead: result.BytesRead,
				TestedAt:  result.TestedAt.Format("2006-01-02T15:04:05Z07:00"),
				Error:     result.Error,
			}
		}

		providers = append(providers, ps)
	}

	return &NNTPStats{
		Pool: NNTPPoolStats{
			MaxConnections: totalMax,
			TotalCreated:   totalActive + totalIdle,
			Active:         totalActive,
			Idle:           totalIdle,
		},
		Providers: providers,
	}
}

func (c *Client) Stat(ctx context.Context, messageID string) (int, string, error) {
	if c.closed.Load() {
		return 0, "", errors.New("nntp client is closed")
	}

	var num int
	var id string
	err := c.ExecuteWithFailover(ctx, func(conn *Connection) error {
		n, echoed, statErr := conn.Stat(messageID)
		if statErr != nil {
			return statErr
		}
		num = n
		id = echoed
		return nil
	})
	return num, id, err
}

// chunkResult holds the results from a single chunk of PipelinedStat
type chunkResult struct {
	startIdx int          // Index into original messageIDs slice
	results  []StatResult // Per-segment results for this chunk
	connErr  error        // Connection-level error (if any)
}

// BatchStat checks availability of multiple message IDs efficiently using pipelining.
// Returns detailed per-segment results so caller can identify exactly which segments are missing.
// Does NOT fail-fast: all chunks are processed to give complete visibility.
func (c *Client) BatchStat(ctx context.Context, maxConnections int, messageIDs []string) (*BatchStatResult, error) {
	if c.closed.Load() {
		return nil, errors.New("nntp client is closed")
	}

	if len(messageIDs) == 0 {
		return &BatchStatResult{}, nil
	}

	// Determine optimal concurrency based on pool size and message count
	// Use fewer connections for small batches, more for large ones
	maxWorkers := min(maxConnections, len(messageIDs), 20)
	if maxWorkers < 1 {
		maxWorkers = 1
	}

	// Optimal pipeline batch size: balance between latency and memory
	const pipelineBatchSize = 50

	// Create work chunks with their start indices
	type chunk struct {
		startIdx   int
		messageIDs []string
	}
	var chunks []chunk
	for i := 0; i < len(messageIDs); i += pipelineBatchSize {
		end := min(i+pipelineBatchSize, len(messageIDs))
		chunks = append(chunks, chunk{startIdx: i, messageIDs: messageIDs[i:end]})
	}

	// Collect results from all workers
	resultsChan := make(chan chunkResult, len(chunks))
	var wg sync.WaitGroup

	// Semaphore to limit concurrent connections
	sem := make(chan struct{}, maxWorkers)

	for _, ch := range chunks {
		// Check context but don't cancel other workers
		if ctx.Err() != nil {
			// Mark remaining chunks as context cancelled
			resultsChan <- chunkResult{
				startIdx: ch.startIdx,
				connErr:  ctx.Err(),
			}
			continue
		}

		wg.Add(1)
		chunkCopy := ch // Capture for goroutine

		go func() {
			defer wg.Done()

			// Acquire semaphore slot
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				resultsChan <- chunkResult{
					startIdx: chunkCopy.startIdx,
					connErr:  ctx.Err(),
				}
				return
			}

			// Execute pipelined stat for this chunk
			var chunkResults []StatResult
			var connErr error

			err := c.ExecuteWithFailover(ctx, func(conn *Connection) error {
				var pipeErr error
				chunkResults, pipeErr = conn.PipelinedStat(chunkCopy.messageIDs)
				return pipeErr
			})

			if err != nil {
				connErr = err
			}

			resultsChan <- chunkResult{
				startIdx: chunkCopy.startIdx,
				results:  chunkResults,
				connErr:  connErr,
			}
		}()
	}

	// Wait for all workers and close results channel
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Aggregate results
	allResults := make([]StatResult, len(messageIDs))
	// Initialize with message IDs
	for i, msgID := range messageIDs {
		allResults[i].MessageID = msgID
	}

	var connectionErrors int
	for cr := range resultsChan {
		if cr.connErr != nil {
			connectionErrors++
			// Mark all messages in this chunk as errored
			endIdx := cr.startIdx + pipelineBatchSize
			if endIdx > len(messageIDs) {
				endIdx = len(messageIDs)
			}
			for i := cr.startIdx; i < endIdx; i++ {
				allResults[i].Available = false
				allResults[i].Error = cr.connErr
			}
			continue
		}

		// Copy per-segment results
		for i, res := range cr.results {
			idx := cr.startIdx + i
			if idx < len(allResults) {
				allResults[idx] = res
			}
		}
	}

	// Build final result
	result := &BatchStatResult{
		Results:    allResults,
		TotalCount: len(messageIDs),
	}

	for _, r := range allResults {
		if r.Available {
			result.FoundCount++
		} else if r.Error != nil {
			// Check if it's a real error vs article not found
			var nntpErr *Error
			if errors.As(r.Error, &nntpErr) && nntpErr.Type == ErrorTypeArticleNotFound {
				// Article not found is not an "error" for counting purposes
				continue
			}
			result.ErrorCount++
		}
	}

	return result, nil
}

// Close shuts down the connection manager
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}

	var totalClosed int
	for _, pp := range c.pools {
		pp.mu.Lock()
		// Close idle connections
		for _, entry := range pp.conns {
			_ = entry.conn.Close()
			totalClosed++
		}
		pp.conns = nil
		pp.mu.Unlock()
		// Note: Active connections will be released when their operations complete
		// and check c.closed in put(). No need to wake waiters since channel-based
		// semaphore uses select with context support.
	}

	c.logger.Info().
		Int("total_closed", totalClosed).
		Msg("Connection manager closed")

	return nil
}

// SpeedTest runs a speed test for a specific provider.
func (c *Client) SpeedTest(ctx context.Context, providerHost string, messageID string) SpeedTestResult {
	result := SpeedTestResult{
		Provider: providerHost,
		TestedAt: time.Now(),
	}

	// Find the provider by host
	var targetProvider *config.UsenetProvider
	for i := range c.providers {
		if c.providers[i].Host == providerHost {
			targetProvider = &c.providers[i]
			break
		}
	}

	if targetProvider == nil {
		result.Error = "provider not found"
		c.speedTestResults.Store(providerHost, result)
		return result
	}

	// Create connection
	conn, err := c.createConnection(ctx, *targetProvider)
	if err != nil {
		result.Error = fmt.Sprintf("connection failed: %v", err)
		c.speedTestResults.Store(providerHost, result)
		return result
	}
	defer func(conn *Connection) {
		_ = conn.Close()
	}(conn)

	// Measure latency using ping (true network RTT)
	pingStart := time.Now()
	if err := conn.ping(); err != nil {
		result.Error = fmt.Sprintf("ping failed: %v", err)
		c.speedTestResults.Store(providerHost, result)
		return result
	}
	result.LatencyMs = time.Since(pingStart).Milliseconds()

	// If no messageID provided, just return latency
	if messageID == "" {
		c.speedTestResults.Store(providerHost, result)
		return result
	}

	// Download the segment to measure actual speed
	downloadStart := time.Now()
	data, err := conn.GetBody(messageID)
	downloadDuration := time.Since(downloadStart)

	if err != nil {
		result.Error = fmt.Sprintf("download failed: %v", err)
		c.speedTestResults.Store(providerHost, result)
		return result
	}

	result.BytesRead = int64(len(data))

	// Calculate speed in MB/s
	if downloadDuration.Seconds() > 0 {
		result.SpeedMBps = float64(result.BytesRead) / downloadDuration.Seconds() / (1024 * 1024)
	}

	c.speedTestResults.Store(providerHost, result)
	return result
}

// GetSpeedTestResults returns all stored speed test results
func (c *Client) GetSpeedTestResults() map[string]SpeedTestResult {
	results := make(map[string]SpeedTestResult)
	c.speedTestResults.Range(func(host string, result SpeedTestResult) bool {
		results[host] = result
		return true
	})
	return results
}

// GetSpeedTestResult returns the speed test result for a specific provider
func (c *Client) GetSpeedTestResult(providerHost string) (SpeedTestResult, bool) {
	return c.speedTestResults.Load(providerHost)
}
