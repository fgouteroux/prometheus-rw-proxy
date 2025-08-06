package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/google/uuid"
	me "github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/prompb"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/http2"
)

const metricsNamespace = "prometheus_rw_proxy"

var (
	metricTimeseriesBatchesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "timeseries_batches_received_total",
		Help:      "The total number of batches received.",
	})
	metricTimeseriesBatchesReceivedBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Name:      "timeseries_batches_received_bytes",
		Help:      "Size in bytes of timeseries batches received.",
		Buckets:   []float64{0.5, 1, 10, 25, 100, 250, 500, 1000, 5000, 10000, 30000, 300000, 600000, 1800000, 3600000},
	})
	metricTimeseriesSkipped = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "timeseries_skipped_total",
		Help:      "The total number of timeseries skipped.",
	})
	metricTimeseriesReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "timeseries_received_total",
		Help:      "The total number of timeseries received.",
	}, []string{"tenant"})
	metricTimeseriesRequestDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Name:      "timeseries_request_duration_milliseconds",
		Help:      "HTTP write request duration for tenant-specific timeseries in milliseconds, filtered by response code.",
		Buckets:   []float64{0.5, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 1800000, 3600000},
	},
		[]string{"code", "tenant"},
	)
	metricTimeseriesRequestErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "timeseries_request_errors_total",
		Help:      "The total number of tenant-specific timeseries writes that yielded errors.",
	}, []string{"tenant"})
	metricTimeseriesRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "timeseries_requests_total",
		Help:      "The total number of tenant-specific timeseries writes.",
	}, []string{"tenant"})
)

type result struct {
	code     int
	body     []byte
	duration float64
	tenant   string
	err      error
}

type processor struct {
	cfg config

	srv *http.Server
	cli *http.Client

	shuttingDown uint32

	auth struct {
		egressHeader string
	}
}

func newProcessor(c config) *processor {
	p := &processor{
		cfg: c,
	}

	transport := &http.Transport{
		MaxIdleConns:        c.MaxConnsPerHost,
		MaxIdleConnsPerHost: c.MaxConnsPerHost,
		IdleConnTimeout:     c.MaxConnDuration,
		TLSHandshakeTimeout: 10 * time.Second,
		DisableCompression:  true,
		ForceAttemptHTTP2:   true,
	}

	// For testing - if testClient is provided, use it
	if c.testClient != nil {
		p.cli = c.testClient
	} else {
		if err := http2.ConfigureTransport(transport); err != nil {
			log.Warnf("Failed to configure HTTP/2 transport: %v", err)
		}

		p.cli = &http.Client{
			Transport: transport,
			Timeout:   c.Timeout,
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /-/ready", p.handleReady)
	mux.HandleFunc("POST "+c.ListenPath, p.handleWrite)

	p.srv = &http.Server{
		Addr:         c.Listen,
		Handler:      mux,
		ReadTimeout:  c.Timeout,
		WriteTimeout: c.Timeout,
		IdleTimeout:  c.IdleTimeout,
		TLSConfig: &tls.Config{
			NextProtos: []string{"h2", "http/1.1"},
			MinVersion: tls.VersionTLS12,
		},
	}

	// Configure HTTP/2 for server only if enabled
	if c.EnableHTTP2 {
		if err := http2.ConfigureServer(p.srv, &http2.Server{
			MaxConcurrentStreams: safeUint32(c.Concurrency),
		}); err != nil {
			log.Warnf("Failed to configure HTTP/2 server: %v", err)
		}
	}

	if c.Auth.Egress.Username != "" {
		authString := fmt.Sprintf("%s:%s", c.Auth.Egress.Username, c.Auth.Egress.Password)
		p.auth.egressHeader = "Basic " + base64.StdEncoding.EncodeToString([]byte(authString))
	}

	return p
}

func (p *processor) run() error {
	var listener net.Listener
	var err error

	// For testing
	if p.cfg.testListener != nil {
		listener = p.cfg.testListener
	} else {
		if listener, err = net.Listen("tcp", p.cfg.Listen); err != nil {
			return err
		}
	}

	go func() {
		var err error
		if p.cfg.TLSCertFile != "" && p.cfg.TLSKeyFile != "" {
			err = p.srv.ServeTLS(listener, p.cfg.TLSCertFile, p.cfg.TLSKeyFile)
		} else {
			err = p.srv.Serve(listener)
		}
		if err != nil && err != http.ErrServerClosed {
			log.Errorf("Server error: %v", err)
		}
	}()

	return nil
}

func (p *processor) handleReady(w http.ResponseWriter, _ *http.Request) {
	if atomic.LoadUint32(&p.shuttingDown) == 1 {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (p *processor) getClientIP(r *http.Request) string {
	// Check X-Forwarded-For first
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take first IP from comma-separated list
		if idx := strings.Index(xff, ","); idx != -1 {
			return strings.TrimSpace(xff[:idx])
		}
		return xff
	}

	// Check X-Real-IP
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}

	// Fall back to RemoteAddr
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	return host
}

func (p *processor) handleWrite(w http.ResponseWriter, r *http.Request) {
	// Check request size limit if configured
	if p.cfg.MaxRequestBodySize > 0 && r.ContentLength > int64(p.cfg.MaxRequestBodySize) {
		http.Error(w, "Request too large", http.StatusRequestEntityTooLarge)
		return
	}

	var body []byte
	var err error

	if p.cfg.MaxRequestBodySize > 0 {
		body, err = io.ReadAll(io.LimitReader(r.Body, int64(p.cfg.MaxRequestBodySize)))
	} else {
		body, err = io.ReadAll(r.Body)
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	metricTimeseriesBatchesReceivedBytes.Observe(float64(len(body)))
	metricTimeseriesBatchesReceived.Inc()

	wrReqIn, err := p.unmarshal(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	tenantPrefix := p.cfg.Tenant.Prefix

	if p.cfg.Tenant.PrefixPreferSource {
		sourceTenantPrefix := r.Header.Get(p.cfg.Tenant.Header)
		if sourceTenantPrefix != "" {
			tenantPrefix = sourceTenantPrefix + "-"
		}
	}

	clientIP := p.getClientIP(r)
	reqID, _ := uuid.NewRandom()

	if len(wrReqIn.Timeseries) == 0 {
		// If there's metadata - just accept the request and drop it
		if len(wrReqIn.Metadata) > 0 {
			if p.cfg.Metadata && p.cfg.Tenant.Default != "" {
				result := p.send(clientIP, reqID, tenantPrefix+p.cfg.Tenant.Default, wrReqIn)
				if result.err != nil {
					http.Error(w, result.err.Error(), http.StatusInternalServerError)
					log.Errorf("proc: src=%s req_id=%s: unable to proxy metadata: %s", clientIP, reqID, result.err)
					return
				}
				w.WriteHeader(result.code)
				_, _ = w.Write(result.body)
			}
			return
		}

		http.Error(w, "No timeseries found in the request", http.StatusBadRequest)
		return
	}

	m, err := p.createWriteRequests(wrReqIn)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	metricTenant := ""
	var errs *me.Error
	results := p.dispatch(clientIP, reqID, tenantPrefix, m)

	code, body := http.StatusOK, []byte("Ok")

	// Return 204 regardless of errors if AcceptAll is enabled
	if p.cfg.Tenant.AcceptAll {
		code, body = http.StatusNoContent, nil
	} else {
		for _, result := range results {
			if p.cfg.MetricsIncludeTenant {
				metricTenant = result.tenant
			}

			metricTimeseriesRequests.WithLabelValues(metricTenant).Inc()

			if result.err != nil {
				metricTimeseriesRequestErrors.WithLabelValues(metricTenant).Inc()
				errs = me.Append(errs, result.err)
				log.Errorf("proc: src=%s %s", clientIP, result.err)
				continue
			}

			if result.code < 200 || result.code >= 300 {
				if p.cfg.LogResponseErrors {
					log.Errorf("proc: src=%s req_id=%s HTTP code %d (%s)", clientIP, reqID, result.code, string(result.body))
				}
			}

			if result.code > code {
				code, body = result.code, result.body
			}

			metricTimeseriesRequestDurationMilliseconds.WithLabelValues(strconv.Itoa(result.code), metricTenant).Observe(result.duration)
		}

		if errs.ErrorOrNil() != nil {
			http.Error(w, errs.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Pass back max status code from upstream response
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

func (p *processor) createWriteRequests(wrReqIn *prompb.WriteRequest) (map[string]*prompb.WriteRequest, error) {
	m := map[string]*prompb.WriteRequest{}
	skippedCount := 0

	for _, ts := range wrReqIn.Timeseries {
		tenant, err := p.processTimeseries(&ts)
		if err != nil {
			// Log the error but continue processing other timeseries
			log.Debugf("proc: Skipping timeseries: %s", err)
			metricTimeseriesSkipped.Inc()
			skippedCount++
			continue
		}

		if p.cfg.MetricsIncludeTenant {
			metricTimeseriesReceived.WithLabelValues(tenant).Inc()
		} else {
			metricTimeseriesReceived.WithLabelValues("").Inc()
		}

		wrReqOut, ok := m[tenant]
		if !ok {
			wrReqOut = &prompb.WriteRequest{}
			m[tenant] = wrReqOut
		}

		wrReqOut.Timeseries = append(wrReqOut.Timeseries, ts)
	}

	if skippedCount > 0 {
		log.Infof("proc: Skipped %d timeseries due to missing tenant labels", skippedCount)
	}

	// Return error only if ALL timeseries were invalid
	if len(m) == 0 && len(wrReqIn.Timeseries) > 0 {
		return nil, fmt.Errorf("no valid timeseries found - all missing tenant labels")
	}

	return m, nil
}

func (p *processor) unmarshal(b []byte) (*prompb.WriteRequest, error) {
	decoded, err := snappy.Decode(nil, b)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to unpack snappy")
	}

	req := &prompb.WriteRequest{}
	if err = proto.Unmarshal(decoded, req); err != nil {
		return nil, errors.Wrap(err, "Unable to unmarshal protobuf")
	}

	return req, nil
}

func (p *processor) marshal(wr *prompb.WriteRequest) ([]byte, error) {
	b := make([]byte, wr.Size())

	// Marshal to Protobuf
	if _, err := wr.MarshalTo(b); err != nil {
		return nil, err
	}

	// Compress with Snappy
	return snappy.Encode(nil, b), nil
}

func (p *processor) dispatch(clientIP string, reqID uuid.UUID, tenantPrefix string, m map[string]*prompb.WriteRequest) []result {
	var wg sync.WaitGroup
	results := make([]result, len(m))

	i := 0
	for tenant, wrReq := range m {
		wg.Add(1)

		go func(idx int, tenant string, wrReq *prompb.WriteRequest) {
			defer wg.Done()

			r := p.send(clientIP, reqID, tenant, wrReq)
			results[idx] = r
		}(i, tenantPrefix+tenant, wrReq)

		i++
	}

	wg.Wait()
	return results
}

func removeOrdered(slice []prompb.Label, s int) []prompb.Label {
	return append(slice[:s], slice[s+1:]...)
}

func (p *processor) findTenantSimple(labels []prompb.Label, configuredLabels []string) (tenant string, idx int) {
	for i, label := range labels {
		for _, configuredLabel := range configuredLabels {
			if label.Name == configuredLabel {
				// Try regex matching first if available
				if len(LabelValueRegex) > 0 {
					for _, matcher := range LabelValueRegex {
						if matcher.Regex.MatchString(label.Value) {
							return matcher.Name, i
						}
					}
				}
				// Default to label value
				return label.Value, i
			}
		}
	}
	return "", -1
}

func (p *processor) processTimeseries(ts *prompb.TimeSeries) (tenant string, err error) {
	tenant, idx := p.findTenantSimple(ts.Labels, p.cfg.Tenant.LabelList)

	if tenant == "" {
		if p.cfg.Tenant.Default == "" {
			return "", fmt.Errorf("label(s) %v not found in timeseries with labels: %v",
				p.cfg.Tenant.LabelList, ts)
		}

		return p.cfg.Tenant.Default, nil
	}

	if p.cfg.Tenant.LabelRemove && idx >= 0 {
		// Order is important. See:
		// https://github.com/thanos-io/thanos/issues/6452
		// https://github.com/prometheus/prometheus/issues/11505
		ts.Labels = removeOrdered(ts.Labels, idx)
	}

	return tenant, nil
}

func (p *processor) send(clientIP string, reqID uuid.UUID, tenant string, wr *prompb.WriteRequest) result {
	start := time.Now()
	r := result{tenant: tenant}

	buf, err := p.marshal(wr)
	if err != nil {
		r.err = err
		return r
	}

	req, err := http.NewRequest(http.MethodPost, p.cfg.Target, bytes.NewReader(buf))
	if err != nil {
		r.err = err
		return r
	}

	p.fillRequestHeaders(clientIP, reqID, tenant, req)

	if p.auth.egressHeader != "" {
		req.Header.Set("Authorization", p.auth.egressHeader)
	}

	resp, err := p.cli.Do(req)
	if err != nil {
		r.err = err
		return r
	}
	defer resp.Body.Close()

	r.code = resp.StatusCode
	r.body, err = io.ReadAll(resp.Body)
	if err != nil {
		r.err = err
		return r
	}

	r.duration = time.Since(start).Seconds() * 1000 // Convert to milliseconds

	return r
}

func (p *processor) fillRequestHeaders(clientIP string, reqID uuid.UUID, tenant string, req *http.Request) {
	req.Header.Set("User-Agent", "prometheus-rw-proxy/"+Version)
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	req.Header.Set("X-Prometheus-RW-Proxy-Client", clientIP)
	req.Header.Set("X-Prometheus-RW-Proxy-ReqID", reqID.String())
	req.Header.Set(p.cfg.Tenant.Header, tenant)
}

func (p *processor) close() error {
	// Signal that we're shutting down
	atomic.StoreUint32(&p.shuttingDown, 1)
	// Let healthcheck detect that we're offline
	time.Sleep(p.cfg.TimeoutShutdown)
	// Shutdown gracefully
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return p.srv.Shutdown(ctx)
}

func safeUint32(v int) uint32 {
	if v <= 0 {
		return 0
	}
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}
