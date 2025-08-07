package main

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
)

const (
	testConfig = `listen: 0.0.0.0:8080
listen_pprof: 0.0.0.0:7008

target: http://127.0.0.1:9091/receive
log_level: debug
timeout: 50ms
timeout_shutdown: 100ms

max_conns_per_host: 64

tenant:
  label_remove: false
  default: default
  label_list:
    - "__tenant__"
    - "__foo__"
    - "__bar__"
`
	testConfigWithValues = `listen: 0.0.0.0:8080
listen_pprof: 0.0.0.0:7008

target: http://127.0.0.1:9091/receive
log_level: debug
timeout: 50ms
timeout_shutdown: 100ms

tenant:
  prefix: foobar-
  label_remove: false
  default: default
  label_list:
    - "__foo__"
    - "__bar__"
`
)

var (
	smpl1 = prompb.Sample{
		Value:     123,
		Timestamp: 456,
	}

	smpl2 = prompb.Sample{
		Value:     789,
		Timestamp: 101112,
	}

	testTS1 = prompb.TimeSeries{
		Labels: []prompb.Label{
			{
				Name:  "__tenant__",
				Value: "foobar",
			},
		},

		Samples: []prompb.Sample{
			smpl1,
		},
	}

	testTS2 = prompb.TimeSeries{
		Labels: []prompb.Label{
			{
				Name:  "__tenant__",
				Value: "foobaz",
			},
		},

		Samples: []prompb.Sample{
			smpl2,
		},
	}

	testTS3 = prompb.TimeSeries{
		Labels: []prompb.Label{
			{
				Name:  "__tenantXXX",
				Value: "foobaz",
			},
		},
	}

	testTS4 = prompb.TimeSeries{
		Labels: []prompb.Label{
			{
				Name:  "__tenant__",
				Value: "foobaz",
			},
		},

		Samples: []prompb.Sample{
			smpl2,
		},
	}

	testWRQ = &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			testTS1,
			testTS2,
		},
	}

	testWRQ1 = &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			testTS1,
		},
	}

	testWRQ2 = &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			testTS2,
		},
	}

	testWRQ3 = &prompb.WriteRequest{}
	testWRQ4 = &prompb.WriteRequest{
		Metadata: []prompb.MetricMetadata{
			{
				MetricFamilyName: "foobar",
			},
		},
	}
)

func getConfig(contents string) (*config, error) {
	err := os.WriteFile("config_test.yml", []byte(contents), 0o666)
	if err != nil {
		return nil, err
	}

	cfg, err := configLoad("config_test.yml")
	if err != nil {
		return nil, err
	}

	if err = os.Remove("config_test.yml"); err != nil {
		return nil, err
	}

	return cfg, nil
}

func createProcessor() (*processor, error) {
	cfg, err := getConfig(testConfig)
	if err != nil {
		return nil, err
	}

	return newProcessor(*cfg), nil
}

// Mock HTTP handler functions for testing
func sinkHandlerError(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "Some error", http.StatusInternalServerError)
}

func sinkHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	reqBuf, err := snappy.Decode(nil, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ok"))
}

func Test_config(t *testing.T) {
	// Create a sample config file for testing
	sampleConfig := `
listen: 0.0.0.0:8080
concurrency: 10
tenant:
  prefix: ""
`
	err := os.WriteFile("config.yml", []byte(sampleConfig), 0o666)
	if err != nil {
		t.Skip("Unable to create test config file")
	}
	defer os.Remove("config.yml")

	cfg, err := configLoad("config.yml")
	assert.Nil(t, err)
	assert.Equal(t, 10, cfg.Concurrency)
}

// Check if Prefix empty by default
func Test_config_is_prefix_empty_by_default(t *testing.T) {
	// Create a sample config file for testing
	sampleConfig := `
listen: 0.0.0.0:8080
tenant:
  prefix: ""
`
	err := os.WriteFile("config.yml", []byte(sampleConfig), 0o666)
	if err != nil {
		t.Skip("Unable to create test config file")
	}
	defer os.Remove("config.yml")

	cfg, err := configLoad("config.yml")
	assert.Nil(t, err)
	assert.Equal(t, "", cfg.Tenant.Prefix)
}

// Check if Prefix empty by default
func Test_config_is_prefix_empty_if_not_set(t *testing.T) {
	cfg, err := getConfig(testConfig)
	assert.Nil(t, err)
	assert.Equal(t, "", cfg.Tenant.Prefix)
}

// Check if Prefix filled with value
func Test_config_is_prefix_filled(t *testing.T) {
	cfg, err := getConfig(testConfigWithValues)
	assert.Nil(t, err)
	assert.Equal(t, "foobar-", cfg.Tenant.Prefix)
}

func Test_request_headers(t *testing.T) {
	cfg, err := getConfig(testConfig)
	assert.Nil(t, err)

	p := newProcessor(*cfg)

	// Create a test HTTP request
	req, _ := http.NewRequest("POST", "http://test.com/api/v1/write", nil)
	clientIP := "1.1.1.1"
	reqID, _ := uuid.NewRandom()

	// Call fillRequestHeaders
	p.fillRequestHeaders(clientIP, reqID, "my-tenant", req)

	// Verify headers are set correctly
	assert.Equal(t, "snappy", req.Header.Get("Content-Encoding"))
	assert.Equal(t, "application/x-protobuf", req.Header.Get("Content-Type"))
	assert.Equal(t, "0.1.0", req.Header.Get("X-Prometheus-Remote-Write-Version"))
	assert.Equal(t, clientIP, req.Header.Get("X-Prometheus-RW-Proxy-Client"))
	assert.Equal(t, reqID.String(), req.Header.Get("X-Prometheus-RW-Proxy-ReqID"))
	assert.Equal(t, "my-tenant", req.Header.Get("X-Scope-OrgID"))
}

func Test_handle(t *testing.T) {
	cfg, err := getConfig(testConfig)
	assert.Nil(t, err)
	cfg.Tenant.LabelRemove = true

	// Create a test server for the backend
	var backendCalls int
	var mu sync.Mutex
	backendHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		backendCalls++
		if backendCalls == 1 {
			// First call returns error
			sinkHandlerError(w, r)
		} else {
			// Subsequent calls return success
			sinkHandler(w, r)
		}
	})

	backend := httptest.NewServer(backendHandler)
	defer backend.Close()

	cfg.Target = backend.URL

	// Create processor with test client
	p := newProcessor(*cfg)

	// Create test server for the processor
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/-/ready":
			p.handleReady(w, r)
		case cfg.ListenPath:
			p.handleWrite(w, r)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer testServer.Close()

	// Marshal test data
	wrq1, err := p.marshal(testWRQ)
	assert.Nil(t, err)

	wrq3, err := p.marshal(testWRQ3)
	assert.Nil(t, err)

	wrq4, err := p.marshal(testWRQ4)
	assert.Nil(t, err)

	client := &http.Client{Timeout: 5 * time.Second}

	// Test 1: Connection failed (error response from backend)
	req1, _ := http.NewRequest("POST", testServer.URL+cfg.ListenPath, bytes.NewReader(wrq1))
	req1.Header.Set("Content-Type", "application/x-protobuf")
	req1.Header.Set("Content-Encoding", "snappy")

	resp1, err := client.Do(req1)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusInternalServerError, resp1.StatusCode)
	resp1.Body.Close()

	// Test 2: Success with valid data
	req2, _ := http.NewRequest("POST", testServer.URL+cfg.ListenPath, bytes.NewReader(wrq1))
	req2.Header.Set("Content-Type", "application/x-protobuf")
	req2.Header.Set("Content-Encoding", "snappy")

	resp2, err := client.Do(req2)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	body, _ := io.ReadAll(resp2.Body)
	assert.Equal(t, "Ok", string(body))
	resp2.Body.Close()

	// Test 3: Success with metadata
	cfg.Metadata = true
	req3, _ := http.NewRequest("POST", testServer.URL+cfg.ListenPath, bytes.NewReader(wrq4))
	req3.Header.Set("Content-Type", "application/x-protobuf")
	req3.Header.Set("Content-Encoding", "snappy")

	resp3, err := client.Do(req3)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	resp3.Body.Close()

	// Test 4: Error - empty timeseries
	req4, _ := http.NewRequest("POST", testServer.URL+cfg.ListenPath, bytes.NewReader(wrq3))
	req4.Header.Set("Content-Type", "application/x-protobuf")
	req4.Header.Set("Content-Encoding", "snappy")

	resp4, err := client.Do(req4)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, resp4.StatusCode)
	resp4.Body.Close()

	// Test 5: Error - invalid data
	req5, _ := http.NewRequest("POST", testServer.URL+cfg.ListenPath, bytes.NewReader([]byte("foobar")))
	req5.Header.Set("Content-Type", "application/x-protobuf")
	req5.Header.Set("Content-Encoding", "snappy")

	resp5, err := client.Do(req5)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, resp5.StatusCode)
	resp5.Body.Close()

	// Test 6: Error - invalid protobuf
	req6, _ := http.NewRequest("POST", testServer.URL+cfg.ListenPath, bytes.NewReader(snappy.Encode(nil, []byte("foobar"))))
	req6.Header.Set("Content-Type", "application/x-protobuf")
	req6.Header.Set("Content-Encoding", "snappy")

	resp6, err := client.Do(req6)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, resp6.StatusCode)
	resp6.Body.Close()

	// Test 7: Ready endpoint during shutdown
	go p.close()
	time.Sleep(30 * time.Millisecond)

	req7, _ := http.NewRequest("GET", testServer.URL+"/-/ready", nil)
	resp7, err := client.Do(req7)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, resp7.StatusCode)
	resp7.Body.Close()
}

func Test_processTimeseries(t *testing.T) {
	cfg, err := getConfig(testConfig)
	assert.Nil(t, err)
	cfg.Tenant.LabelRemove = true

	p := newProcessor(*cfg)
	assert.Nil(t, err)

	ten, err := p.processTimeseries(&testTS4)
	assert.Nil(t, err)
	assert.Equal(t, "foobaz", ten)

	ten, err = p.processTimeseries(&testTS3)
	assert.Nil(t, err)
	assert.Equal(t, "default", ten)

	cfg.Tenant.Default = ""
	p = newProcessor(*cfg)
	assert.Nil(t, err)

	_, err = p.processTimeseries(&testTS3)
	assert.NotNil(t, err)
}

func Test_marshal(t *testing.T) {
	p, err := createProcessor()
	assert.Nil(t, err)

	_, err = p.unmarshal([]byte{0xFF})
	assert.NotNil(t, err)

	_, err = p.unmarshal(snappy.Encode(nil, []byte{0xFF}))
	assert.NotNil(t, err)

	buf, err := p.marshal(testWRQ)
	assert.Nil(t, err)

	wrq, err := p.unmarshal(buf)
	assert.Nil(t, err)

	assert.Equal(t, testTS1, wrq.Timeseries[0])
	assert.Equal(t, testTS2, wrq.Timeseries[1])
}

func Test_createWriteRequests(t *testing.T) {
	p, err := createProcessor()
	assert.Nil(t, err)

	m, err := p.createWriteRequests(testWRQ)
	assert.Nil(t, err)

	mExp := map[string]*prompb.WriteRequest{
		"foobar": testWRQ1,
		"foobaz": testWRQ2,
	}

	assert.Equal(t, mExp, m)
}

func Test_getClientIP(t *testing.T) {
	p, err := createProcessor()
	assert.Nil(t, err)

	// Test X-Forwarded-For
	req1, _ := http.NewRequest("GET", "http://test.com", nil)
	req1.Header.Set("X-Forwarded-For", "192.168.1.1, 10.0.0.1")
	req1.RemoteAddr = "127.0.0.1:1234"
	assert.Equal(t, "192.168.1.1", p.getClientIP(req1))

	// Test X-Real-IP
	req2, _ := http.NewRequest("GET", "http://test.com", nil)
	req2.Header.Set("X-Real-IP", "192.168.1.2")
	req2.RemoteAddr = "127.0.0.1:1234"
	assert.Equal(t, "192.168.1.2", p.getClientIP(req2))

	// Test RemoteAddr fallback
	req3, _ := http.NewRequest("GET", "http://test.com", nil)
	req3.RemoteAddr = "127.0.0.1:1234"
	assert.Equal(t, "127.0.0.1", p.getClientIP(req3))
}

func Test_shutdown(t *testing.T) {
	cfg, err := getConfig(testConfig)
	assert.Nil(t, err)

	// Create a test listener
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.Nil(t, err)
	cfg.testListener = listener

	p := newProcessor(*cfg)

	// Start the processor
	err = p.run()
	assert.Nil(t, err)

	// Give server time to start
	time.Sleep(50 * time.Millisecond)

	// Shutdown the processor
	err = p.close()
	assert.Nil(t, err)

	// Verify shutting down flag is set
	assert.Equal(t, uint32(1), p.shuttingDown)
}

func Benchmark_marshal(b *testing.B) {
	p, _ := createProcessor()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, _ := p.marshal(testWRQ)
		_, _ = p.unmarshal(buf)
	}
}

func TestRemoveOrdered(t *testing.T) {
	l := []prompb.Label{
		{
			Name:  "aaa",
			Value: "bbb",
		},
	}

	l = removeOrdered(l, 0)
	require.Equal(t, []prompb.Label{}, l)

	l = []prompb.Label{
		{
			Name:  "aaa",
			Value: "bbb",
		},
		{
			Name:  "ccc",
			Value: "ddd",
		},
	}
	l = removeOrdered(l, 0)
	require.Equal(t, []prompb.Label{
		{
			Name:  "ccc",
			Value: "ddd",
		},
	}, l)
}

// generateTestCertificate generates a self-signed certificate for testing
func generateTestCertificate() (certFile string, keyFile string, err error) {
	// Generate RSA private key
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"Test Org"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{""},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames:              []string{"localhost"},
	}

	// Generate certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return "", "", err
	}

	// Create temporary files for cert and key
	certTempFile, err := os.CreateTemp("", "test-cert-*.pem")
	if err != nil {
		return "", "", err
	}
	certFile = certTempFile.Name()

	keyTempFile, err := os.CreateTemp("", "test-key-*.pem")
	if err != nil {
		os.Remove(certFile)
		return "", "", err
	}
	keyFile = keyTempFile.Name()

	// Write certificate to file
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})
	if _, err := certTempFile.Write(certPEM); err != nil {
		os.Remove(certFile)
		os.Remove(keyFile)
		return "", "", err
	}
	certTempFile.Close()

	// Write private key to file
	privKeyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		os.Remove(certFile)
		os.Remove(keyFile)
		return "", "", err
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: privKeyDER,
	})
	if _, err := keyTempFile.Write(keyPEM); err != nil {
		os.Remove(certFile)
		os.Remove(keyFile)
		return "", "", err
	}
	keyTempFile.Close()

	return certFile, keyFile, nil
}

func Test_HTTP2_Support(t *testing.T) {
	cfg, err := getConfig(testConfig)
	assert.Nil(t, err)

	// Enable HTTP/2
	cfg.EnableHTTP2 = true

	// Create a test backend server with HTTP/2 support
	backend := httptest.NewUnstartedServer(http.HandlerFunc(sinkHandler))
	backend.EnableHTTP2 = true
	backend.StartTLS()
	defer backend.Close()

	// Update config with test backend URL before creating processor
	cfg.Target = backend.URL

	// Create a custom transport with HTTP/2 support for testing
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // For testing with self-signed certs
		},
		ForceAttemptHTTP2: true,
	}

	// Configure HTTP/2
	err = http2.ConfigureTransport(transport)
	assert.Nil(t, err)

	testClient := &http.Client{
		Transport: transport,
		Timeout:   5 * time.Second,
	}

	// Set test client in config before creating processor
	cfg.testClient = testClient

	// Create processor with HTTP/2 enabled and test client
	p := newProcessor(*cfg)

	// Verify that the server has HTTP/2 configured
	assert.NotNil(t, p.srv.TLSConfig)
	assert.Contains(t, p.srv.TLSConfig.NextProtos, "h2")
	assert.Contains(t, p.srv.TLSConfig.NextProtos, "http/1.1")

	// Verify the target is set correctly
	assert.Equal(t, backend.URL, p.cfg.Target)

	// Send a request through the processor
	r := p.send("127.0.0.1", uuid.Must(uuid.NewRandom()), "test-tenant", testWRQ)

	// Verify the request was successful
	assert.Nil(t, r.err)
	assert.Equal(t, http.StatusOK, r.code)
	assert.Equal(t, []byte("Ok"), r.body)
}

func Test_TLS_Setup(t *testing.T) {
	// Generate a self-signed certificate for testing
	certFile, keyFile, err := generateTestCertificate()
	assert.Nil(t, err)
	defer os.Remove(certFile)
	defer os.Remove(keyFile)

	// Create config with TLS settings
	cfg, err := getConfig(testConfig)
	assert.Nil(t, err)
	cfg.TLSCertFile = certFile
	cfg.TLSKeyFile = keyFile
	cfg.Listen = "127.0.0.1:0" // Use random port

	// Create processor with TLS configuration
	p := newProcessor(*cfg)

	// Create a listener
	listener, err := net.Listen("tcp", cfg.Listen)
	assert.Nil(t, err)

	// Get the actual address the server is listening on
	serverAddr := listener.Addr().String()

	// Start the server with TLS in a goroutine
	serverStarted := make(chan bool)
	serverErrors := make(chan error, 1)
	go func() {
		// Load the certificate
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			serverErrors <- err
			return
		}

		// Create TLS listener
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"h2", "http/1.1"},
		}
		tlsListener := tls.NewListener(listener, tlsConfig)

		// Update server with the TLS listener address
		p.srv.Addr = serverAddr

		// Signal that server is starting
		close(serverStarted)

		// Start serving
		if err := p.srv.Serve(tlsListener); err != nil && err != http.ErrServerClosed {
			serverErrors <- err
		}
	}()

	// Wait for server to start or error
	select {
	case <-serverStarted:
		// Server started successfully
	case err := <-serverErrors:
		t.Fatalf("Server failed to start: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Server failed to start within timeout")
	}

	// Give server a moment to be fully ready
	time.Sleep(100 * time.Millisecond)

	defer p.close()

	// Test 1: Verify that we can connect with HTTPS
	httpsClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // Accept self-signed cert for testing
			},
		},
		Timeout: 5 * time.Second,
	}

	httpsReq, err := http.NewRequest("GET", "https://"+serverAddr+"/-/ready", nil)
	assert.Nil(t, err)

	httpsResp, err := httpsClient.Do(httpsReq)
	assert.Nil(t, err, "HTTPS request should succeed")
	assert.Equal(t, http.StatusOK, httpsResp.StatusCode)
	httpsResp.Body.Close()
}

func Test_TLS_With_Auth(t *testing.T) {
	cfg, err := getConfig(testConfig)
	assert.Nil(t, err)

	// Configure egress authentication
	cfg.Auth.Egress.Username = "testuser"
	cfg.Auth.Egress.Password = "testpass"

	// Create processor with auth configuration
	p := newProcessor(*cfg)

	// Verify auth header is set correctly
	expectedAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("testuser:testpass"))
	assert.Equal(t, expectedAuth, p.auth.egressHeader)

	// Create a test backend that verifies authentication
	authVerified := false
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == expectedAuth {
			authVerified = true
			sinkHandler(w, r)
		} else {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
	}))
	defer backend.Close()

	cfg.Target = backend.URL
	p.cfg.Target = backend.URL

	// Send a request through the processor
	reqID, _ := uuid.NewRandom()
	r := p.send("127.0.0.1", reqID, "test-tenant", testWRQ)

	// Verify the request was successful and auth was verified
	assert.Nil(t, r.err)
	assert.Equal(t, http.StatusOK, r.code)
	assert.True(t, authVerified, "Authentication header should have been verified")
}

const integrationTestConfig = `listen: 0.0.0.0:28080
target: http://127.0.0.1:9090/api/v1/write
log_level: debug
timeout: 30s
listen_metrics_address: 127.0.0.1:28081

tenant:
  label: "__tenant__"
  label_list:
    - "__tenant__"
  prefix: "initial-"
  default: "initial_default"
  label_remove: false
  allow_list:
    - "service1"
    - "service2"
  label_value_matcher:
    - name: "prod"
      regex: "^prod-.*"
    - name: "dev"
      regex: "^dev-.*"
`

const updatedIntegrationTestConfig = `listen: 0.0.0.0:28080
target: http://127.0.0.1:9090/api/v1/write
log_level: debug
timeout: 30s
listen_metrics_address: 127.0.0.1:28081

tenant:
  label: "__tenant__"
  label_list:
    - "__tenant__"
    - "__org__"
  prefix: "updated-"
  default: "updated_default"
  label_remove: true
  allow_list:
    - "service1"
    - "service2"
    - "service3"
  label_value_matcher:
    - name: "production"
      regex: "^production-.*"
    - name: "staging"
      regex: "^staging-.*"
    - name: "development"
      regex: "^dev-.*"
`

func Test_IntegrationSighupReload(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "integration_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, "config.yml")
	logPath := filepath.Join(tmpDir, "proxy.log")

	// Write initial config
	err = os.WriteFile(configPath, []byte(integrationTestConfig), 0644)
	require.NoError(t, err)

	// Build the proxy if it doesn't exist
	if _, err := os.Stat("prometheus-rw-proxy"); os.IsNotExist(err) {
		t.Log("Building prometheus-rw-proxy...")
		cmd := exec.Command("go", "build", "-o", "prometheus-rw-proxy", ".")
		err := cmd.Run()
		require.NoError(t, err, "Failed to build prometheus-rw-proxy")
	}

	// Start the proxy
	t.Log("Starting prometheus-rw-proxy...")
	cmd := exec.Command("./prometheus-rw-proxy", "-config", configPath)

	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	defer logFile.Close()

	cmd.Stdout = logFile
	cmd.Stderr = logFile

	err = cmd.Start()
	require.NoError(t, err)

	defer func() {
		if cmd.Process != nil {
			cmd.Process.Signal(syscall.SIGTERM)
			cmd.Wait()
		}
	}()

	// Wait for startup
	t.Log("Waiting for proxy to start...")
	time.Sleep(3 * time.Second)

	// Verify process is running
	assert.NoError(t, cmd.Process.Signal(syscall.Signal(0)), "Proxy should be running")

	// Read initial logs
	initialLogs, err := os.ReadFile(logPath)
	require.NoError(t, err)

	t.Log("Initial startup logs:")
	t.Log(string(initialLogs))

	// Update configuration file
	t.Log("Updating configuration...")
	err = os.WriteFile(configPath, []byte(updatedIntegrationTestConfig), 0644)
	require.NoError(t, err)

	// Send SIGHUP
	t.Log("Sending SIGHUP signal...")
	err = cmd.Process.Signal(syscall.SIGHUP)
	require.NoError(t, err)

	// Wait for reload
	time.Sleep(2 * time.Second)

	// Read updated logs
	updatedLogs, err := os.ReadFile(logPath)
	require.NoError(t, err)

	logsStr := string(updatedLogs)
	t.Log("Logs after SIGHUP:")
	t.Log(logsStr)

	// Verify reload happened
	assert.Contains(t, logsStr, "Received SIGHUP", "Should receive SIGHUP signal")
	assert.Contains(t, logsStr, "Reloading tenant configuration", "Should start reloading")
	assert.Contains(t, logsStr, "Tenant configuration reloaded successfully", "Should complete reload successfully")

	// Verify specific changes were detected
	assert.Contains(t, logsStr, "prefix: initial- -> updated-", "Should detect prefix change")
	assert.Contains(t, logsStr, "default: initial_default -> updated_default", "Should detect default change")
	assert.Contains(t, logsStr, "label_remove: false -> true", "Should detect label_remove change")

	// Verify label matcher changes
	assert.Contains(t, logsStr, "=== Label Value Matcher Changes ===", "Should show detailed matcher changes")
	assert.Contains(t, logsStr, "REMOVED matcher: prod", "Should show removed matcher")
	assert.Contains(t, logsStr, "ADDED matcher: production", "Should show added matcher")
	assert.Contains(t, logsStr, "Label matcher summary: 2 old -> 3 new matchers", "Should show matcher summary")

	// Verify process is still running after reload
	assert.NoError(t, cmd.Process.Signal(syscall.Signal(0)), "Proxy should still be running after reload")

	t.Log("Integration test completed successfully")
}

func Test_IntegrationSighupInvalidConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "integration_invalid_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, "config.yml")
	logPath := filepath.Join(tmpDir, "proxy.log")

	// Write initial valid config
	err = os.WriteFile(configPath, []byte(integrationTestConfig), 0644)
	require.NoError(t, err)

	// Start the proxy
	t.Log("Starting prometheus-rw-proxy...")
	cmd := exec.Command("./prometheus-rw-proxy", "-config", configPath)

	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	defer logFile.Close()

	cmd.Stdout = logFile
	cmd.Stderr = logFile

	err = cmd.Start()
	require.NoError(t, err)

	defer func() {
		if cmd.Process != nil {
			cmd.Process.Signal(syscall.SIGTERM)
			cmd.Wait()
		}
	}()

	// Wait for startup
	time.Sleep(3 * time.Second)

	// Verify process is running
	assert.NoError(t, cmd.Process.Signal(syscall.Signal(0)), "Proxy should be running")

	// Write invalid configuration (missing tenant labels, no regex issues)
	invalidConfig := `listen: 0.0.0.0:28080
target: http://127.0.0.1:9090/api/v1/write
log_level: debug
timeout: 30s
listen_metrics_address: 127.0.0.1:28081

tenant:
  label: ""
  label_list: bad_type
  default: "should_not_apply"
  prefix: "invalid-"
  label_remove: false
`

	t.Log("Writing invalid configuration...")
	err = os.WriteFile(configPath, []byte(invalidConfig), 0644)
	require.NoError(t, err)

	// Send SIGHUP with invalid config
	t.Log("Sending SIGHUP with invalid config...")
	err = cmd.Process.Signal(syscall.SIGHUP)
	require.NoError(t, err)

	// Wait for reload attempt
	time.Sleep(2 * time.Second)

	// Read logs
	logs, err := os.ReadFile(logPath)
	require.NoError(t, err)

	logsStr := string(logs)
	t.Log("Logs after invalid config SIGHUP:")
	t.Log(logsStr)

	// Verify error handling
	assert.Contains(t, logsStr, "Received SIGHUP", "Should receive SIGHUP signal")
	assert.Contains(t, logsStr, "Failed to reload tenant config", "Should fail to reload invalid config")

	// The error can be either validation error or regex compilation error
	hasValidationError := strings.Contains(logsStr, "tenant label or label_list must be specified")
	hasRegexError := strings.Contains(logsStr, "invalid regex for 'invalid'")
	hasConfigLoadError := strings.Contains(logsStr, "failed to load config")

	assert.True(t, hasValidationError || hasRegexError || hasConfigLoadError,
		"Should show some kind of validation/config error")

	if hasRegexError {
		t.Log("Got regex compilation error (expected)")
		assert.Contains(t, logsStr, "error parsing regexp", "Should show regex parsing error")
	}

	if hasValidationError {
		t.Log("Got tenant validation error (also valid)")
	}

	if hasConfigLoadError {
		t.Log("Got config loading error (also valid)")
	}

	// Verify process is still running after failed reload
	assert.NoError(t, cmd.Process.Signal(syscall.Signal(0)), "Proxy should still be running after failed reload")

	t.Log("Invalid config test completed successfully")
}

func Test_IntegrationSighupRegexError(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "integration_regex_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, "config.yml")
	logPath := filepath.Join(tmpDir, "proxy.log")

	// Write initial valid config
	err = os.WriteFile(configPath, []byte(integrationTestConfig), 0644)
	require.NoError(t, err)

	// Start the proxy
	t.Log("Starting prometheus-rw-proxy...")
	cmd := exec.Command("./prometheus-rw-proxy", "-config", configPath)

	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	defer logFile.Close()

	cmd.Stdout = logFile
	cmd.Stderr = logFile

	err = cmd.Start()
	require.NoError(t, err)

	defer func() {
		if cmd.Process != nil {
			cmd.Process.Signal(syscall.SIGTERM)
			cmd.Wait()
		}
	}()

	// Wait for startup
	time.Sleep(3 * time.Second)

	// Write config with invalid regex
	invalidRegexConfig := `listen: 0.0.0.0:28080
target: http://127.0.0.1:9090/api/v1/write
log_level: debug
timeout: 30s
listen_metrics_address: 127.0.0.1:28081

tenant:
  label: "__tenant__"
  label_list:
    - "__tenant__"
  default: "test"
  label_value_matcher:
    - name: "invalid"
      regex: "[invalid-regex"
`

	t.Log("Writing config with invalid regex...")
	err = os.WriteFile(configPath, []byte(invalidRegexConfig), 0644)
	require.NoError(t, err)

	// Send SIGHUP with invalid regex config
	t.Log("Sending SIGHUP with invalid regex config...")
	err = cmd.Process.Signal(syscall.SIGHUP)
	require.NoError(t, err)

	// Wait for reload attempt
	time.Sleep(2 * time.Second)

	// Read logs
	logs, err := os.ReadFile(logPath)
	require.NoError(t, err)

	logsStr := string(logs)
	t.Log("Logs after invalid regex config SIGHUP:")
	t.Log(logsStr)

	// Verify regex error handling
	assert.Contains(t, logsStr, "Received SIGHUP", "Should receive SIGHUP signal")
	assert.Contains(t, logsStr, "Failed to reload tenant config", "Should fail to reload invalid config")
	assert.Contains(t, logsStr, "invalid regex for 'invalid'", "Should show regex validation error")
	assert.Contains(t, logsStr, "error parsing regexp", "Should show regex parsing error details")

	// Verify process is still running after failed reload
	assert.NoError(t, cmd.Process.Signal(syscall.Signal(0)), "Proxy should still be running after failed reload")

	t.Log("Invalid regex config test completed successfully")
}
