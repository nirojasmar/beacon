package probes

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"beacon/internal/config"
	"beacon/internal/model"
)

const maxResponseBody = 1 << 20

func Run(ctx context.Context, check *model.Check, cfg *config.Config) model.Result {
	start := time.Now()
	result := model.Result{Timestamp: start, Type: check.Type}
	timeout := time.Duration(check.TimeoutSeconds) * time.Second
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var err error
	switch check.Type {
	case "http":
		result.HTTPStatus, err = probeHTTP(probeCtx, check, cfg)
	case "tcp":
		err = probeTCP(probeCtx, check.Target, cfg.BlockPrivateNetworks)
	case "dns":
		err = probeDNS(probeCtx, check.Target, check.RecordType, cfg.DNSServer)
	default:
		err = fmt.Errorf("unsupported probe type %q", check.Type)
	}
	result.LatencyMs = int(time.Since(start).Milliseconds())
	result.OK = err == nil
	if err != nil {
		result.Error = err.Error()
	}
	return result
}

func probeHTTP(ctx context.Context, check *model.Check, cfg *config.Config) (int, error) {
	target, err := url.ParseRequestURI(check.Target)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Hostname() == "" {
		return 0, fmt.Errorf("invalid HTTP target")
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify},
		DialContext: guardedDialContext(cfg.BlockPrivateNetworks),
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if len(via) > 3 {
				return fmt.Errorf("stopped after 3 redirects")
			}
			return nil
		},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, check.Target, nil)
	if err != nil {
		return 0, fmt.Errorf("create HTTP request: %w", err)
	}
	request.Header.Set("User-Agent", "beacon/"+cfg.AppVersion)
	response, err := client.Do(request)
	if err != nil {
		return 0, fmt.Errorf("HTTP request: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBody+1))
	if err != nil {
		return response.StatusCode, fmt.Errorf("read HTTP response: %w", err)
	}
	if len(body) > maxResponseBody {
		body = body[:maxResponseBody]
	}

	minStatus, maxStatus := check.Expect.StatusMin, check.Expect.StatusMax
	if minStatus == 0 {
		minStatus = http.StatusOK
	}
	if maxStatus == 0 {
		maxStatus = 399
	}
	if response.StatusCode < minStatus || response.StatusCode > maxStatus {
		return response.StatusCode, fmt.Errorf("unexpected HTTP status %d (expected %d..%d)", response.StatusCode, minStatus, maxStatus)
	}
	if check.Expect.BodyContains != "" && !strings.Contains(string(body), check.Expect.BodyContains) {
		return response.StatusCode, fmt.Errorf("response body does not contain expected text")
	}
	return response.StatusCode, nil
}

func probeTCP(ctx context.Context, target string, blockPrivate bool) error {
	if _, _, err := net.SplitHostPort(target); err != nil {
		return fmt.Errorf("invalid TCP target: expected host:port")
	}
	conn, err := guardedDialContext(blockPrivate)(ctx, "tcp", target)
	if err != nil {
		return fmt.Errorf("TCP dial: %w", err)
	}
	return conn.Close()
}

func probeDNS(ctx context.Context, hostname, recordType, server string) error {
	resolver := net.DefaultResolver
	if server != "" {
		host, port, err := net.SplitHostPort(server)
		if err != nil {
			return fmt.Errorf("invalid DNS_SERVER: %w", err)
		}
		if _, err := strconv.Atoi(port); err != nil {
			return fmt.Errorf("invalid DNS_SERVER port: %w", err)
		}
		resolver = &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(host, port))
			},
		}
	}
	switch strings.ToUpper(recordType) {
	case "", "A", "AAAA":
		network := "ip4"
		if strings.EqualFold(recordType, "AAAA") {
			network = "ip6"
		}
		addresses, err := resolver.LookupIP(ctx, network, hostname)
		if err != nil {
			return fmt.Errorf("DNS lookup: %w", err)
		}
		if len(addresses) == 0 {
			return fmt.Errorf("DNS lookup returned no addresses")
		}
	case "CNAME":
		if _, err := resolver.LookupCNAME(ctx, hostname); err != nil {
			return fmt.Errorf("DNS lookup: %w", err)
		}
	default:
		return fmt.Errorf("unsupported DNS record type %q", recordType)
	}
	return nil
}

func guardedDialContext(blockPrivate bool) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("resolve target: %w", err)
		}
		if len(ips) == 0 {
			return nil, fmt.Errorf("target resolved to no IP addresses")
		}
		for _, entry := range ips {
			if forbiddenIP(entry.IP, blockPrivate) {
				return nil, fmt.Errorf("target resolves to a disallowed IP address")
			}
		}
		dialer := &net.Dialer{}
		var lastErr error
		for _, entry := range ips {
			conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(entry.IP.String(), port))
			if dialErr == nil {
				return conn, nil
			}
			lastErr = dialErr
		}
		return nil, lastErr
	}
}

func forbiddenIP(ip net.IP, blockPrivate bool) bool {
	if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	return blockPrivate && (ip.IsPrivate() || ip.IsLoopback() || ip.IsUnspecified())
}
