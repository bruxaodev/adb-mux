package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

type Upstream struct {
	Host string
	Port int
}

func (u Upstream) String() string { return fmt.Sprintf("%s:%d", u.Host, u.Port) }

type MuxState struct {
	Upstreams []Upstream
}

// collectDevices faz fan-out de host:devices(-l) e retorna payload concatenado
// + mapa serial -> upstream.
func (s *MuxState) collectDevices(ctx context.Context, long bool) ([]byte, map[string]Upstream) {
	cmd := "host:devices"
	if long {
		cmd = "host:devices-l"
	}
	type result struct {
		up    Upstream
		lines []string
		err   error
	}
	results := make([]result, len(s.Upstreams))
	var wg sync.WaitGroup
	for i, u := range s.Upstreams {
		wg.Add(1)
		go func(i int, u Upstream) {
			defer wg.Done()
			status, payload, err := upstreamSimple(ctx, u.Host, u.Port, cmd)
			if err != nil {
				results[i] = result{u, nil, err}
				return
			}
			if string(status) != "OKAY" {
				results[i] = result{u, nil, fmt.Errorf("upstream FAIL: %s", string(payload))}
				return
			}
			var lines []string
			for _, ln := range strings.Split(string(payload), "\n") {
				if strings.TrimSpace(ln) != "" {
					lines = append(lines, ln)
				}
			}
			results[i] = result{u, lines, nil}
		}(i, u)
	}
	wg.Wait()

	serialMap := make(map[string]Upstream)
	var merged []string
	for _, r := range results {
		if r.err != nil {
			logf("upstream %s erro: %v", r.up, r.err)
			continue
		}
		for _, ln := range r.lines {
			serial := parseSerialFromLine(ln)
			if serial == "" {
				continue
			}
			if _, dup := serialMap[serial]; dup {
				logf("WARN serial duplicado '%s' (descartando do upstream %s)", serial, r.up)
				continue
			}
			serialMap[serial] = r.up
			merged = append(merged, ln)
		}
	}
	body := ""
	if len(merged) > 0 {
		body = strings.Join(merged, "\n") + "\n"
	}
	return []byte(body), serialMap
}

func (s *MuxState) findUpstream(ctx context.Context, serial string) (Upstream, bool) {
	_, m := s.collectDevices(ctx, false)
	u, ok := m[serial]
	return u, ok
}

func parseSerialFromLine(line string) string {
	f := strings.Fields(line)
	if len(f) == 0 {
		return ""
	}
	return f[0]
}

// parseTransportTarget retorna o serial se o request for host:transport:<serial>
// ou host:tport:serial:<serial>. Retorna "" se for any/usb/local.
func parseTransportTarget(req string) string {
	if strings.HasPrefix(req, "host:transport:") {
		return strings.TrimPrefix(req, "host:transport:")
	}
	if strings.HasPrefix(req, "host:tport:serial:") {
		return strings.TrimPrefix(req, "host:tport:serial:")
	}
	return ""
}

// handleClient processa uma conexão de cliente ADB.
func (s *MuxState) handleClient(conn net.Conn) {
	defer conn.Close()
	peer := conn.RemoteAddr().String()
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	req, err := readRequest(conn)
	if err != nil {
		return
	}
	logf("[%s] -> %s", peer, req)
	// Após ler o request, removemos o deadline para suportar streams longos.
	conn.SetDeadline(time.Time{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	switch {
	case req == "host:version":
		s.handleVersion(conn)
	case req == "host:kill":
		conn.Write([]byte("OKAY"))
	case req == "host:devices" || req == "host:devices-l":
		payload, _ := s.collectDevices(ctx, strings.HasSuffix(req, "-l"))
		writeOKAYPayload(conn, payload)
	case req == "host:track-devices":
		s.handleTrackDevices(ctx, conn)
	case strings.HasPrefix(req, "host:transport:") || strings.HasPrefix(req, "host:tport:"):
		s.handleTransport(ctx, conn, req)
	case strings.HasPrefix(req, "host-serial:"):
		s.handleHostSerial(ctx, conn, req)
	default:
		// Fallback: primeiro upstream
		if len(s.Upstreams) == 0 {
			writeFAIL(conn, "nenhum upstream configurado")
			return
		}
		s.proxyAfterRequest(conn, s.Upstreams[0], req)
	}
}

func (s *MuxState) handleVersion(conn net.Conn) {
	for _, u := range s.Upstreams {
		status, payload, err := upstreamSimple(context.Background(), u.Host, u.Port, "host:version")
		if err == nil && string(status) == "OKAY" {
			writeOKAYPayload(conn, payload)
			return
		}
	}
	writeOKAYPayload(conn, []byte("0029"))
}

func (s *MuxState) handleTransport(ctx context.Context, conn net.Conn, req string) {
	serial := parseTransportTarget(req)
	var target Upstream
	if serial == "" {
		// any/usb/local — pega primeiro upstream com algum device
		_, smap := s.collectDevices(ctx, false)
		if len(smap) == 0 {
			writeFAIL(conn, "nenhum device disponivel")
			return
		}
		for _, u := range smap {
			target = u
			break
		}
	} else {
		u, ok := s.findUpstream(ctx, serial)
		if !ok {
			writeFAIL(conn, "device nao encontrado: "+serial)
			return
		}
		target = u
	}
	s.proxyAfterRequest(conn, target, req)
}

func (s *MuxState) handleHostSerial(ctx context.Context, conn net.Conn, req string) {
	parts := strings.SplitN(req, ":", 3)
	if len(parts) < 3 {
		writeFAIL(conn, "host-serial mal formado")
		return
	}
	serial := parts[1]
	target, ok := s.findUpstream(ctx, serial)
	if !ok {
		writeFAIL(conn, "device nao encontrado: "+serial)
		return
	}
	s.proxyAfterRequest(conn, target, req)
}

// proxyAfterRequest abre conexão no upstream, envia o request inicial,
// repassa o status, e faz pipe bidirecional do restante.
func (s *MuxState) proxyAfterRequest(client net.Conn, up Upstream, req string) {
	upstream, err := dialUpstream(context.Background(), up.Host, up.Port)
	if err != nil {
		writeFAIL(client, fmt.Sprintf("upstream %s indisponivel: %v", up, err))
		return
	}
	defer upstream.Close()

	if _, err := upstream.Write(encodeRequest(req)); err != nil {
		writeFAIL(client, fmt.Sprintf("erro enviando upstream: %v", err))
		return
	}

	status, err := readStatus(upstream)
	if err != nil {
		writeFAIL(client, fmt.Sprintf("erro lendo upstream: %v", err))
		return
	}
	if _, err := client.Write(status); err != nil {
		return
	}
	if string(status) != "OKAY" {
		payload, _ := readLengthPrefixed(upstream)
		fmt.Fprintf(client, "%04x", len(payload))
		client.Write(payload)
		return
	}

	// Stream bruto bidirecional
	done := make(chan error, 2)
	go func() { _, err := io.Copy(upstream, client); done <- err }()
	go func() { _, err := io.Copy(client, upstream); done <- err }()
	<-done
	// Sinaliza EOF aos dois lados
	if tc, ok := upstream.(*net.TCPConn); ok {
		tc.CloseWrite()
	}
	if tc, ok := client.(*net.TCPConn); ok {
		tc.CloseWrite()
	}
	<-done
}

// handleTrackDevices mantém uma conexão track-devices em cada upstream e
// re-emite a lista agregada toda vez que algo muda.
func (s *MuxState) handleTrackDevices(ctx context.Context, client net.Conn) {
	if _, err := client.Write([]byte("OKAY")); err != nil {
		return
	}

	type update struct {
		idx   int
		lines []string
	}
	updates := make(chan update, 32)
	last := make([][]string, len(s.Upstreams))
	var prevBody string
	firstSent := false

	var wg sync.WaitGroup
	for i, u := range s.Upstreams {
		wg.Add(1)
		go func(idx int, up Upstream) {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				conn, err := dialUpstream(ctx, up.Host, up.Port)
				if err != nil {
					logf("track %s reconectando: %v", up, err)
					select {
					case updates <- update{idx, nil}:
					case <-ctx.Done():
						return
					}
					select {
					case <-time.After(2 * time.Second):
					case <-ctx.Done():
						return
					}
					continue
				}
				if _, err := conn.Write(encodeRequest("host:track-devices")); err != nil {
					conn.Close()
					continue
				}
				status, err := readStatus(conn)
				if err != nil || string(status) != "OKAY" {
					conn.Close()
					continue
				}
				for {
					payload, err := readLengthPrefixed(conn)
					if err != nil {
						conn.Close()
						select {
						case updates <- update{idx, nil}:
						case <-ctx.Done():
							return
						}
						break
					}
					var lines []string
					for _, ln := range strings.Split(string(payload), "\n") {
						if strings.TrimSpace(ln) != "" {
							lines = append(lines, ln)
						}
					}
					select {
					case updates <- update{idx, lines}:
					case <-ctx.Done():
						conn.Close()
						return
					}
				}
				// Reconectar
				select {
				case <-time.After(2 * time.Second):
				case <-ctx.Done():
					return
				}
			}
		}(i, u)
	}

	// Watcher para detectar cliente fechando
	clientClosed := make(chan struct{})
	go func() {
		buf := make([]byte, 1)
		for {
			if _, err := client.Read(buf); err != nil {
				close(clientClosed)
				return
			}
		}
	}()

	for {
		select {
		case u := <-updates:
			last[u.idx] = u.lines
			seen := make(map[string]struct{})
			var merged []string
			for i := range s.Upstreams {
				for _, ln := range last[i] {
					serial := parseSerialFromLine(ln)
					if serial == "" {
						continue
					}
					if _, dup := seen[serial]; dup {
						continue
					}
					seen[serial] = struct{}{}
					merged = append(merged, ln)
				}
			}
			body := ""
			if len(merged) > 0 {
				body = strings.Join(merged, "\n") + "\n"
			}
			// Não envia o primeiro frame se ainda estiver vazio (esperando upstreams)
			if !firstSent && body == "" {
				continue
			}
			// Dedup: só envia se mudou
			if firstSent && body == prevBody {
				continue
			}
			prevBody = body
			firstSent = true
			data := []byte(body)
			if _, err := fmt.Fprintf(client, "%04x", len(data)); err != nil {
				return
			}
			if _, err := client.Write(data); err != nil {
				return
			}
		case <-clientClosed:
			return
		}
	}
}
