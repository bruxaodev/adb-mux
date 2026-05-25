package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

const upstreamTimeout = 3 * time.Second

func readExact(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}

// readRequest lê um pacote smart-socket: 4 hex chars de tamanho + payload.
func readRequest(r io.Reader) (string, error) {
	hdr, err := readExact(r, 4)
	if err != nil {
		return "", err
	}
	length, err := strconv.ParseInt(string(hdr), 16, 32)
	if err != nil {
		return "", fmt.Errorf("header invalido %q: %w", hdr, err)
	}
	if length == 0 {
		return "", nil
	}
	payload, err := readExact(r, int(length))
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func encodeRequest(s string) []byte {
	out := make([]byte, 0, 4+len(s))
	out = append(out, []byte(fmt.Sprintf("%04x", len(s)))...)
	out = append(out, s...)
	return out
}

func readStatus(r io.Reader) ([]byte, error) {
	return readExact(r, 4)
}

func readLengthPrefixed(r io.Reader) ([]byte, error) {
	hdr, err := readExact(r, 4)
	if err != nil {
		return nil, err
	}
	length, err := strconv.ParseInt(string(hdr), 16, 32)
	if err != nil {
		return nil, fmt.Errorf("header invalido %q: %w", hdr, err)
	}
	if length == 0 {
		return []byte{}, nil
	}
	return readExact(r, int(length))
}

func writeOKAYPayload(w io.Writer, payload []byte) error {
	if _, err := w.Write([]byte("OKAY")); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "%04x", len(payload)); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func writeFAIL(w io.Writer, msg string) error {
	data := []byte(msg)
	if _, err := w.Write([]byte("FAIL")); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "%04x", len(data)); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

// dialUpstream abre conexão com o ADB upstream.
func dialUpstream(ctx context.Context, host string, port int) (net.Conn, error) {
	d := net.Dialer{Timeout: upstreamTimeout}
	c, cancel := context.WithTimeout(ctx, upstreamTimeout)
	defer cancel()
	return d.DialContext(c, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
}

// upstreamSimple envia um comando host:* e retorna status (OKAY/FAIL) e payload.
func upstreamSimple(ctx context.Context, host string, port int, cmd string) ([]byte, []byte, error) {
	conn, err := dialUpstream(ctx, host, port)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(upstreamTimeout))
	if _, err := conn.Write(encodeRequest(cmd)); err != nil {
		return nil, nil, err
	}
	status, err := readStatus(conn)
	if err != nil {
		return nil, nil, err
	}
	payload, err := readLengthPrefixed(conn)
	if err != nil {
		return nil, nil, err
	}
	return status, payload, nil
}

// pipe copia src->dst até EOF; usado nos dois sentidos após transport switch.
func pipe(dst io.Writer, src io.Reader, done chan<- error) {
	_, err := io.Copy(dst, src)
	done <- err
}

// Hex usado eventualmente em debug; manter pra evitar "unused" se preciso.
var _ = hex.EncodeToString

// ErrNoUpstreams indica que nada está configurado.
var ErrNoUpstreams = errors.New("nenhum upstream configurado")
