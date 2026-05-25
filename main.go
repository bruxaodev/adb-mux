package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultPort = 5037
	defaultBind = "127.0.0.1"
)

type Config struct {
	Bind      string     `json:"bind"`
	Port      int        `json:"port"`
	Upstreams []Upstream `json:"upstreams"`
}

func configDir() string {
	if v := os.Getenv("ADB_MUX_HOME"); v != "" {
		return v
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "adb-mux")
}

func configFile() string { return filepath.Join(configDir(), "config.json") }
func pidFile() string    { return filepath.Join(configDir(), "daemon.pid") }
func logFile() string    { return filepath.Join(configDir(), "daemon.log") }

func loadConfig() (Config, error) {
	cfg := Config{Port: defaultPort, Bind: defaultBind}
	b, err := os.ReadFile(configFile())
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, err
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	if cfg.Port == 0 {
		cfg.Port = defaultPort
	}
	if cfg.Bind == "" {
		cfg.Bind = defaultBind
	}
	return cfg, nil
}

func saveConfig(cfg Config) error {
	if err := os.MkdirAll(configDir(), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(configFile(), b, 0o644)
}

// -------- logging --------

var (
	logMu sync.Mutex
	logFH *os.File
)

func logf(format string, args ...any) {
	logMu.Lock()
	defer logMu.Unlock()
	line := fmt.Sprintf("[%s] %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
	if logFH != nil {
		logFH.WriteString(line)
	} else {
		os.Stdout.WriteString(line)
	}
}

// -------- daemon --------

func readPID() (int, bool) {
	b, err := os.ReadFile(pidFile())
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		os.Remove(pidFile())
		return 0, false
	}
	// Verifica se está vivo
	if err := syscall.Kill(pid, 0); err != nil {
		os.Remove(pidFile())
		return 0, false
	}
	return pid, true
}

func isAddrFree(bind string, port int) bool {
	addr := net.JoinHostPort(bind, strconv.Itoa(port))
	l, err := net.Listen("tcp", addr)
	if err == nil {
		l.Close()
		return true
	}
	// Verifica se alguém realmente escuta (não só TIME_WAIT)
	probe := bind
	if probe == "0.0.0.0" || probe == "::" {
		probe = "127.0.0.1"
	}
	c, derr := net.DialTimeout("tcp", net.JoinHostPort(probe, strconv.Itoa(port)), 200*time.Millisecond)
	if derr == nil {
		c.Close()
		return false
	}
	return true
}

func runServer(bind string, port int, ups []Upstream) error {
	state := &MuxState{Upstreams: ups}
	addr := net.JoinHostPort(bind, strconv.Itoa(port))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	logf("adb-mux ouvindo em %s", addr)
	logf("upstreams: %v", ups)

	// Sinal para shutdown limpo
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		logf("encerrando...")
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go state.handleClient(conn)
	}
}

// -------- subcomandos --------

func cmdStart(args []string) int {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	foreground := fs.Bool("f", false, "roda em foreground")
	fs.Parse(args)

	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "erro lendo config:", err)
		return 1
	}
	if len(cfg.Upstreams) == 0 {
		fmt.Fprintln(os.Stderr, "Nenhum upstream configurado. Use: adb-mux add-server <host>:<port>")
		return 2
	}
	if pid, ok := readPID(); ok {
		fmt.Printf("Já em execução (pid %d).\n", pid)
		return 0
	}
	if !isAddrFree(cfg.Bind, cfg.Port) {
		fmt.Fprintf(os.Stderr, "Endereço %s:%d já em uso. Rode: adb kill-server\n", cfg.Bind, cfg.Port)
		return 1
	}
	if cfg.Bind != "127.0.0.1" && cfg.Bind != "localhost" {
		fmt.Fprintf(os.Stderr, "AVISO: bindando em %s — qualquer um na rede que alcance esse IP poderá controlar seus devices.\n", cfg.Bind)
	}

	if *foreground {
		return runForeground(cfg)
	}
	return runDaemonized(cfg)
}

func runForeground(cfg Config) int {
	if err := os.MkdirAll(configDir(), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "mkdir config:", err)
		return 1
	}
	// Se for filho do fork, redireciona log para arquivo
	if os.Getenv("ADB_MUX_DAEMONIZED") == "1" {
		f, err := os.OpenFile(logFile(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			logFH = f
		}
	}
	os.WriteFile(pidFile(), []byte(strconv.Itoa(os.Getpid())), 0o644)
	defer os.Remove(pidFile())
	err := runServer(cfg.Bind, cfg.Port, cfg.Upstreams)
	if err != nil {
		logf("erro server: %v", err)
		return 1
	}
	return 0
}

// runDaemonized faz re-exec em background com flag escondida.
func runDaemonized(cfg Config) int {
	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintln(os.Stderr, "executable:", err)
		return 1
	}
	cmd := exec.Command(exe, "start", "-f")
	cmd.Env = append(os.Environ(), "ADB_MUX_DAEMONIZED=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "fork:", err)
		return 1
	}
	// Espera um pouquinho e checa se o PID file apareceu
	time.Sleep(300 * time.Millisecond)
	if _, ok := readPID(); ok {
		fmt.Printf("adb-mux iniciado na porta %d (pid em %s)\n", cfg.Port, pidFile())
		return 0
	}
	fmt.Fprintln(os.Stderr, "daemon não iniciou — veja", logFile())
	return 1
}

func cmdStop(_ []string) int {
	pid, ok := readPID()
	if !ok {
		fmt.Println("Não está em execução.")
		return 0
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		fmt.Fprintf(os.Stderr, "Falha ao parar pid %d: %v\n", pid, err)
		return 1
	}
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		if err := syscall.Kill(pid, 0); err != nil {
			break
		}
	}
	os.Remove(pidFile())
	fmt.Printf("Parado (pid %d).\n", pid)
	return 0
}

func cmdStatus(_ []string) int {
	cfg, _ := loadConfig()
	fmt.Printf("bind:       %s\n", cfg.Bind)
	fmt.Printf("porta:      %d\n", cfg.Port)
	pid, ok := readPID()
	if ok {
		fmt.Printf("estado:     rodando (pid %d)\n", pid)
	} else {
		fmt.Println("estado:     parado")
	}
	if len(cfg.Upstreams) == 0 {
		fmt.Println("upstreams:  (nenhum)")
	} else {
		fmt.Println("upstreams:")
		for _, u := range cfg.Upstreams {
			fmt.Printf("  - %s\n", u)
		}
	}
	fmt.Printf("log:        %s\n", logFile())
	return 0
}

func parseHostPort(s string) (string, int, error) {
	if i := strings.LastIndex(s, ":"); i >= 0 {
		h := s[:i]
		p, err := strconv.Atoi(s[i+1:])
		if err != nil {
			return "", 0, err
		}
		return h, p, nil
	}
	return s, 5037, nil
}

func cmdAddServer(args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "uso: adb-mux add-server <host>[:<port>]")
		return 1
	}
	h, p, err := parseHostPort(args[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, "alvo invalido:", err)
		return 1
	}
	cfg, _ := loadConfig()
	for _, u := range cfg.Upstreams {
		if u.Host == h && u.Port == p {
			fmt.Printf("Já existe: %s:%d\n", h, p)
			return 0
		}
	}
	cfg.Upstreams = append(cfg.Upstreams, Upstream{Host: h, Port: p})
	if err := saveConfig(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "erro salvando:", err)
		return 1
	}
	fmt.Printf("Adicionado: %s:%d\n", h, p)
	if _, ok := readPID(); ok {
		fmt.Println("Reinicie o daemon para aplicar: adb-mux stop && adb-mux start")
	}
	return 0
}

func cmdRemoveServer(args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "uso: adb-mux remove-server <host>[:<port>]")
		return 1
	}
	h, p, err := parseHostPort(args[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, "alvo invalido:", err)
		return 1
	}
	cfg, _ := loadConfig()
	var kept []Upstream
	removed := false
	for _, u := range cfg.Upstreams {
		if u.Host == h && u.Port == p {
			removed = true
			continue
		}
		kept = append(kept, u)
	}
	if !removed {
		fmt.Printf("Não encontrado: %s:%d\n", h, p)
		return 1
	}
	cfg.Upstreams = kept
	saveConfig(cfg)
	fmt.Printf("Removido: %s:%d\n", h, p)
	if _, ok := readPID(); ok {
		fmt.Println("Reinicie o daemon para aplicar: adb-mux stop && adb-mux start")
	}
	return 0
}

func cmdListServers(_ []string) int {
	cfg, _ := loadConfig()
	if len(cfg.Upstreams) == 0 {
		fmt.Println("(nenhum)")
		return 0
	}
	for _, u := range cfg.Upstreams {
		fmt.Println(u)
	}
	return 0
}

func cmdSetBind(args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "uso: adb-mux set-bind <addr>  (ex: 127.0.0.1, 0.0.0.0, 100.x.y.z)")
		return 1
	}
	cfg, _ := loadConfig()
	cfg.Bind = args[0]
	saveConfig(cfg)
	fmt.Printf("bind = %s\n", cfg.Bind)
	return 0
}

func cmdSetPort(args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "uso: adb-mux set-port <port>")
		return 1
	}
	p, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, "porta invalida:", err)
		return 1
	}
	cfg, _ := loadConfig()
	cfg.Port = p
	saveConfig(cfg)
	fmt.Printf("port = %d\n", p)
	return 0
}

func usage() {
	fmt.Fprintln(os.Stderr, `adb-mux — proxy multiplexador de servidores ADB

uso:
  adb-mux start [-f]
  adb-mux stop
  adb-mux status
  adb-mux list-servers
  adb-mux add-server    <host>[:<port>]
  adb-mux remove-server <host>[:<port>]
  adb-mux set-port      <port>
  adb-mux set-bind      <addr>   (127.0.0.1 padrão | 0.0.0.0 expõe na rede)`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	args := os.Args[2:]
	switch os.Args[1] {
	case "start":
		os.Exit(cmdStart(args))
	case "stop":
		os.Exit(cmdStop(args))
	case "status":
		os.Exit(cmdStatus(args))
	case "list-servers":
		os.Exit(cmdListServers(args))
	case "add-server":
		os.Exit(cmdAddServer(args))
	case "remove-server":
		os.Exit(cmdRemoveServer(args))
	case "set-port":
		os.Exit(cmdSetPort(args))
	case "set-bind":
		os.Exit(cmdSetBind(args))
	case "-h", "--help", "help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
}
