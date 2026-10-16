package natsconn

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ariandi/gocom/internal/natstest"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// syncBuffer is a bytes.Buffer that is safe to read while nats.go's callback
// goroutine writes to it through logrus.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.b.String()
}

// fast shrinks the reconnect cadence so a "long" outage fits in a test: nats.go's
// default budget of 60 attempts is used up in a fraction of a second at 2ms apart.
func fast() Config {
	return Config{ReconnectWait: 2 * time.Millisecond, ReconnectJitter: time.Millisecond, Log: &natstest.Recorder{}}
}

func mustConnect(t *testing.T, url string, cfg Config) *nats.Conn {
	t.Helper()

	nc, err := Connect("test", url, cfg)
	if err != nil {
		t.Fatalf("Connect(%s): %v", Servers(url), err)
	}
	t.Cleanup(nc.Close)

	return nc
}

// observer is a plain nats.go client that never gives up, so it is not the thing
// under test when a message does or does not arrive.
func observer(t *testing.T, url string) *nats.Conn {
	t.Helper()

	nc, err := nats.Connect(url,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Millisecond),
		nats.ReconnectJitter(time.Millisecond, time.Millisecond),
	)
	if err != nil {
		t.Fatalf("observer: %v", err)
	}
	t.Cleanup(nc.Close)

	return nc
}

func expect(t *testing.T, ch <-chan string, want string) {
	t.Helper()

	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("no message arrived, want %q", want)
	}
}

// The incident: a broker that stays down longer than nats.go's default budget
// (60 attempts x 2s) killed the client for good, and every Publish afterwards
// failed with "nats: connection closed" until the process was restarted.
func TestConnect_SurvivesBrokerOutageLongerThanNatsGoDefaultBudget(t *testing.T) {
	srv := natstest.Start(t)
	pub := mustConnect(t, srv.URL(), fast())
	obs := observer(t, srv.URL())

	got := make(chan string, 4)
	if _, err := obs.Subscribe("outage.test", func(m *nats.Msg) { got <- string(m.Data) }); err != nil {
		t.Fatal(err)
	}
	if err := obs.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := pub.Publish("outage.test", []byte("before")); err != nil {
		t.Fatal(err)
	}
	expect(t, got, "before")

	srv.Stop()
	natstest.Eventually(t, 3*time.Second, func() bool { return !pub.IsConnected() }, "publisher notices the outage")

	// 60 attempts at ~3ms are gone within half a second; stay down three times as long.
	time.Sleep(1500 * time.Millisecond)
	if pub.IsClosed() {
		t.Fatalf("connection was closed during the outage (last error: %v): every Publish from now on fails with %q until the process restarts",
			pub.LastError(), nats.ErrConnectionClosed)
	}

	srv.Restart()
	natstest.Eventually(t, 5*time.Second, func() bool { return pub.IsConnected() && obs.IsConnected() }, "both ends reconnect")
	if err := obs.FlushTimeout(3 * time.Second); err != nil { // the re-registered subscription is in place
		t.Fatalf("observer flush after reconnect: %v", err)
	}

	if err := pub.Publish("outage.test", []byte("after")); err != nil {
		t.Fatalf("publish after the broker came back: %v", err)
	}
	expect(t, got, "after")
}

func TestConnect_LogsDisconnectAndReconnectWithoutLeakingCredentials(t *testing.T) {
	for name, userinfo := range map[string]string{
		"user and password": "svc-user:hunter2@",
		"token":             "s3cr3t-token@",
	} {
		userinfo := userinfo
		t.Run(name, func(t *testing.T) {
			srv := natstest.Start(t)
			rec := &natstest.Recorder{}
			cfg := fast()
			cfg.Log = rec
			mustConnect(t, strings.Replace(srv.URL(), "nats://", "nats://"+userinfo, 1), cfg)

			srv.Stop()
			natstest.Eventually(t, 3*time.Second, func() bool { return rec.Has("WARN", "disconnected") }, "a WARN line says the broker is gone; log so far:", rec.Text)

			srv.Restart()
			natstest.Eventually(t, 5*time.Second, func() bool { return rec.Has("INFO", "reconnected") }, "an INFO line says the broker is back; log so far:", rec.Text)

			logs := rec.Text()
			for _, secret := range []string{"hunter2", "svc-user", "s3cr3t-token"} {
				if strings.Contains(logs, secret) {
					t.Fatalf("log leaks %q:\n%s", secret, logs)
				}
			}
			if !strings.Contains(logs, srv.Addr()) {
				t.Fatalf("log should name the broker (%s) so the line is actionable:\n%s", srv.Addr(), logs)
			}
			if !strings.Contains(logs, "test") {
				t.Fatalf("log should carry the adapter name:\n%s", logs)
			}
		})
	}
}

// With unlimited reconnects, a CLOSED connection means something other than an
// outage (the process closed it, or the broker keeps refusing the credentials).
// That must be loud: nothing will ever be delivered through it again.
func TestConnect_LogsPermanentCloseAsError(t *testing.T) {
	srv := natstest.Start(t)
	rec := &natstest.Recorder{}
	cfg := fast()
	cfg.Log = rec
	nc := mustConnect(t, strings.Replace(srv.URL(), "nats://", "nats://svc-user:hunter2@", 1), cfg)

	nc.Close()

	natstest.Eventually(t, 3*time.Second, func() bool { return rec.Has("ERROR", "closed") }, "an ERROR line says the connection is gone for good; log so far:", rec.Text)

	logs := rec.Text()
	for _, secret := range []string{"hunter2", "svc-user"} {
		if strings.Contains(logs, secret) {
			t.Fatalf("log leaks %q:\n%s", secret, logs)
		}
	}
	if !strings.Contains(logs, srv.Addr()) {
		t.Fatalf("log should name the broker (%s):\n%s", srv.Addr(), logs)
	}
}

// Nobody has to configure a logger: without one the events go to the logrus
// standard logger, the one gocom/logger.SetupLogs wires to stdout, stderr and the log file.
func TestConnect_WithoutALoggerWritesToTheLogrusStandardLogger(t *testing.T) {
	var out syncBuffer
	prev := logrus.StandardLogger().Out
	logrus.SetOutput(&out)
	t.Cleanup(func() { logrus.SetOutput(prev) })

	srv := natstest.Start(t)
	nc := mustConnect(t, srv.URL(), Config{ReconnectWait: 2 * time.Millisecond, ReconnectJitter: time.Millisecond}) // Log left nil

	srv.Stop()
	natstest.Eventually(t, 3*time.Second, func() bool { return strings.Contains(out.String(), "disconnected from "+srv.Addr()) },
		"the default logger receives the disconnect line; it holds:", out.String)

	// Close here and wait for its callback: the cleanup that puts logrus back must not
	// run before nats.go's callback goroutine has written its last line.
	nc.Close()
	natstest.Eventually(t, 3*time.Second, func() bool { return strings.Contains(out.String(), "closed for good") },
		"the default logger receives the close line; it holds:", out.String)
}

func TestConnect_HonoursConfiguredTiming(t *testing.T) {
	srv := natstest.Start(t)
	nc := mustConnect(t, srv.URL(), Config{ReconnectWait: 7 * time.Second, ReconnectJitter: 30 * time.Millisecond, Log: &natstest.Recorder{}})

	if got := nc.Opts.ReconnectWait; got != 7*time.Second {
		t.Errorf("Opts.ReconnectWait = %v, want 7s", got)
	}
	if got := nc.Opts.ReconnectJitter; got != 30*time.Millisecond {
		t.Errorf("Opts.ReconnectJitter = %v, want 30ms", got)
	}
	if got := nc.Opts.ReconnectJitterTLS; got != 30*time.Millisecond {
		t.Errorf("Opts.ReconnectJitterTLS = %v, want 30ms", got)
	}
}

// Without a config file none of the app.nats.* keys exist, and that must mean the
// production defaults rather than a panic or a half-set Config.
func TestConfigFromApp_WithoutConfigKeysIsTheProductionDefault(t *testing.T) {
	if got := ConfigFromApp(); got.ReconnectWait != 0 || got.ReconnectJitter != 0 || got.RetryOnFailedConnect || got.Log != nil {
		t.Fatalf("ConfigFromApp() = %+v, want the zero Config", got)
	}
}

// Unchanged on purpose: callers such as `if pubsub.Get() == nil { os.Exit(1) }`
// (fail fast, let the container restart) depend on Connect failing when no broker
// answers at startup.
func TestConnect_BrokerDownAtStartup_FailsFastByDefault(t *testing.T) {
	srv := natstest.Start(t)
	url := srv.URL()
	srv.Stop()

	nc, err := Connect("test", url, fast())
	if err == nil {
		nc.Close()
		t.Fatal("Connect must fail when no broker answers")
	}
	if !errors.Is(err, nats.ErrNoServers) {
		t.Fatalf("err = %v, want %v", err, nats.ErrNoServers)
	}
}

func TestConnect_RetryOnFailedConnect_ConnectsOnceTheBrokerAppears(t *testing.T) {
	srv := natstest.Start(t)
	url := srv.URL()
	srv.Stop()

	rec := &natstest.Recorder{}
	cfg := fast()
	cfg.Log = rec
	cfg.RetryOnFailedConnect = true

	nc, err := Connect("test", url, cfg)
	if err != nil {
		t.Fatalf("with RetryOnFailedConnect, Connect must not fail while the broker is down: %v", err)
	}
	t.Cleanup(nc.Close)
	if nc.IsConnected() {
		t.Fatal("no broker is running, so the connection cannot be up yet")
	}
	if !rec.Has("WARN", "unreachable") {
		t.Fatalf("startup without a broker must be visible in the log:\n%s", rec.Text())
	}

	srv.Restart()
	natstest.Eventually(t, 5*time.Second, nc.IsConnected, "connects by itself once the broker is up")
	if err := nc.FlushTimeout(3 * time.Second); err != nil {
		t.Fatalf("round trip after the late connect: %v", err)
	}
}

// "Healthy broker: nothing changes." Everything except unlimited reconnects stays
// at the nats.go default, so a future option that quietly alters the healthy path
// has to change a number here.
func TestConnect_KeepsNatsGoDefaultsForTheHealthyPath(t *testing.T) {
	srv := natstest.Start(t)
	nc := mustConnect(t, srv.URL(), Config{Log: &natstest.Recorder{}})

	o := nc.Opts
	for _, c := range []struct {
		name      string
		got, want interface{}
	}{
		{"MaxReconnect", o.MaxReconnect, -1},
		{"ReconnectWait", o.ReconnectWait, 2 * time.Second},
		{"ReconnectJitter", o.ReconnectJitter, 100 * time.Millisecond},
		{"ReconnectJitterTLS", o.ReconnectJitterTLS, time.Second},
		{"Timeout", o.Timeout, 2 * time.Second},
		{"PingInterval", o.PingInterval, 2 * time.Minute},
		{"MaxPingsOut", o.MaxPingsOut, 2},
		{"ReconnectBufSize", o.ReconnectBufSize, 8 * 1024 * 1024},
		{"AllowReconnect", o.AllowReconnect, true},
		{"RetryOnFailedConnect", o.RetryOnFailedConnect, false},
		{"Verbose", o.Verbose, false},
		{"Pedantic", o.Pedantic, false},
		{"NoEcho", o.NoEcho, false},
	} {
		if c.got != c.want {
			t.Errorf("Opts.%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestServers_KeepsOnlyHostAndPort(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"nats://127.0.0.1:4222", "127.0.0.1:4222"},
		{"nats://svc-user:hunter2@broker.internal:4223", "broker.internal:4223"},
		{"nats://svc-user:p@ss@broker.internal:4223", "broker.internal:4223"},
		{"nats://s3cr3t-token@broker.internal:4223", "broker.internal:4223"},
		{"tls://u:p@a:4222/path?x=secret,nats://b:4223", "a:4222,b:4223"},
		{"nats://[::1]:4222", "[::1]:4222"},
		{"broker.internal:4222", "broker.internal:4222"}, // no scheme: nats.go accepts it
		{"  nats://a:1 , nats://b:2  ", "a:1,b:2"},
		{"", "127.0.0.1:4222"},                         // nats.go falls back to its default URL
		{"nats://bad%zz@host:4222", "?"},               // unparsable: never echo the input
		{"nats://ok:1,nats://bad%zz@host:2", "ok:1,?"}, // one bad entry does not hide the others
		// An unescaped '/', '?' or '#' inside the userinfo ends the authority early, so
		// url.Parse reports a piece of the credentials as the host.
		{"nats://TOKEN/ab@host:4222", "?"},
		{"nats://user:12?x@host:4222", "?"},
		{"nats://user:pw#frag@host:4222", "?"},
	} {
		if got := Servers(c.in); got != c.want {
			t.Errorf("Servers(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestConfigFrom(t *testing.T) {
	getter := func(m map[string]string) func(string, ...string) string {
		return func(name string, def ...string) string {
			if v, ok := m[name]; ok {
				return v
			}
			if len(def) > 0 {
				return def[0]
			}

			return ""
		}
	}

	for _, c := range []struct {
		name  string
		in    map[string]string
		wait  time.Duration // 0 = "use the default"
		retry bool
		buf   int // 0 = "use the default"
	}{
		{"nothing configured", nil, 0, false, 0},
		{"all set", map[string]string{"app.nats.reconnect-wait": "750ms", "app.nats.retry-on-failed-connect": "true", "app.nats.reconnect-buf-size": "1048576"}, 750 * time.Millisecond, true, 1048576},
		{"spaces and capitals", map[string]string{"app.nats.reconnect-wait": " 3s ", "app.nats.retry-on-failed-connect": " TRUE ", "app.nats.reconnect-buf-size": " 4096 "}, 3 * time.Second, true, 4096},
		{"unparsable wait falls back to the default", map[string]string{"app.nats.reconnect-wait": "soon"}, 0, false, 0},
		{"a wait without a unit is not guessed", map[string]string{"app.nats.reconnect-wait": "5"}, 0, false, 0},
		{"zero wait falls back to the default", map[string]string{"app.nats.reconnect-wait": "0s"}, 0, false, 0},
		{"negative wait falls back to the default", map[string]string{"app.nats.reconnect-wait": "-5s"}, 0, false, 0},
		{"1 and t switch retry on, like config.GetBool", map[string]string{"app.nats.retry-on-failed-connect": "1"}, 0, true, 0},
		{"t", map[string]string{"app.nats.retry-on-failed-connect": "t"}, 0, true, 0},
		{"anything unrecognised leaves retry off", map[string]string{"app.nats.retry-on-failed-connect": "yes"}, 0, false, 0},
		{"false leaves retry off", map[string]string{"app.nats.retry-on-failed-connect": "false"}, 0, false, 0},
		{"negative buffer size is kept: it means no buffering", map[string]string{"app.nats.reconnect-buf-size": "-1"}, 0, false, -1},
		{"unparsable buffer size falls back to the default", map[string]string{"app.nats.reconnect-buf-size": "8MB"}, 0, false, 0},
	} {
		got := configFrom(getter(c.in))
		if got.ReconnectWait != c.wait || got.RetryOnFailedConnect != c.retry || got.ReconnectBufSize != c.buf {
			t.Errorf("%s: got wait=%v retry=%v buf=%d, want wait=%v retry=%v buf=%d",
				c.name, got.ReconnectWait, got.RetryOnFailedConnect, got.ReconnectBufSize, c.wait, c.retry, c.buf)
		}
	}
}

// End to end through the real config package: the keys an operator puts in
// config.properties reach Connect.
func TestConfigFromApp_ReadsTheAppNatsKeys(t *testing.T) {
	viper.SetConfigType("properties")
	if err := viper.ReadConfig(strings.NewReader("app.nats.reconnect-wait=9s\napp.nats.retry-on-failed-connect=true\n")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = viper.ReadConfig(strings.NewReader("")) })

	got := ConfigFromApp()
	if got.ReconnectWait != 9*time.Second || !got.RetryOnFailedConnect {
		t.Fatalf("ConfigFromApp() = %+v, want ReconnectWait=9s RetryOnFailedConnect=true", got)
	}
}

// nats.go runs the reconnect callback asynchronously, so by the time it runs the
// link can have dropped again and ConnectedUrl() is empty. The line must then name
// the configured servers instead of inventing a host.
func TestReconnectedTo(t *testing.T) {
	for _, c := range []struct{ connected, configured, want string }{
		{"nats://svc-user:hunter2@b:4223", "a:1,b:4223", "b:4223"},
		{"", "a:1,b:2", "a:1,b:2"},
		{"  ", "a:1", "a:1"},
	} {
		if got := reconnectedTo(c.connected, c.configured); got != c.want {
			t.Errorf("reconnectedTo(%q, %q) = %q, want %q", c.connected, c.configured, got, c.want)
		}
	}
}

// A malformed connection string comes back from nats.go as a *url.Error that quotes
// the WHOLE string, credentials included, and callers print it (pubsub.Get logs
// "Error create PubSub : <err>").
func TestConnect_MalformedConnectionStringErrorDoesNotEchoCredentials(t *testing.T) {
	nc, err := Connect("test", "nats://svc-user:hun%zzter2@127.0.0.1:1", fast())
	if err == nil {
		nc.Close()
		t.Fatal("a malformed connection string must be rejected")
	}
	for _, secret := range []string{"hunter2", "hun%zz", "svc-user", "%zz"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaks %q: %v", secret, err)
		}
	}
}

// Unchanged by default: while the broker is away, Publish is accepted into the nats.go
// buffer (8 MB) and delivered after the reconnect, exactly as it was for outages
// shorter than the old two-minute limit.
func TestConnect_PublishWhileDisconnectedIsBufferedByDefault(t *testing.T) {
	srv := natstest.Start(t)
	nc := mustConnect(t, srv.URL(), fast())

	srv.Stop()
	natstest.Eventually(t, 3*time.Second, func() bool { return !nc.IsConnected() }, "the client notices the outage")

	if err := nc.Publish("buffered", []byte("x")); err != nil {
		t.Fatalf("Publish while disconnected = %v, want nil (buffered for the reconnect)", err)
	}
}

// Opt-in (app.nats.reconnect-buf-size=-1): a caller that must know at once that
// nothing is listening gets the error instead of a silent in-memory queue.
func TestConnect_NegativeReconnectBufSizeFailsPublishAtOnceWhileDisconnected(t *testing.T) {
	srv := natstest.Start(t)
	cfg := fast()
	cfg.ReconnectBufSize = -1
	nc := mustConnect(t, srv.URL(), cfg)

	srv.Stop()
	natstest.Eventually(t, 3*time.Second, func() bool { return !nc.IsConnected() }, "the client notices the outage")

	if err := nc.Publish("unbuffered", []byte("x")); !errors.Is(err, nats.ErrReconnectBufExceeded) {
		t.Fatalf("Publish while disconnected = %v, want %v", err, nats.ErrReconnectBufExceeded)
	}
}

func TestConnect_HonoursConfiguredReconnectBufSize(t *testing.T) {
	for _, c := range []struct{ in, want int }{
		{4096, 4096},
		{-1, -1},
		{-7, -1}, // any negative means "no buffering"; nats.go is given its documented -1
	} {
		srv := natstest.Start(t)
		cfg := fast()
		cfg.ReconnectBufSize = c.in
		nc := mustConnect(t, srv.URL(), cfg)

		if got := nc.Opts.ReconnectBufSize; got != c.want {
			t.Errorf("ReconnectBufSize %d: Opts.ReconnectBufSize = %d, want %d", c.in, got, c.want)
		}
	}
}
