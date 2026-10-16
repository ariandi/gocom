// Package natsconn builds the nats.go connection behind gocom's NATS pubsub and
// queue adapters.
//
// Why it exists: both adapters used to call nats.Connect(url) with no options.
// nats.go's default is 60 reconnect attempts, 2s apart, after which the connection
// is CLOSED for good. A broker that stayed down for more than ~2 minutes therefore
// left every client of the adapters dead until its process was restarted: Publish
// failed with "nats: connection closed", subscriptions silently stopped delivering,
// and Get() kept handing out the same closed client.
package natsconn

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ariandi/gocom/config"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

const (
	// DefaultReconnectWait is the nats.go default, kept on purpose: with a healthy
	// broker, and for outages that were already survivable, nothing changes.
	DefaultReconnectWait = 2 * time.Second

	keyReconnectWait        = "app.nats.reconnect-wait"
	keyRetryOnFailedConnect = "app.nats.retry-on-failed-connect"
	keyReconnectBufSize     = "app.nats.reconnect-buf-size"
)

// Logger is the part of *logrus.Logger the connection handlers use.
type Logger interface {
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// Config tunes Connect. The zero value is the production setting.
type Config struct {
	// ReconnectWait is the pause between reconnect attempts; <= 0 means DefaultReconnectWait.
	ReconnectWait time.Duration

	// ReconnectJitter randomises that pause by up to this much; <= 0 keeps the
	// nats.go default (100ms, 1s over TLS). Tests shrink it.
	ReconnectJitter time.Duration

	// ReconnectBufSize is how many bytes of Publish nats.go keeps in memory while the
	// broker is away and delivers after the reconnect. 0 keeps the nats.go default
	// (8 MiB): Publish returns nil for the whole outage, so a caller that treats nil as
	// "queued" sees success for messages that may go out much later. A negative value
	// turns buffering off: Publish returns nats.ErrReconnectBufExceeded at once while
	// disconnected.
	ReconnectBufSize int

	// RetryOnFailedConnect makes Connect hand back a connection that keeps trying
	// in the background when the broker is down at startup, instead of failing with
	// nats.ErrNoServers. Off by default: callers such as
	// `if pubsub.Get() == nil { os.Exit(1) }` (fail fast, let the container
	// restart) rely on that failure.
	RetryOnFailedConnect bool

	// Log receives the connection events. nil means the logrus standard logger,
	// which is the one gocom/logger.SetupLogs wires to stdout, stderr and the log file.
	Log Logger
}

func (c Config) withDefaults() Config {
	if c.ReconnectWait <= 0 {
		c.ReconnectWait = DefaultReconnectWait
	}
	if c.Log == nil {
		c.Log = logrus.StandardLogger()
	}

	return c
}

// ConfigFromApp reads the optional process-wide keys
//
//	app.nats.reconnect-wait             Go duration WITH a unit, e.g. 2s; missing, unparsable or <= 0 means 2s
//	app.nats.retry-on-failed-connect    true/1/t to survive a broker that is down at startup; anything else means off
//	app.nats.reconnect-buf-size         bytes buffered while disconnected; -1 = none; missing or unparsable means 8 MiB
//
// Nothing needs to be configured: without the keys the production defaults apply.
func ConfigFromApp() Config { return configFrom(config.Get) }

func configFrom(get func(name string, defVal ...string) string) Config {
	var c Config

	if d, err := time.ParseDuration(strings.TrimSpace(get(keyReconnectWait))); err == nil && d > 0 {
		c.ReconnectWait = d
	}
	c.RetryOnFailedConnect, _ = strconv.ParseBool(strings.TrimSpace(get(keyRetryOnFailedConnect))) // invalid means off
	if n, err := strconv.Atoi(strings.TrimSpace(get(keyReconnectBufSize))); err == nil {
		c.ReconnectBufSize = n
	}

	return c
}

// Connect dials connString (nats.go syntax: one URL, or several separated by
// commas). role names the adapter ("pubsub", "queue") in the log lines.
//
// Compared with a bare nats.Connect it keeps reconnecting for as long as the broker
// is unreachable (MaxReconnects(-1)) and logs disconnects, reconnects and a permanent
// close. Every other option stays at the nats.go default.
//
// What still ends the connection is nats.go's own rule that the same authentication
// error twice in a row from a server means "stop" (so a broker that rejects the
// credentials is not retried forever) and a Close by the application. Both end in
// the ERROR line of the Closed handler.
//
// Log lines and the returned error carry host:port only: the connection string may
// hold credentials (nats://user:secret@host, nats://token@host).
func Connect(role, connString string, cfg Config) (*nats.Conn, error) {
	cfg = cfg.withDefaults()
	servers := Servers(connString)

	opts := []nats.Option{
		nats.MaxReconnects(-1),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			cfg.Log.Warnf("gocom %s: NATS disconnected from %s (%s); retrying every %s until it is back",
				role, servers, errText(err, "connection lost"), cfg.ReconnectWait)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			cfg.Log.Infof("gocom %s: NATS reconnected to %s", role, reconnectedTo(nc.ConnectedUrl(), servers))
		}),
		// With unlimited reconnects this only fires when the process closed the
		// connection or the broker keeps refusing the credentials: nothing will be
		// delivered through it again, so it must not pass quietly.
		nats.ClosedHandler(func(nc *nats.Conn) {
			cfg.Log.Errorf("gocom %s: NATS connection to %s closed for good (%s); nothing is published or delivered through it until the process restarts",
				role, servers, errText(nc.LastError(), "closed by the application"))
		}),
	}
	if cfg.ReconnectJitter > 0 {
		opts = append(opts, nats.ReconnectJitter(cfg.ReconnectJitter, cfg.ReconnectJitter))
	}
	if cfg.RetryOnFailedConnect {
		opts = append(opts, nats.RetryOnFailedConnect(true))
	}
	if cfg.ReconnectBufSize != 0 {
		size := cfg.ReconnectBufSize
		if size < 0 {
			size = -1
		}
		opts = append(opts, nats.ReconnectBufSize(size))
	}

	nc, err := nats.Connect(connString, opts...)
	if err != nil {
		return nil, scrub(err)
	}

	if cfg.RetryOnFailedConnect && !nc.IsConnected() {
		cfg.Log.Warnf("gocom %s: NATS broker at %s unreachable at startup; retrying every %s in the background",
			role, servers, cfg.ReconnectWait)
	}

	return nc, nil
}

// Servers renders a connection string for logs: host:port of each server and
// nothing else (no scheme, credentials, path or query). An entry that cannot be
// parsed becomes "?" instead of being echoed, because it may be the credentials.
func Servers(connString string) string {
	var hosts []string
	for _, entry := range strings.Split(connString, ",") {
		if entry = strings.TrimSpace(entry); entry != "" {
			hosts = append(hosts, hostPort(entry))
		}
	}
	if len(hosts) == 0 {
		return hostPort(nats.DefaultURL) // what nats.Connect("") dials
	}

	return strings.Join(hosts, ",")
}

// reconnectedTo names the server for the "reconnected" line. nats.go runs the
// callback asynchronously, so by then ConnectedUrl() can be empty again (the link
// dropped once more); the configured list is the honest fallback.
func reconnectedTo(connectedURL, configured string) string {
	if strings.TrimSpace(connectedURL) == "" {
		return configured
	}

	return Servers(connectedURL)
}

// scrub keeps credentials out of the error Connect hands back. nats.go reports a
// malformed connection string as a *url.Error that quotes the whole string, and
// callers print it (pubsub.Get logs "Error create PubSub : <err>").
func scrub(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return errors.New("nats: invalid connection string (not shown: it may contain credentials)")
	}

	return err
}

func hostPort(entry string) string {
	if !strings.Contains(entry, "://") {
		entry = "nats://" + entry // nats.go accepts a bare host:port
	}

	u, err := url.Parse(entry)
	if err != nil || u.Host == "" {
		return "?"
	}
	// An unescaped '/', '?' or '#' typed inside the userinfo ends the authority early,
	// and u.Host is then a piece of the credentials. An '@' with no userinfo parsed
	// out of it is that case.
	if u.User == nil && strings.Contains(entry, "@") {
		return "?"
	}

	return u.Host
}

func errText(err error, none string) string {
	if err == nil {
		return none
	}

	return err.Error()
}
