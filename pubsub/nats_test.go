package pubsub

import (
	"testing"
	"time"

	"github.com/ariandi/gocom/internal/natsconn"
	"github.com/ariandi/gocom/internal/natstest"
)

// fast shrinks the reconnect cadence so a long outage fits in a test: nats.go's
// default budget of 60 attempts is used up in a fraction of a second at 2ms apart.
func fast() natsconn.Config {
	return natsconn.Config{ReconnectWait: 2 * time.Millisecond, ReconnectJitter: time.Millisecond, Log: natstest.Quiet{}}
}

// useNatsConfig substitutes appNatsConfig for one test and puts back whatever was
// there before (not a hard-coded default: a later test must still see the package's
// own wiring, or a broken default would hide behind an earlier test's cleanup).
func useNatsConfig(t *testing.T, f func() natsconn.Config) {
	t.Helper()

	prev := appNatsConfig
	appNatsConfig = f
	t.Cleanup(func() { appNatsConfig = prev })
}

func newClient(t *testing.T, url string) *NatsPubSubClient {
	t.Helper()

	c, err := newNatsPubSubClient(url, fast())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	client := c.(*NatsPubSubClient)
	t.Cleanup(client.conn.Close)

	return client
}

// flush returns once the broker has processed everything the client sent so far,
// including subscriptions re-registered after a reconnect.
func flush(t *testing.T, c *NatsPubSubClient) {
	t.Helper()

	if err := c.conn.FlushTimeout(3 * time.Second); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func receive(t *testing.T, ch <-chan string, want string) {
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

// outage takes the broker away for longer than nats.go's default reconnect budget
// and brings it back, failing if either client was closed in between.
func outage(t *testing.T, srv *natstest.Server, clients ...*NatsPubSubClient) {
	t.Helper()

	srv.Stop()
	natstest.Eventually(t, 3*time.Second, func() bool {
		for _, c := range clients {
			if c.conn.IsConnected() {
				return false
			}
		}

		return true
	}, "clients notice the outage")

	time.Sleep(1500 * time.Millisecond)
	for i, c := range clients {
		if c.conn.IsClosed() {
			t.Fatalf("client %d was closed during the outage (last error: %v): from now on Publish fails with \"nats: connection closed\" until the process restarts",
				i, c.conn.LastError())
		}
	}

	srv.Restart()
	natstest.Eventually(t, 5*time.Second, func() bool {
		for _, c := range clients {
			if !c.conn.IsConnected() {
				return false
			}
		}

		return true
	}, "clients reconnect once the broker is back")
	for _, c := range clients {
		flush(t, c)
	}
}

func TestNatsPubSub_PublishSubscribeRoundTrip(t *testing.T) {
	srv := natstest.Start(t)
	pub, sub := newClient(t, srv.URL()), newClient(t, srv.URL())

	got := make(chan string, 4)
	sub.Subscribe("greeting", func(name, msg string) { got <- name + "|" + msg })
	flush(t, sub)

	for _, c := range []struct {
		in   interface{}
		want string
	}{
		{"hello", "hello"},
		{42, "42"},
		{map[string]int{"a": 1}, `{"a":1}`},
	} {
		if err := pub.Publish("greeting", c.in); err != nil {
			t.Fatal(err)
		}
		receive(t, got, "greeting|"+c.want)
	}
}

func TestNatsPubSub_KeepsDeliveringAfterBrokerOutage(t *testing.T) {
	srv := natstest.Start(t)
	pub, sub := newClient(t, srv.URL()), newClient(t, srv.URL())

	got := make(chan string, 8)
	sub.Subscribe("outage", func(_, msg string) { got <- msg })
	sub.QueueSubscribe("outage.queue", "workers", func(_, msg string) { got <- "queue:" + msg })
	flush(t, sub)

	if err := pub.Publish("outage", "before"); err != nil {
		t.Fatal(err)
	}
	receive(t, got, "before")

	outage(t, srv, pub, sub)

	if err := pub.Publish("outage", "after"); err != nil {
		t.Fatalf("publish after the broker came back: %v", err)
	}
	receive(t, got, "after")

	if err := pub.Publish("outage.queue", "after"); err != nil {
		t.Fatal(err)
	}
	receive(t, got, "queue:after")
}

func TestNatsPubSub_RequestReplyKeepsWorkingAfterBrokerOutage(t *testing.T) {
	srv := natstest.Start(t)
	responder, requester := newClient(t, srv.URL()), newClient(t, srv.URL())

	responder.RequestSubscribe("echo", func(_, msg string) string { return "re:" + msg })
	flush(t, responder)

	if got, err := requester.Request("echo", "one", 3*time.Second); err != nil || got != "re:one" {
		t.Fatalf("request before the outage = %q, %v", got, err)
	}

	outage(t, srv, responder, requester)

	if got, err := requester.Request("echo", "two", 3*time.Second); err != nil || got != "re:two" {
		t.Fatalf("request after the outage = %q, %v", got, err)
	}
}

// Callers such as `if pubsub.Get() == nil { os.Exit(1) }` (fail fast, let the
// container restart) depend on a failed connect coming back as a NIL interface:
// a typed nil pointer wrapped in the interface would compare != nil.
func TestNatsPubSub_RegisteredCreatorReturnsNilInterfaceWhenBrokerIsDown(t *testing.T) {
	srv := natstest.Start(t)
	url := srv.URL()
	srv.Stop()

	creator, ok := pubSubCreatorMap["nats"]
	if !ok {
		t.Fatal(`no creator registered for "nats"`)
	}

	client, err := creator(url)
	if err == nil {
		t.Fatal("creator must fail when no broker answers")
	}
	if client != nil {
		t.Fatalf("creator returned %T alongside the error; Get() would hand it out as if it were a client", client)
	}
}

// The registered creator is what pubsub.Get() calls: its tuning must come from the
// application config (the app.nats.* keys), not from a hard-coded Config.
func TestNatsPubSub_RegisteredCreatorUsesTheAppConfig(t *testing.T) {
	srv := natstest.Start(t)
	url := srv.URL()
	srv.Stop()

	useNatsConfig(t, func() natsconn.Config {
		cfg := fast()
		cfg.RetryOnFailedConnect = true

		return cfg
	})

	client, err := pubSubCreatorMap["nats"](url)
	if err != nil {
		t.Fatalf("with retry-on-failed-connect configured, the creator must not fail while the broker is down: %v", err)
	}
	t.Cleanup(client.(*NatsPubSubClient).conn.Close)
}

// The adapter name in the log line is how an operator tells which connection
// dropped when pubsub and queue clients live in the same process.
func TestNatsPubSub_LogsUnderItsOwnName(t *testing.T) {
	srv := natstest.Start(t)
	rec := &natstest.Recorder{}
	cfg := fast()
	cfg.Log = rec
	c, err := newNatsPubSubClient(srv.URL(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.(*NatsPubSubClient).conn.Close)

	srv.Stop()
	natstest.Eventually(t, 3*time.Second, func() bool { return rec.Has("WARN", "gocom pubsub: NATS disconnected") },
		`a line starting "gocom pubsub:" says the broker is gone; log so far:`, rec.Text)
}
