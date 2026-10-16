package queue

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

func newClient(t *testing.T, url string) *NatsQueueClient {
	t.Helper()

	c, err := newNatsQueueClient(url, fast())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	client := c.(*NatsQueueClient)
	t.Cleanup(client.conn.Close)

	return client
}

// flush returns once the broker has processed everything the client sent so far,
// including subscriptions re-registered after a reconnect.
func flush(t *testing.T, c *NatsQueueClient) {
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
func outage(t *testing.T, srv *natstest.Server, clients ...*NatsQueueClient) {
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

func TestNatsQueue_PublishConsumeRoundTrip(t *testing.T) {
	srv := natstest.Start(t)
	producer, consumer := newClient(t, srv.URL()), newClient(t, srv.URL())

	got := make(chan string, 4)
	consumer.Consume("jobs", func(name, msg string) { got <- name + "|" + msg })
	consumer.ConsumeRaw("jobs.raw", func(name string, msg []byte) { got <- name + "|" + string(msg) })
	flush(t, consumer)

	if err := producer.Publish("jobs", map[string]int{"id": 7}); err != nil {
		t.Fatal(err)
	}
	receive(t, got, `jobs|{"id":7}`)

	if err := producer.PublishRaw("jobs.raw", []byte("raw")); err != nil {
		t.Fatal(err)
	}
	receive(t, got, "jobs.raw|raw")
}

func TestNatsQueue_KeepsDeliveringAfterBrokerOutage(t *testing.T) {
	srv := natstest.Start(t)
	producer, consumer := newClient(t, srv.URL()), newClient(t, srv.URL())

	got := make(chan string, 4)
	consumer.Consume("outage", func(_, msg string) { got <- msg })
	flush(t, consumer)

	if err := producer.Publish("outage", "before"); err != nil {
		t.Fatal(err)
	}
	receive(t, got, "before")

	outage(t, srv, producer, consumer)

	if err := producer.Publish("outage", "after"); err != nil {
		t.Fatalf("publish after the broker came back: %v", err)
	}
	receive(t, got, "after")
}

func TestNatsQueue_RequestReplyKeepsWorkingAfterBrokerOutage(t *testing.T) {
	srv := natstest.Start(t)
	responder, requester := newClient(t, srv.URL()), newClient(t, srv.URL())

	responder.Reply("echo", func(_, msg string) string { return "re:" + msg })
	flush(t, responder)

	if got, err := requester.Request("echo", "one", 3*time.Second); err != nil || got != "re:one" {
		t.Fatalf("request before the outage = %q, %v", got, err)
	}

	outage(t, srv, responder, requester)

	if got, err := requester.Request("echo", "two", 3*time.Second); err != nil || got != "re:two" {
		t.Fatalf("request after the outage = %q, %v", got, err)
	}
}

// A failed connect must come back as a NIL interface: a typed nil pointer wrapped
// in the interface would compare != nil and defeat `if queue.Get() == nil` guards.
func TestNatsQueue_RegisteredCreatorReturnsNilInterfaceWhenBrokerIsDown(t *testing.T) {
	srv := natstest.Start(t)
	url := srv.URL()
	srv.Stop()

	creator, ok := queueCreatorMap["nats"]
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

// The registered creator is what queue.Get() calls: its tuning must come from the
// application config (the app.nats.* keys), not from a hard-coded Config.
func TestNatsQueue_RegisteredCreatorUsesTheAppConfig(t *testing.T) {
	srv := natstest.Start(t)
	url := srv.URL()
	srv.Stop()

	useNatsConfig(t, func() natsconn.Config {
		cfg := fast()
		cfg.RetryOnFailedConnect = true

		return cfg
	})

	client, err := queueCreatorMap["nats"](url)
	if err != nil {
		t.Fatalf("with retry-on-failed-connect configured, the creator must not fail while the broker is down: %v", err)
	}
	t.Cleanup(client.(*NatsQueueClient).conn.Close)
}

// The adapter name in the log line is how an operator tells which connection
// dropped when pubsub and queue clients live in the same process.
func TestNatsQueue_LogsUnderItsOwnName(t *testing.T) {
	srv := natstest.Start(t)
	rec := &natstest.Recorder{}
	cfg := fast()
	cfg.Log = rec
	c, err := newNatsQueueClient(srv.URL(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.(*NatsQueueClient).conn.Close)

	srv.Stop()
	natstest.Eventually(t, 3*time.Second, func() bool { return rec.Has("WARN", "gocom queue: NATS disconnected") },
		`a line starting "gocom queue:" says the broker is gone; log so far:`, rec.Text)
}
