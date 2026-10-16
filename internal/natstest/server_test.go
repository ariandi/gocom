package natstest

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// patient connects with timings fast enough for tests and, unlike the nats.go
// default, never gives up reconnecting.
func patient(t *testing.T, url string) *nats.Conn {
	t.Helper()

	nc, err := nats.Connect(url,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Millisecond),
		nats.ReconnectJitter(time.Millisecond, time.Millisecond),
	)
	if err != nil {
		t.Fatalf("connect %s: %v", url, err)
	}
	t.Cleanup(nc.Close)

	return nc
}

func recv(t *testing.T, ch <-chan string, want string) {
	t.Helper()

	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("no message, want %q", want)
	}
}

func TestServer_DeliversToExactAndWildcardSubscribers(t *testing.T) {
	srv := Start(t)
	pub, sub := patient(t, srv.URL()), patient(t, srv.URL())

	exact, star, rest := make(chan string, 4), make(chan string, 4), make(chan string, 4)
	for pattern, ch := range map[string]chan string{"a.b": exact, "a.*": star, "a.>": rest} {
		ch := ch
		if _, err := sub.Subscribe(pattern, func(m *nats.Msg) { ch <- string(m.Data) }); err != nil {
			t.Fatal(err)
		}
	}
	if err := sub.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := pub.Publish("a.b", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	recv(t, exact, "hello")
	recv(t, star, "hello")
	recv(t, rest, "hello")

	if err := pub.Publish("a.b.c", []byte("deep")); err != nil {
		t.Fatal(err)
	}
	recv(t, rest, "deep")
	if err := pub.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(exact)+len(star) != 0 {
		t.Fatalf("a.b.c must reach only a.> (exact=%d star=%d)", len(exact), len(star))
	}
}

func TestServer_QueueGroupDeliversToOneMember(t *testing.T) {
	srv := Start(t)
	pub, c1, c2 := patient(t, srv.URL()), patient(t, srv.URL()), patient(t, srv.URL())

	got := make(chan string, 8)
	for _, c := range []*nats.Conn{c1, c2} {
		if _, err := c.QueueSubscribe("work", "workers", func(m *nats.Msg) { got <- string(m.Data) }); err != nil {
			t.Fatal(err)
		}
		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	if err := pub.Publish("work", []byte("job")); err != nil {
		t.Fatal(err)
	}
	recv(t, got, "job")
	if err := pub.Flush(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if len(got) != 0 {
		t.Fatalf("queue group delivered the message %d extra time(s)", len(got))
	}
}

func TestServer_RequestReply(t *testing.T) {
	srv := Start(t)
	responder, requester := patient(t, srv.URL()), patient(t, srv.URL())

	if _, err := responder.Subscribe("echo", func(m *nats.Msg) { _ = m.Respond(append([]byte("re:"), m.Data...)) }); err != nil {
		t.Fatal(err)
	}
	if err := responder.Flush(); err != nil {
		t.Fatal(err)
	}

	resp, err := requester.Request("echo", []byte("ping"), 3*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if string(resp.Data) != "re:ping" {
		t.Fatalf("reply = %q", resp.Data)
	}
}

// The reason the server exists: it must look like a dead broker when stopped and
// come back on the same address with the clients' subscriptions restorable.
func TestServer_StopAndRestartOnSameAddress(t *testing.T) {
	srv := Start(t)
	addr := srv.Addr()
	pub, sub := patient(t, srv.URL()), patient(t, srv.URL())

	got := make(chan string, 4)
	if _, err := sub.Subscribe("t", func(m *nats.Msg) { got <- string(m.Data) }); err != nil {
		t.Fatal(err)
	}
	if err := sub.Flush(); err != nil {
		t.Fatal(err)
	}
	Eventually(t, 3*time.Second, func() bool { return srv.ConnCount() == 2 }, "both clients connected")

	srv.Stop()
	Eventually(t, 3*time.Second, func() bool { return !pub.IsConnected() && !sub.IsConnected() }, "clients notice the outage")
	if srv.ConnCount() != 0 {
		t.Fatalf("ConnCount after Stop = %d", srv.ConnCount())
	}

	srv.Restart()
	if srv.Addr() != addr {
		t.Fatalf("restarted on %s, want %s", srv.Addr(), addr)
	}
	Eventually(t, 5*time.Second, func() bool { return pub.IsConnected() && sub.IsConnected() }, "clients reconnect")
	if err := sub.FlushTimeout(3 * time.Second); err != nil {
		t.Fatalf("subscriber flush after reconnect: %v", err)
	}

	if err := pub.Publish("t", []byte("again")); err != nil {
		t.Fatal(err)
	}
	recv(t, got, "again")
}

func TestServer_StopIsIdempotent(t *testing.T) {
	srv := Start(t)
	_ = patient(t, srv.URL())

	srv.Stop()
	srv.Stop()
}
