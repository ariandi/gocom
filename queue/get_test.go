package queue

import (
	"strings"
	"testing"
	"time"

	"github.com/ariandi/gocom/internal/natstest"
	"github.com/spf13/viper"
)

// configure plays the part of config.properties for the global config package.
func configure(t *testing.T, props string) {
	t.Helper()

	viper.SetConfigType("properties")
	if err := viper.ReadConfig(strings.NewReader(props)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = viper.ReadConfig(strings.NewReader("")) })
}

// The public entry point end to end: app.queue.* config -> Get() -> a client that works.
func TestGet_ReturnsAWorkingClientFromConfig(t *testing.T) {
	srv := natstest.Start(t)
	configure(t, "app.queue.e2e.type=nats\napp.queue.e2e.url="+srv.URL()+"\n")
	useNatsConfig(t, fast)
	t.Cleanup(func() { delete(queueMap, "e2e") })

	q := Get("e2e")
	if q == nil {
		t.Fatal("Get returned nil with a healthy broker")
	}
	client := q.(*NatsQueueClient)
	t.Cleanup(client.conn.Close)

	got := make(chan string, 1)
	q.Consume("e2e.jobs", func(_, msg string) { got <- msg })
	flush(t, client)

	if err := q.Publish("e2e.jobs", "hello"); err != nil {
		t.Fatal(err)
	}
	receive(t, got, "hello")
}

// A dead broker must still come back from Get() as a nil interface.
func TestGet_ReturnsNilWhenNoBrokerAnswers(t *testing.T) {
	srv := natstest.Start(t)
	url := srv.URL()
	srv.Stop()

	configure(t, "app.queue.e2edown.type=nats\napp.queue.e2edown.url="+url+"\n")
	useNatsConfig(t, fast)
	t.Cleanup(func() { delete(queueMap, "e2edown") })

	if q := Get("e2edown"); q != nil {
		t.Fatalf("Get returned %T for a dead broker; startup guards `if queue.Get() == nil` would stop firing", q)
	}
}

// The default wiring, with appNatsConfig left alone: the app.nats.* keys of the
// config file are what decide how the client connects. With
// retry-on-failed-connect=true a dead broker at startup still yields a client, which
// connects by itself once the broker is up.
func TestGet_HonoursTheNatsKeysOfTheConfigFile(t *testing.T) {
	srv := natstest.Start(t)
	url := srv.URL()
	srv.Stop()

	configure(t, "app.queue.e2eretry.type=nats\napp.queue.e2eretry.url="+url+"\napp.nats.retry-on-failed-connect=true\napp.nats.reconnect-wait=5ms\n")
	t.Cleanup(func() { delete(queueMap, "e2eretry") })

	c := Get("e2eretry")
	if c == nil {
		t.Fatal("with retry-on-failed-connect=true in the config file, Get must hand out a client while the broker is down")
	}
	client := c.(*NatsQueueClient)
	t.Cleanup(client.conn.Close)

	srv.Restart()
	natstest.Eventually(t, 5*time.Second, client.conn.IsConnected, "the client connects by itself once the broker is up")
}
