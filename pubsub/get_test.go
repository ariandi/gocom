package pubsub

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

// The public entry point end to end: app.pubsub.* config -> Get() -> a client that works.
func TestGet_ReturnsAWorkingClientFromConfig(t *testing.T) {
	srv := natstest.Start(t)
	configure(t, "app.pubsub.e2e.type=nats\napp.pubsub.e2e.url="+srv.URL()+"\n")
	useNatsConfig(t, fast)
	t.Cleanup(func() { delete(pubSubMap, "e2e") })

	ps := Get("e2e")
	if ps == nil {
		t.Fatal("Get returned nil with a healthy broker")
	}
	client := ps.(*NatsPubSubClient)
	t.Cleanup(client.conn.Close)

	got := make(chan string, 1)
	ps.Subscribe("e2e.topic", func(_, msg string) { got <- msg })
	flush(t, client)

	if err := ps.Publish("e2e.topic", "hello"); err != nil {
		t.Fatal(err)
	}
	receive(t, got, "hello")
}

// Unchanged on purpose: il-scheduller and il-ai run `if pubsub.Get() == nil { exit }`
// at startup, so a dead broker must still come back from Get() as a nil interface.
func TestGet_ReturnsNilWhenNoBrokerAnswers(t *testing.T) {
	srv := natstest.Start(t)
	url := srv.URL()
	srv.Stop()

	configure(t, "app.pubsub.e2edown.type=nats\napp.pubsub.e2edown.url="+url+"\n")
	useNatsConfig(t, fast)
	t.Cleanup(func() { delete(pubSubMap, "e2edown") })

	if ps := Get("e2edown"); ps != nil {
		t.Fatalf("Get returned %T for a dead broker; startup guards `if pubsub.Get() == nil` would stop firing", ps)
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

	configure(t, "app.pubsub.e2eretry.type=nats\napp.pubsub.e2eretry.url="+url+"\napp.nats.retry-on-failed-connect=true\napp.nats.reconnect-wait=5ms\n")
	t.Cleanup(func() { delete(pubSubMap, "e2eretry") })

	c := Get("e2eretry")
	if c == nil {
		t.Fatal("with retry-on-failed-connect=true in the config file, Get must hand out a client while the broker is down")
	}
	client := c.(*NatsPubSubClient)
	t.Cleanup(client.conn.Close)

	srv.Restart()
	natstest.Eventually(t, 5*time.Second, client.conn.IsConnected, "the client connects by itself once the broker is up")
}
