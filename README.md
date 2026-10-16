# gocom
ADL Indo golang common library. Provide basic feature for develops api server such as HTTP server, db, key/value, queue and pubsub

## NATS connection (pubsub and queue)

The `nats` adapters of `pubsub` and `queue` keep reconnecting to the broker for as long as it is unreachable
(`MaxReconnects(-1)`, 2s apart) and log disconnects, reconnects and a permanent close (host:port only, never the
credentials in the URL). Before, a broker that stayed down for more than about 2 minutes (nats.go default: 60 attempts
x 2s) left the client closed for good: every `Publish` failed with `nats: connection closed` and subscriptions went
silent until the process was restarted.

What still closes the connection is nats.go's own rule that the same authentication error twice in a row from a server
means "stop" (a broker that keeps rejecting the credentials is not retried forever), and an explicit `Close`. Both are
logged at ERROR (`closed for good`).

Optional keys, none required:

| Key | Default | Meaning |
|---|---|---|
| `app.nats.reconnect-wait` | `2s` | Pause between reconnect attempts: a Go duration WITH a unit (`2s`, `500ms`; plain `5` is not accepted). Missing, unparsable or <= 0 means `2s`. |
| `app.nats.retry-on-failed-connect` | `false` | `true`/`1`: start even when the broker is down and keep trying in the background. Default: `Get()` returns `nil` when no broker answers, so a startup guard such as `if pubsub.Get() == nil { os.Exit(1) }` keeps working. |
| `app.nats.reconnect-buf-size` | `8388608` (8 MiB) | Bytes of `Publish` kept in memory while the broker is away and delivered after the reconnect. `-1`: no buffering, `Publish` returns `nats: outbound buffer limit exceeded` at once while disconnected. |

With the default buffer, `Publish` returns `nil` for the whole outage (until 8 MiB are queued), so a caller that reads
`nil` as "queued" sees success for messages that go out only when the broker is back. Set
`app.nats.reconnect-buf-size=-1` where that is not acceptable. Subscriptions are restored by nats.go on reconnect, but
a message published while a subscriber is disconnected is not delivered to it (NATS core keeps nothing).
