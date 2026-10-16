// Package natstest is a minimal in-process NATS server for tests.
//
// It speaks just enough of the client protocol (INFO, CONNECT, PING/PONG, SUB,
// UNSUB, PUB, MSG, with `*` and `>` wildcards and queue groups) for nats.go
// clients to connect, subscribe, publish and request. What the reconnect tests
// need from it is that it can be stopped and started again on the SAME address,
// which is how a broker outage looks from a client. It is not a broker: no
// headers, JetStream, auth or clustering.
package natstest

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Server is the in-process broker. Start it with Start; it is stopped by the
// test cleanup, and Stop/Restart simulate an outage in between.
type Server struct {
	t    testing.TB
	addr string // fixed at Start so Restart comes back on the same port

	mu       sync.Mutex
	ln       net.Listener
	gen      int // bumped by Stop; a session accepted under an older gen is dropped
	sessions map[*session]struct{}
	wg       sync.WaitGroup
}

type session struct {
	conn net.Conn
	wmu  sync.Mutex      // serialises writes to conn
	subs map[string]*sub // sid -> sub, guarded by Server.mu
}

type sub struct {
	sid     string
	subject string
	queue   string
	max     int // 0 = unlimited (UNSUB sid max)
	seen    int
}

// Start listens on a free loopback port and serves until Stop or test cleanup.
func Start(t testing.TB) *Server {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("natstest: listen: %v", err)
	}

	s := &Server{t: t, addr: ln.Addr().String(), sessions: map[*session]struct{}{}}
	s.listen(ln)
	t.Cleanup(s.Stop)

	return s
}

// URL is the connection string clients use.
func (s *Server) URL() string { return "nats://" + s.addr }

// Addr is host:port.
func (s *Server) Addr() string { return s.addr }

// ConnCount is the number of clients currently connected.
func (s *Server) ConnCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.sessions)
}

// Stop closes the listener and every client connection, the way a dead broker
// looks to its clients. It is idempotent.
func (s *Server) Stop() {
	s.mu.Lock()
	ln := s.ln
	s.ln = nil
	s.gen++
	sessions := make([]*session, 0, len(s.sessions))
	for sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.sessions = map[*session]struct{}{}
	s.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}
	for _, sess := range sessions {
		_ = sess.conn.Close()
	}

	s.wg.Wait()
}

// Restart listens again on the address of the first Start.
func (s *Server) Restart() {
	s.t.Helper()

	s.mu.Lock()
	running := s.ln != nil
	s.mu.Unlock()
	if running {
		s.t.Fatalf("natstest: Restart on a running server")
	}

	var (
		ln  net.Listener
		err error
	)
	deadline := time.Now().Add(3 * time.Second)
	for {
		ln, err = net.Listen("tcp", s.addr)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		s.t.Fatalf("natstest: listen again on %s: %v", s.addr, err)
	}

	s.listen(ln)
}

func (s *Server) listen(ln net.Listener) {
	s.mu.Lock()
	s.ln = ln
	gen := s.gen
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed by Stop
			}
			s.wg.Add(1)
			go s.serve(conn, gen)
		}
	}()
}

func (s *Server) serve(conn net.Conn, gen int) {
	defer s.wg.Done()
	defer conn.Close()

	sess := &session{conn: conn, subs: map[string]*sub{}}

	s.mu.Lock()
	if gen != s.gen {
		s.mu.Unlock()
		return
	}
	s.sessions[sess] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.sessions, sess)
		s.mu.Unlock()
	}()

	host, port, _ := net.SplitHostPort(s.addr)
	portNum, _ := strconv.Atoi(port)
	sess.write(fmt.Sprintf("INFO {\"server_id\":\"natstest\",\"server_name\":\"natstest\",\"version\":\"2.10.2\","+
		"\"proto\":1,\"host\":%q,\"port\":%d,\"headers\":false,\"max_payload\":1048576}\r\n", host, portNum))

	r := bufio.NewReader(conn)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}

		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}

		switch strings.ToUpper(f[0]) {
		case "CONNECT", "PONG":
			// nothing to do: no auth, no verbose mode
		case "PING":
			sess.write("PONG\r\n")
		case "SUB": // SUB <subject> [queue] <sid>
			if len(f) != 3 && len(f) != 4 {
				sess.write("-ERR 'Unknown Protocol Operation'\r\n")
				return
			}
			sb := &sub{subject: f[1], sid: f[len(f)-1]}
			if len(f) == 4 {
				sb.queue = f[2]
			}
			s.mu.Lock()
			sess.subs[sb.sid] = sb
			s.mu.Unlock()
		case "UNSUB": // UNSUB <sid> [max]
			if len(f) < 2 {
				sess.write("-ERR 'Unknown Protocol Operation'\r\n")
				return
			}
			max := 0
			if len(f) > 2 {
				max, _ = strconv.Atoi(f[2])
			}
			s.mu.Lock()
			if sb := sess.subs[f[1]]; sb != nil {
				if max > 0 && sb.seen < max {
					sb.max = max
				} else {
					delete(sess.subs, f[1])
				}
			}
			s.mu.Unlock()
		case "PUB": // PUB <subject> [reply] <#bytes>
			if len(f) != 3 && len(f) != 4 {
				sess.write("-ERR 'Unknown Protocol Operation'\r\n")
				return
			}
			size, err := strconv.Atoi(f[len(f)-1])
			if err != nil || size < 0 {
				sess.write("-ERR 'Invalid Payload Size'\r\n")
				return
			}
			payload := make([]byte, size+2) // payload + CRLF
			if _, err := io.ReadFull(r, payload); err != nil {
				return
			}
			reply := ""
			if len(f) == 4 {
				reply = f[2]
			}
			s.route(f[1], reply, payload[:size])
		default:
			sess.write("-ERR 'Unknown Protocol Operation'\r\n")
			return
		}
	}
}

// route delivers a published message to every matching subscription, and to ONE
// member of each queue group.
func (s *Server) route(subject, reply string, payload []byte) {
	type delivery struct {
		sess *session
		msg  []byte
	}
	var out []delivery

	s.mu.Lock()
	served := map[string]bool{}
	for sess := range s.sessions {
		for sid, sb := range sess.subs {
			if !match(sb.subject, subject) {
				continue
			}
			if sb.queue != "" {
				if served[sb.queue] {
					continue
				}
				served[sb.queue] = true
			}

			hdr := "MSG " + subject + " " + sid + " "
			if reply != "" {
				hdr += reply + " "
			}
			hdr += strconv.Itoa(len(payload)) + "\r\n"
			msg := append([]byte(hdr), payload...)
			msg = append(msg, '\r', '\n')
			out = append(out, delivery{sess: sess, msg: msg})

			sb.seen++
			if sb.max > 0 && sb.seen >= sb.max {
				delete(sess.subs, sid)
			}
		}
	}
	s.mu.Unlock()

	for _, d := range out {
		d.sess.writeBytes(d.msg)
	}
}

func (sess *session) write(s string) { sess.writeBytes([]byte(s)) }

func (sess *session) writeBytes(b []byte) {
	sess.wmu.Lock()
	defer sess.wmu.Unlock()
	_, _ = sess.conn.Write(b)
}

// match reports whether a subscription pattern ('*' = one token, '>' = the rest)
// matches a published subject.
func match(pattern, subject string) bool {
	p := strings.Split(pattern, ".")
	s := strings.Split(subject, ".")
	for i, tok := range p {
		if tok == ">" {
			return len(s) > i
		}
		if i >= len(s) {
			return false
		}
		if tok != "*" && tok != s[i] {
			return false
		}
	}

	return len(p) == len(s)
}

// Eventually polls cond every 10ms until it holds, and fails the test if it does not
// within d. what says what was being waited for; each details func is called only
// when the wait fails, so it can report state (a log, say) as of that moment.
// Call it from the test goroutine only.
func Eventually(t testing.TB, d time.Duration, cond func() bool, what string, details ...func() string) {
	t.Helper()

	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			msg := fmt.Sprintf("not true after %s: %s", d, what)
			for _, f := range details {
				msg += "\n" + f()
			}
			t.Fatal(msg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
