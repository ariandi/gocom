package natstest

import (
	"fmt"
	"strings"
	"sync"
)

// Quiet is a logger that drops everything, for tests that do not look at the
// connection log (it satisfies natsconn.Logger).
type Quiet struct{}

func (Quiet) Infof(string, ...interface{})  {}
func (Quiet) Warnf(string, ...interface{})  {}
func (Quiet) Errorf(string, ...interface{}) {}

// Recorder is a logger that keeps every line as "LEVEL message" (it satisfies
// natsconn.Logger).
type Recorder struct {
	mu    sync.Mutex
	lines []string
}

func (r *Recorder) log(level, format string, args ...interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, level+" "+fmt.Sprintf(format, args...))
}

func (r *Recorder) Infof(f string, a ...interface{})  { r.log("INFO", f, a...) }
func (r *Recorder) Warnf(f string, a ...interface{})  { r.log("WARN", f, a...) }
func (r *Recorder) Errorf(f string, a ...interface{}) { r.log("ERROR", f, a...) }

// Text is every line so far, one per line.
func (r *Recorder) Text() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return strings.Join(r.lines, "\n")
}

// Has reports whether a line of that level contains substr.
func (r *Recorder) Has(level, substr string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, l := range r.lines {
		if strings.HasPrefix(l, level+" ") && strings.Contains(l, substr) {
			return true
		}
	}

	return false
}
