package logger

import (
	"github.com/pkg/errors"
)

const (
	SourceFileName = "file"
	SourceLineName = "name"
	SourceFuncName = "func"
)

type state struct {
	b []byte
}

// Write implement fmt.Formatter interface.
func (s *state) Write(b []byte) (n int, err error) {
	s.b = b
	return len(b), nil
}

// Width implement fmt.Formatter interface.
func (s *state) Width() (wid int, ok bool) {
	return 0, false
}

// Precision implement fmt.Formatter interface.
func (s *state) Precision() (prec int, ok bool) {
	return 0, false
}

// Flag implement fmt.Formatter interface.
func (s *state) Flag(c int) bool {
	return false
}

type stackTracer interface{ StackTrace() errors.StackTrace }

func frameField(f errors.Frame, s *state, c rune) string {
	f.Format(s, c)
	return string(s.b)
}

func MarshalStack(err error) interface{} {

	st, ok := err.(stackTracer)
	if !ok {
		return nil
	}

	sterr := st.StackTrace()
	s := &state{}
	out := make([]map[string]string, 0, len(sterr))
	for _, frame := range sterr {
		out = append(out, map[string]string{
			SourceFileName: frameField(frame, s, 'v'),
			SourceLineName: frameField(frame, s, 'd'),
			SourceFuncName: frameField(frame, s, 'n'),
		})
	}
	return out
}
