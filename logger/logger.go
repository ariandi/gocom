package logger

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/ariandi/gocom/constant"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

const logFormat = `date=%s, method=%s, url=%s,  response_time=%s`

var logLevel = flag.String("log_level", "info", "set log level")

// Change the name of flag to ver as v is conflicting with existing flag on service
var version = flag.Bool("ver", false, "for version")

var stdoutLog, stderrLog string

var fnTrace bool

func init() {

	flag.StringVar(&stdoutLog, "loginfo", "", "log file for stdout")
	flag.StringVar(&stderrLog, "logerror", "", "log file for stderr")

	if *version {
		os.Exit(0)
		return
	}
	SetLevel(*logLevel)
}

// New create logger instance with request_id as a field
// also set the default format to JSON.
func New(serviceName, serviceVersion string) *logrus.Entry {
	newLog := logrus.NewEntry(logrus.New())
	newLog.WithFields(logrus.Fields{
		"service": serviceName,
		"version": serviceVersion,
	})
	return newLog
}

func GetLogger(ctx context.Context, pkg, fnName string) *logrus.Entry {
	_, file, _, _ := runtime.Caller(1)
	file = file[strings.LastIndex(file, "/")+1:]
	return WithContext(ctx).WithFields(logrus.Fields{
		"function": fnName,
		"package":  pkg,
		"source":   file,
	})
}

func EnableFnTrace() {
	fnTrace = true
}

func LogInit() {

	if stdoutLog != stderrLog && stdoutLog != "" {
		logrus.Println("Log Init: using ", stdoutLog, stderrLog)
	}

	reopen(1, stdoutLog)
	reopen(2, stderrLog)

	SetupLogs()

}

// WriterHook is a hook that writes logs of specified LogLevels to specified Writer
type WriterHook struct {
	Writer    io.Writer
	LogLevels []logrus.Level
}

// Fire will be called when some logging function is called with current hook
// It will format log entry to string and write it to appropriate writer
func (hook *WriterHook) Fire(entry *logrus.Entry) error {
	line, err := entry.String()
	if err != nil {
		return err
	}
	_, err = hook.Writer.Write([]byte(line))
	return err
}

// Levels define on which log levels this hook would trigger
func (hook *WriterHook) Levels() []logrus.Level {
	return hook.LogLevels
}

// SetupLogs adds hooks to send logs to different destinations depending on level
func SetupLogs(logFile ...string) {
	logrus.SetOutput(ioutil.Discard) // Send all logs to nowhere by default

	filename := "./logs/app.log"
	if len(logFile) > 0 && logFile[0] != "" {
		filename = logFile[0]
	}
	writer, err := NewDailyFileWriter(filename)
	if err != nil {
		logrus.Fatalf("failed to create daily log writer: %v", err)
	}

	logrus.AddHook(&WriterHook{ // Send logs with level higher than warning to stderr
		Writer: os.Stderr,
		LogLevels: []logrus.Level{
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
			logrus.WarnLevel,
		},
	})
	logrus.AddHook(&WriterHook{ // Send info and debug logs to stdout
		Writer: os.Stdout,
		LogLevels: []logrus.Level{
			logrus.InfoLevel,
			logrus.DebugLevel,
		},
	})

	logrus.AddHook(&WriterHook{
		Writer:    writer,
		LogLevels: logrus.AllLevels,
	})
}

func reopen(fd int, filename string) {

	//if filename == "" {
	//	return
	//}
	//
	//logFile, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	//
	//if err != nil {
	//	logrus.Println("Error in opening ", filename, err)
	//	os.Exit(2)
	//}

	//if err = Dup(int(logFile.Fd()), fd); err != nil {
	//	logrus.Println("Failed to dup", filename)
	//}
}

type Fields logrus.Fields

func SetLevel(level string) {
	switch level {
	case "panic":
		logrus.SetLevel(logrus.PanicLevel)
	case "fatal":
		logrus.SetLevel(logrus.FatalLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	case "warning":
		logrus.SetLevel(logrus.WarnLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	default:
		logrus.SetLevel(logrus.InfoLevel)
	}
}

func GetLevel() string {
	return strings.ToUpper(logrus.GetLevel().String())
}

func Request(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t := time.Now()

		reqID := r.Header.Get(constant.RequestIDHeader.String())
		if reqID == "" {
			reqID = uuid.NewV4().String()
		}

		ctx := context.WithValue(r.Context(), constant.RequestIDHeader, reqID)
		r = r.WithContext(ctx)

		logger := GetLogger(ctx, "middleware", "Request")
		logger = logger.WithField("request_id", reqID)
		logger.Infof("Incoming request: %s %s", r.Method, r.RequestURI)

		w.Header().Set(constant.RequestIDHeader.String(), reqID)

		next.ServeHTTP(w, r)
		Infof(logFormat, t, r.Method, r.RequestURI, time.Since(t))
	})

}

func SetFormatter(formatter logrus.Formatter) {
	logrus.SetFormatter(formatter)
}

func Info(args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Info(args...)
}

func Infoln(args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Infoln(args...)
}

func Infof(format string, args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Infof(format, args...)
}

func Print(args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Print(args...)
}

func Println(args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Println(args...)
}

func Printf(format string, args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Printf(format, args...)
}

func Debug(args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Debug(args...)
}

func Debugln(args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Debugln(args...)
}

func Debugf(format string, args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Debugf(format, args...)
}

func Warn(args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Warn(args...)
}

func Warnln(args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Warnln(args...)
}

func Warnf(format string, args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Warnf(format, args...)
}

func Error(args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Error(args...)
}

func Errorln(args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Errorln(args...)
}

func Errorf(format string, args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Errorf(format, args...)
}

func Fatal(args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Fatal(args...)
}

func Fatalln(args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Fatalln(args...)
}

func Fatalf(format string, args ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}
	logrus.WithField("source", fmt.Sprintf("%s:%d", file, line)).Fatalf(format, args...)
}

func WithContext(ctx context.Context) *logrus.Entry {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}

	reqID := uuid.NewV4().String()

	if ctx != nil {
		if rID, ok := ctx.Value(constant.RequestIDHeader).(string); ok {
			reqID = rID
		} else {
			ctx = context.WithValue(ctx, constant.RequestIDHeader, reqID)
		}
	}

	fields := logrus.Fields{
		"source":     fmt.Sprintf("%s:%d", file, line),
		"request_id": reqID,
	}

	return logrus.WithContext(ctx).WithFields(fields)
}

func WithFields(fields Fields) *logrus.Entry {
	pc, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}

	fields["source"] = fmt.Sprintf("%s:%d", file, line)

	if fnTrace {
		funcname := runtime.FuncForPC(pc).Name()
		fn := funcname[strings.LastIndex(funcname, ".")+1:]
		fields["function"] = fn
	}

	logrusFields := logrus.Fields{}

	for key, value := range fields {
		logrusFields[key] = value
	}

	return logrus.WithFields(logrusFields)
}

func WithErrorContext(ctx context.Context, err error) *logrus.Entry {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		file = file[slash+1:]
	}

	reqID := uuid.NewV4().String()

	if ctx != nil {
		if rID, ok := ctx.Value(constant.RequestIDHeader).(string); ok {
			reqID = rID
		}
	}

	fields := logrus.Fields{
		"source":     fmt.Sprintf("%s:%d", file, line),
		"error":      err,
		"request_id": reqID,
	}

	return logrus.WithFields(fields)
}

func WithStack(err error) *logrus.Entry {
	stack := MarshalStack(errors.WithStack(err))
	return logrus.WithField("stack", stack)
}

func WithRequest(ctx context.Context, req *http.Request) context.Context {
	var requestID = func(r *http.Request) string {
		return r.Header.Get(constant.RequestIDHeader.String())
	}

	newReqID := uuid.NewV4().String()

	if reqID := requestID(req); reqID != "" {
		newReqID = reqID
	}

	ctx = context.WithValue(ctx, constant.RequestIDHeader, newReqID)

	return ctx
}
