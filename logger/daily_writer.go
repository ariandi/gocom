package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type DailyFileWriter struct {
	basePath string
	file     *os.File
	curDate  string
	mu       sync.Mutex
}

func NewDailyFileWriter(basePath string) (*DailyFileWriter, error) {
	w := &DailyFileWriter{
		basePath: basePath,
	}
	err := w.rotate()
	return w, err
}

func (w *DailyFileWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now().Format("2006-01-02")
	if now != w.curDate {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}
	return w.file.Write(p)
}

func (w *DailyFileWriter) rotate() error {
	if w.file != nil {
		_ = w.file.Close()
	}

	now := time.Now().Format("2006-01-02")
	filename := fmt.Sprintf("%s-%s.log", w.basePath, now)

	err := os.MkdirAll(filepath.Dir(filename), 0755)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	w.file = f
	w.curDate = now
	return nil
}
