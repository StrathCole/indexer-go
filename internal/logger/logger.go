package logger

import (
	"io"
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func Init(level string, filePath string) {
	// Set global level
	l, err := zerolog.ParseLevel(level)
	if err != nil {
		l = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(l)

	var writers []io.Writer

	// Console writer (pretty print)
	writers = append(writers, zerolog.ConsoleWriter{Out: os.Stdout})

	// File writer
	if filePath != "" {
		// Ensure directory exists
		dir := filepath.Dir(filePath)
		if err := os.MkdirAll(dir, 0755); err == nil {
			file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err == nil {
				writers = append(writers, file)
			} else {
				// Fallback to console if file fails, but we already have console
				// Just print error to stderr
				os.Stderr.WriteString("Failed to open log file: " + err.Error() + "\n")
			}
		} else {
			os.Stderr.WriteString("Failed to create log directory: " + err.Error() + "\n")
		}
	}

	// Multi writer
	mw := io.MultiWriter(writers...)
	log.Logger = zerolog.New(mw).With().Timestamp().Logger()
}
