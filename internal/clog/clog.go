package clog

import (
	"fmt"
	"log/slog"
	"os"
	"time"
)

var logger *slog.Logger

func InitLogger() *slog.Logger {
	currentTime := time.Now().Format("2006-01-02_15:04:05")
	logFileName := fmt.Sprintf("%s%s", "./log/logfile_", currentTime)

	f, _ := os.Create(logFileName)
	logger = slog.New(slog.NewJSONHandler(f, nil))

	return logger
}
