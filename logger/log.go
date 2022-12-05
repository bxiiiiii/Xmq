package logger

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	// "gopkg.in/natefinch/lumberjack.v2"
)

var log *zap.SugaredLogger

var logLevel = zap.NewAtomicLevel()

type Level int8

const (
	DebugLevel Level = iota - 1
	InfoLevel
	WarnLevel
	ErrorLevel
)

func init() {
	filepath := getFilePath()
	// w := zapcore.AddSync(&lumberjack.Logger{
	// 	Filename:  filepath,
	// 	MaxSize:   1024,
	// 	LocalTime: true,
	// 	Compress:  true,
	// })
	file, _ := os.Create(filepath)
	ws := io.MultiWriter(file, os.Stdout)
	w := zapcore.AddSync(ws)
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncodeLevel = zapcore.CapitalColorLevelEncoder
	core := zapcore.NewCore(
		// zapcore.NewJSONEncoder(config),
		zapcore.NewConsoleEncoder(config),
		w,
		logLevel,
	)
	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
	log = logger.Sugar()
}

func getFilePath() string {
	logfile := getCurrentDir() + "/" + "xmq" + ".log"
	return logfile
}

func getCurrentDir() string {
	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	return strings.Replace(dir, "\\", "/", -1)
}

func SetLevel(level Level) {
	logLevel.SetLevel(zapcore.Level(level))
}

func Debugf(format string, v ...interface{}) {
	log.Debugf(format, v)
}

func Debugln(v ...interface{}) {
	log.Debugln(v)
}

func Infof(format string, v ...interface{}) {
	log.Infof(format, v)
}

func Infoln(v ...interface{}) {
	log.Infoln(v)
}

func Warnf(format string, v ...interface{}) {
	log.Warnf(format, v)
}

func Warnln(v ...interface{}) {
	log.Warnln(v)
}

func Errorf(format string, v ...interface{}) error {
	log.Errorf(format, v)
	return errors.New(fmt.Sprintf(format, v...))
}