package logger

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
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
	w := zapcore.AddSync(&lumberjack.Logger{
		Filename:  filepath,
		MaxSize:   1024,
		LocalTime: true,
		Compress:  true,
	})
	config := zap.NewProductionEncoderConfig()
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(config),
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
	dir,_ := filepath.Abs(filepath.Dir(os.Args[0]))
	return strings.Replace(dir, "\\", "/", -1)
}

func SetLevel(level Level){
	logLevel.SetLevel(zapcore.Level(level))
}

func Debugf(format string, v ...interface{}) {
	log.Debugf(format, v)
}

func Infof(format string, v ...interface{}) {
	log.Infof(format, v)
}
func Warnf(format string, v ...interface{}) {
	log.Warnf(format, v)
}

func Errorf(format string, v ...interface{}) error {
	log.Errorf(format, v)
	return errors.New(format)
}
