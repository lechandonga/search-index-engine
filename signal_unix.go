//go:build !windows

package searchengine

import (
	"os"
	"os/signal"
	"syscall"
)

func signalNotify(c chan<- os.Signal) { signal.Notify(c, syscall.SIGINT, syscall.SIGTERM) }
