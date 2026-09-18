//go:build !windows

package searchengine

import (
	"os"
	"syscall"
)

func tryLockFile(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return errf(CodeInvalidArgument, "acquire lock", f.Name(),
			"index directory is already locked by another process: %v", err)
	}
	return nil
}

func unlockFile(f *os.File) {
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
