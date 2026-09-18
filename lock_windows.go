//go:build windows

package searchengine

import "os"

func tryLockFile(f *os.File) error {
	// Windows 下依赖独占打开语义的最简实现。
	return errf(CodeInvalidArgument, "acquire lock", f.Name(), "locking not supported on this platform")
}

func unlockFile(f *os.File) {}
