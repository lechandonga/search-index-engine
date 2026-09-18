package searchengine

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// acquireLock 获取目录级排他锁（LOCK 文件）。
// 使用普通文件 + 进程独占约定；锁文件在进程结束后可复用。
// 同一进程重复打开同一目录会返回明确错误，防止双开踩数据。
func acquireLock(dir string) (*os.File, error) {
	path := filepath.Join(dir, lockName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, errf(CodeCorruptData, "acquire lock", path, "%v", err)
	}
	if err := tryLockFile(f); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Truncate(0); err != nil {
		releaseLock(f)
		return nil, err
	}
	return f, nil
}

func releaseLock(f *os.File) error {
	if f == nil {
		return nil
	}
	unlockFile(f)
	path := f.Name()
	err1 := f.Close()
	err2 := os.Remove(path)
	if err2 != nil && !errors.Is(err2, fs.ErrNotExist) {
		return err2
	}
	return err1
}
