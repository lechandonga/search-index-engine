package searchengine

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// manifest 是清单文件的内存表示。
// 它是唯一的“真相来源”，描述当前生效的段集合与待回放 WAL。
type manifest struct {
	Version   uint32
	Segments  []segMeta
	ActiveWAL string
	WALs      []string // 已冻结、可能尚未被段完全覆盖的 WAL（按新旧有序）
	NextSegID uint64
}

const (
	manifestHeaderMagic = "SIMAN001"
	manifestFooterMagic = "SIMEND01"
)

func manifestPath(dir string) string { return filepath.Join(dir, manifestName) }

// readManifest 读取并校验清单：
//   - 文件不存在 -> CodeMissingFile（调用方据此创建新索引）；
//   - 版本高于当前代码 -> CodeIncompatibleVersion；
//   - 截断/魔数错/CRC 错 -> CodeCorruptData。
func readManifest(dir string) (*manifest, error) {
	path := manifestPath(dir)
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, errf(CodeMissingFile, "read manifest", path, "no manifest yet")
		}
		return nil, errf(CodeCorruptData, "read manifest", path, "%v", err)
	}
	if len(raw) < 20 || string(raw[:8]) != manifestHeaderMagic ||
		string(raw[len(raw)-12:len(raw)-4]) != manifestFooterMagic {
		return nil, errf(CodeCorruptData, "read manifest", path, "bad magic or truncated")
	}
	ver := binary.BigEndian.Uint32(raw[8:12])
	if ver > formatVersion {
		return nil, errf(CodeIncompatibleVersion, "read manifest", path,
			"manifest version %d > supported version %d", ver, formatVersion)
	}
	body := raw[12 : len(raw)-12]
	if len(body) < 4 {
		return nil, errf(CodeCorruptData, "read manifest", path, "missing body crc")
	}
	wantCRC := binary.BigEndian.Uint32(body[len(body)-4:])
	if crc32.ChecksumIEEE(body[:len(body)-4]) != wantCRC {
		return nil, errf(CodeCorruptData, "read manifest", path, "body crc mismatch")
	}
	r := &segReader{b: body[:len(body)-4]}
	uvar := func(what string) uint64 {
		if err != nil {
			return 0
		}
		var v uint64
		v, err = r.uvar()
		if err != nil {
			err = errf(CodeCorruptData, "read manifest", path, "%s: %v", what, err)
		}
		return v
	}
	sval := func(what string) string {
		if err != nil {
			return ""
		}
		var v string
		v, err = r.s()
		if err != nil {
			err = errf(CodeCorruptData, "read manifest", path, "%s: %v", what, err)
		}
		return v
	}

	m := &manifest{Version: ver}
	m.NextSegID = uvar("next segment id")
	m.ActiveWAL = sval("active wal")
	walCount := int(uvar("wal count"))
	m.WALs = make([]string, 0, walCount)
	for i := 0; i < walCount; i++ {
		m.WALs = append(m.WALs, sval("wal name"))
	}
	segCount := int(uvar("segment count"))
	m.Segments = make([]segMeta, 0, segCount)
	for i := 0; i < segCount; i++ {
		var sm segMeta
		sm.ID = uvar("segment id")
		sm.Name = sval("segment name")
		sm.Docs = int(uvar("segment docs"))
		sm.HiddenDocs = int(uvar("segment hidden docs"))
		sm.HiddenLen = int64(uvar("segment hidden len"))
		m.Segments = append(m.Segments, sm)
	}
	if err != nil {
		return nil, err
	}
	if r.off != len(r.b) {
		return nil, errf(CodeCorruptData, "read manifest", path, "%d trailing bytes", len(r.b)-r.off)
	}
	return m, nil
}

// writeManifest 原子写入清单：临时文件 + fsync + rename + fsync(目录)。
// 写完 rename 前的崩溃只会留下临时文件（下次启动忽略），绝不出现半份清单。
func writeManifest(dir string, m *manifest) error {
	path := manifestPath(dir)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return errf(CodeCorruptData, "write manifest", tmp, "%v", err)
	}
	abort := func(cause error) error {
		f.Close()
		os.Remove(tmp)
		return cause
	}

	var b []byte
	putU := func(v uint64) {
		var tmp [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(tmp[:], v)
		b = append(b, tmp[:n]...)
	}
	putS := func(v string) {
		putU(uint64(len(v)))
		b = append(b, v...)
	}

	putU(m.NextSegID)
	putS(m.ActiveWAL)
	putU(uint64(len(m.WALs)))
	for _, w := range m.WALs {
		putS(w)
	}
	putU(uint64(len(m.Segments)))
	for _, s := range m.Segments {
		putU(s.ID)
		putS(s.Name)
		putU(uint64(s.Docs))
		putU(uint64(s.HiddenDocs))
		putU(uint64(s.HiddenLen))
	}

	// header
	hdr := make([]byte, 12)
	copy(hdr[:8], manifestHeaderMagic)
	binary.BigEndian.PutUint32(hdr[8:], formatVersion)
	// body crc
	var crcb [4]byte
	binary.BigEndian.PutUint32(crcb[:], crc32.ChecksumIEEE(b))
	// footer
	footer := make([]byte, 12)
	copy(footer[:8], manifestFooterMagic)
	binary.BigEndian.PutUint32(footer[8:], formatVersion)

	if _, err := f.Write(hdr); err != nil {
		return abort(errf(CodeCorruptData, "write manifest", tmp, "%v", err))
	}
	if _, err := f.Write(b); err != nil {
		return abort(errf(CodeCorruptData, "write manifest", tmp, "%v", err))
	}
	if _, err := f.Write(crcb[:]); err != nil {
		return abort(errf(CodeCorruptData, "write manifest", tmp, "%v", err))
	}
	if _, err := f.Write(footer); err != nil {
		return abort(errf(CodeCorruptData, "write manifest", tmp, "%v", err))
	}
	if err := f.Sync(); err != nil {
		return abort(errf(CodeCorruptData, "sync manifest", tmp, "%v", err))
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return errf(CodeCorruptData, "rename manifest", path, "%v", err)
	}
	return fsyncDir(dir)
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return errf(CodeCorruptData, "fsync dir", dir, "%v", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return errf(CodeCorruptData, "fsync dir", dir, "%v", err)
	}
	return nil
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

var _ = io.ErrUnexpectedEOF
