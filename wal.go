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

const (
	walFrameUpsert byte = 1
	walFrameDelete byte = 2
)

var (
	walHeaderMagic = []byte("SIWAL001")
	walFrameMagic  = []byte{'S', 'I', 'W', 'F'}
)

// walEntry 是一帧已解析的 WAL 记录。
type walEntry struct {
	Seq   uint64
	Type  byte
	Doc   Document // Type==walFrameUpsert 时有效
	DocID string   // Type==walFrameDelete 时有效
}

// walWriter 向活动 WAL 顺序追加帧；Sync 后帧在崩溃后可恢复。
type walWriter struct {
	path       string
	f          *os.File
	syncWrites bool
}

func openWALWriter(dir, name string, syncWrites bool) (*walWriter, error) {
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, errf(CodeCorruptData, "open wal", path, "%v", err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() == 0 {
		if _, err := f.Write(walHeaderMagic); err != nil {
			f.Close()
			return nil, errf(CodeCorruptData, "write wal header", path, "%v", err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
	}
	return &walWriter{path: path, f: f, syncWrites: syncWrites}, nil
}

// append 追加一帧；syncWrites=true 时随后 fsync，返回成功即持久化。
func (w *walWriter) append(e walEntry) error {
	payload, err := encodeWALPayload(e)
	if err != nil {
		return err
	}
	frame := make([]byte, 0, 4+8+1+4+len(payload)+4)
	frame = append(frame, walFrameMagic...)
	var hdr [13]byte
	binary.BigEndian.PutUint64(hdr[0:8], e.Seq)
	hdr[8] = e.Type
	binary.BigEndian.PutUint32(hdr[9:13], uint32(len(payload)))
	frame = append(frame, hdr[:]...)
	frame = append(frame, payload...)
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(append(hdr[:], payload...)))
	frame = append(frame, crc[:]...)

	if _, err := w.f.Write(frame); err != nil {
		return errf(CodeCorruptData, "write wal frame", w.path, "%v", err)
	}
	if w.syncWrites {
		return w.sync()
	}
	return nil
}

func (w *walWriter) sync() error {
	if err := w.f.Sync(); err != nil {
		return errf(CodeCorruptData, "sync wal", w.path, "%v", err)
	}
	return nil
}

func (w *walWriter) close() error { return w.f.Close() }

// remove 在文件已关闭后删除它。
func (w *walWriter) remove() error {
	if err := os.Remove(w.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func encodeWALPayload(e walEntry) ([]byte, error) {
	buf := make([]byte, 0, 64)
	putStr := func(s string) {
		var lb [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(lb[:], uint64(len(s)))
		buf = append(buf, lb[:n]...)
		buf = append(buf, s...)
	}
	switch e.Type {
	case walFrameUpsert:
		putStr(e.Doc.ID)
		var fc [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(fc[:], uint64(len(e.Doc.Fields)))
		buf = append(buf, fc[:n]...)
		for _, f := range e.Doc.Fields {
			putStr(f.Name)
			putStr(f.Value)
			var flags byte
			if f.Store {
				flags |= 1
			}
			if f.Index {
				flags |= 2
			}
			buf = append(buf, flags)
		}
	case walFrameDelete:
		putStr(e.DocID)
	default:
		return nil, errf(CodeCorruptData, "encode wal", "", "unknown frame type %d", e.Type)
	}
	return buf, nil
}

// walReplay 按顺序回放若干 WAL 文件，对每条完整帧调用 fn。
//   - 文件起始魔数不匹配：CodeCorruptData；
//   - 文件尾部因强杀产生的半个/损坏帧：截断尾部并通过 notice 上报，
//     之前完整帧照常回放；
//   - 中部损坏（在其后能重新同步到合法帧）：CodeCorruptData，不静默；
//   - 文件缺失：repair=true 时上报并跳过，否则 CodeMissingFile。
//
// 帧去重：seq 必须严格递增；重复（seq 不大于上一帧）的帧被跳过，
// 使「段已包含旧 WAL 内容而旧 WAL 仍在」的崩溃场景可幂等恢复。
func walReplay(dir string, names []string, repair bool, notice func(error),
	fn func(walEntry) error) error {
	var lastSeq uint64
	for _, name := range names {
		path := filepath.Join(dir, name)
		raw, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				if repair {
					notice(errf(CodeMissingFile, "replay wal", path, "wal listed in manifest is gone; skipping"))
					continue
				}
				return errf(CodeMissingFile, "replay wal", path, "%v", err)
			}
			return errf(CodeCorruptData, "replay wal", path, "%v", err)
		}
		if len(raw) < len(walHeaderMagic) || string(raw[:len(walHeaderMagic)]) != string(walHeaderMagic) {
			return errf(CodeCorruptData, "replay wal", path, "bad wal header magic")
		}
		off := len(walHeaderMagic)
		goodEnd := off
		for off < len(raw) {
			e, next, ok, torn := parseFrame(raw, off)
			if ok {
				if e.Seq > lastSeq {
					if err := fn(e); err != nil {
						return err
					}
					lastSeq = e.Seq
				}
				off = next
				goodEnd = next
				continue
			}
			// 当前位置无法解析。尝试在后续字节中重新同步。
			resync := findFrameMagic(raw, off+1)
			if resync >= 0 {
				if re, _, rok, _ := parseFrame(raw, resync); rok {
					return errf(CodeCorruptData, "replay wal", path,
						"corrupt frame at offset %d before valid frame at %d (seq=%d)", off, resync, re.Seq)
				}
			}
			// 无法再同步：视为尾部半帧。
			if torn {
				if err := os.Truncate(path, int64(goodEnd)); err != nil {
					return errf(CodeCorruptData, "truncate wal", path, "%v", err)
				}
				if notice != nil {
					notice(errf(CodeCorruptData, "replay wal", path,
						"discarded %d torn trailing bytes from interrupted write", len(raw)-goodEnd))
				}
			}
			break
		}
	}
	return nil
}

// parseFrame 尝试从 off 解析一帧。
// 返回 (entry, 下一偏移, 是否完整合法, 是否为可容忍的尾部破损)。
func parseFrame(raw []byte, off int) (walEntry, int, bool, bool) {
	const frameHead = 4 + 13
	if off+frameHead > len(raw) {
		return walEntry{}, off, false, true
	}
	if string(raw[off:off+4]) != string(walFrameMagic) {
		return walEntry{}, off, false, false
	}
	h := off + 4
	seq := binary.BigEndian.Uint64(raw[h : h+8])
	typ := raw[h+8]
	plen := int(binary.BigEndian.Uint32(raw[h+9 : h+13]))
	end := h + 13 + plen + 4
	if end > len(raw) {
		return walEntry{}, off, false, true // 长度声明超出文件：半帧
	}
	payload := raw[h+13 : h+13+plen]
	wantCRC := binary.BigEndian.Uint32(raw[end-4 : end])
	if crc32.ChecksumIEEE(raw[h:h+13+plen]) != wantCRC {
		return walEntry{}, off, false, false // CRC 错：内容损坏
	}
	e, err := decodeWALPayload(seq, typ, payload)
	if err != nil {
		return walEntry{}, off, false, false
	}
	return e, end, true, false
}

func findFrameMagic(raw []byte, from int) int {
	for i := from; i+4 <= len(raw); i++ {
		if raw[i] == walFrameMagic[0] && string(raw[i:i+4]) == string(walFrameMagic) {
			return i
		}
	}
	return -1
}

func decodeWALPayload(seq uint64, typ byte, b []byte) (walEntry, error) {
	r := &byteReader{b: b}
	readStr := func() (string, error) {
		n, err := binary.ReadUvarint(r)
		if err != nil {
			return "", err
		}
		if r.off+int(n) > len(r.b) {
			return "", io.ErrUnexpectedEOF
		}
		s := string(r.b[r.off : r.off+int(n)])
		r.off += int(n)
		return s, nil
	}
	switch typ {
	case walFrameUpsert:
		id, err := readStr()
		if err != nil {
			return walEntry{}, err
		}
		fc, err := binary.ReadUvarint(r)
		if err != nil {
			return walEntry{}, err
		}
		doc := Document{ID: id}
		for i := uint64(0); i < fc; i++ {
			name, err := readStr()
			if err != nil {
				return walEntry{}, err
			}
			value, err := readStr()
			if err != nil {
				return walEntry{}, err
			}
			if r.off >= len(r.b) {
				return walEntry{}, io.ErrUnexpectedEOF
			}
			flags := r.b[r.off]
			r.off++
			doc.Fields = append(doc.Fields, Field{
				Name:  name,
				Value: value,
				Store: flags&1 != 0,
				Index: flags&2 != 0,
			})
		}
		return walEntry{Seq: seq, Type: typ, Doc: doc}, nil
	case walFrameDelete:
		id, err := readStr()
		if err != nil {
			return walEntry{}, err
		}
		return walEntry{Seq: seq, Type: typ, DocID: id}, nil
	default:
		return walEntry{}, errf(CodeCorruptData, "decode wal", "", "unknown frame type %d", typ)
	}
}

type byteReader struct {
	b   []byte
	off int
}

func (r *byteReader) ReadByte() (byte, error) {
	if r.off >= len(r.b) {
		return 0, io.EOF
	}
	v := r.b[r.off]
	r.off++
	return v, nil
}
