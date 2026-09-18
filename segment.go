package searchengine

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

const (
	segHeaderMagic = "SISEG001"
	segFooterMagic = "SIEND001"
	segPrefix      = "seg_"
	walPrefix      = "wal_"
	manifestName   = "MANIFEST"
	lockName       = "LOCK"
)

// segMeta 记录一个不可变磁盘段的清单信息。
type segMeta struct {
	ID         uint64
	Name       string
	Docs       int
	HiddenDocs int
	HiddenLen  int64
}

// segment 是已加载的不可变磁盘段，实现 readView。
type segment struct {
	meta       segMeta
	viewSeq    uint64 // 冻结时该段包含的最大提交序号，决定视图新旧
	path       string
	fields     []string
	fieldIndex map[string]int
	docs       []storedDocView // 按 docID 升序（仅存活）
	docIndex   map[string]int
	tombstones map[string]uint64 // 已删 docID -> seq
	postings   map[string][]segPosting
	hiddenDocs int
	hiddenLen  int64
	liveLen    int64
	fieldLen   map[string]int64
	fieldN     map[string]int
}

// segPosting 段内单条倒排记录。
type segPosting struct {
	DocOrd int
	Field  int
	TF     int
	Pos    []int
}

func (s *segment) ID() uint64 { return s.viewSeq }

func (s *segment) Lookup(docID string) (docState, bool) {
	if ord, ok := s.docIndex[docID]; ok {
		return docState{Seq: s.docs[ord].Seq, Deleted: false}, true
	}
	if seq, ok := s.tombstones[docID]; ok {
		return docState{Seq: seq, Deleted: true}, true
	}
	return docState{}, false
}

func (s *segment) AllDocIDs() []string {
	out := make([]string, 0, len(s.docs)+len(s.tombstones))
	for _, d := range s.docs {
		out = append(out, d.ID)
	}
	for id := range s.tombstones {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func (s *segment) Postings(term string) []posting {
	sps := s.postings[term]
	if len(sps) == 0 {
		return nil
	}
	out := make([]posting, 0, len(sps))
	for _, sp := range sps {
		out = append(out, posting{
			DocID: s.docs[sp.DocOrd].ID,
			Field: s.fields[sp.Field],
			TF:    sp.TF,
			Pos:   sp.Pos,
		})
	}
	return out
}

func (s *segment) Stored(docID string) (storedDocView, bool) {
	ord, ok := s.docIndex[docID]
	if !ok {
		return storedDocView{}, false
	}
	d := s.docs[ord]
	return storedDocView{ID: d.ID, Seq: d.Seq, Stored: d.Stored, FieldLens: d.FieldLens}, true
}

func (s *segment) HiddenCounts() (int, int64) { return s.hiddenDocs, s.hiddenLen }

func (s *segment) corpusAgg() (n int, totalLen int64, fieldLen map[string]int64, fieldN map[string]int) {
	return len(s.docs), s.liveLen, s.fieldLen, s.fieldN
}

// forEachTermPostingRaw 零分配遍历段内某词项的原始倒排。
func (s *segment) forEachTermPostingRaw(term string, f func(docID, field string, tf int, pos []int)) {
	for _, sp := range s.postings[term] {
		f(s.docs[sp.DocOrd].ID, s.fields[sp.Field], sp.TF, sp.Pos)
	}
}

func (s *segment) close() error { return nil }

// segWriter 负责段内记录的确定性编码。
type segWriter struct {
	buf []byte
}

func (sw *segWriter) uvar(v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	sw.buf = append(sw.buf, tmp[:n]...)
}

func (sw *segWriter) s(v string) {
	sw.uvar(uint64(len(v)))
	sw.buf = append(sw.buf, v...)
}

func (sw *segWriter) bytes(v []byte) {
	sw.uvar(uint64(len(v)))
	sw.buf = append(sw.buf, v...)
}

// record: body(uvarint length) + crc32(4)。
func (sw *segWriter) record() []byte {
	body := sw.buf
	out := make([]byte, 0, len(body)+binary.MaxVarintLen64+4)
	var lb [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lb[:], uint64(len(body)))
	out = append(out, lb[:n]...)
	out = append(out, body...)
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(body))
	out = append(out, crc[:]...)
	sw.buf = sw.buf[:0]
	return out
}

// writeSegment 把冻结内存段原子写成段文件（临时文件 + fsync + rename + 目录 fsync）。
// 布局是全序确定的：字段、文档、词项均按字典序排列，故同一份数据产生字节一致的段。
func writeSegment(dir string, m *memData, id uint64, hiddenDocs int, hiddenLen int64) (*segment, segMeta, error) {
	name := segmentName(id)
	path := filepath.Join(dir, name)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, segMeta{}, errf(CodeCorruptData, "write segment", tmp, "%v", err)
	}
	abort := func(cause error) (*segment, segMeta, error) {
		f.Close()
		os.Remove(tmp)
		return nil, segMeta{}, cause
	}

	// header: magic(8) + version(4 BE)
	hdr := make([]byte, 12)
	copy(hdr[:8], segHeaderMagic)
	binary.BigEndian.PutUint32(hdr[8:], formatVersion)
	if _, err := f.Write(hdr); err != nil {
		return abort(errf(CodeCorruptData, "write segment header", tmp, "%v", err))
	}

	sw := &segWriter{}
	writeRec := func() error {
		if _, err := f.Write(sw.record()); err != nil {
			return errf(CodeCorruptData, "write segment record", tmp, "%v", err)
		}
		return nil
	}

	// 收集字段表（确定性：排序）与墓碑列表（按 docID 升序）。
	fieldSet := map[string]struct{}{}
	docIDs := m.AllDocIDs()
	liveDocs := make([]*memDoc, 0, len(docIDs))
	type tomb struct {
		id  string
		seq uint64
	}
	tombList := make([]tomb, 0)
	for _, idd := range docIDs {
		d := m.docs[idd]
		if d.deleted {
			tombList = append(tombList, tomb{idd, d.seq})
			continue
		}
		liveDocs = append(liveDocs, d)
		for fld := range d.fieldLens {
			fieldSet[fld] = struct{}{}
		}
	}
	fields := sortedKeys(fieldSet)
	fieldOrd := make(map[string]int, len(fields))
	for i, fld := range fields {
		fieldOrd[fld] = i
	}

	// 计算该段冻结时的最大提交序号（存活文档与墓碑一并计入）。
	var viewSeq uint64
	for _, d := range liveDocs {
		if d.seq > viewSeq {
			viewSeq = d.seq
		}
	}
	for _, tb := range tombList {
		if tb.seq > viewSeq {
			viewSeq = tb.seq
		}
	}

	// record: metadata（含墓碑数与视图序号）
	sw.uvar(id)
	sw.uvar(uint64(len(liveDocs)))
	sw.uvar(uint64(len(tombList)))
	sw.uvar(viewSeq)
	sw.uvar(uint64(hiddenDocs))
	sw.uvar(uint64(hiddenLen))
	if err := writeRec(); err != nil {
		return abort(err)
	}

	// record: field table
	sw.uvar(uint64(len(fields)))
	for _, fld := range fields {
		sw.s(fld)
	}
	if err := writeRec(); err != nil {
		return abort(err)
	}

	// record: tombstones（单条记录承载全部墓碑，按 docID 升序）
	sw.uvar(uint64(len(tombList)))
	for _, tb := range tombList {
		sw.s(tb.id)
		sw.uvar(tb.seq)
	}
	if err := writeRec(); err != nil {
		return abort(err)
	}

	// records: documents（仅存活文档）
	for _, did := range docIDs {
		d := m.docs[did]
		if d.deleted {
			continue
		}
		sw.s(did)
		sw.uvar(d.seq)
		// field lengths（只写有内容的字段，按字段序）
		sw.uvar(uint64(len(fields)))
		for _, fld := range fields {
			sw.uvar(uint64(d.fieldLens[fld]))
		}
		// stored fields（按写入顺序保留）
		sw.uvar(uint64(len(d.stored)))
		for _, sf := range d.stored {
			sw.s(sf.Name)
			sw.s(sf.Value)
		}
		if err := writeRec(); err != nil {
			return abort(err)
		}
	}

	// records: terms + postings（词项按字典序，文档按 docID 升序）
	terms := sortedKeys(m.terms)
	sw.uvar(uint64(len(terms)))
	if err := writeRec(); err != nil {
		return abort(err)
	}
	for _, term := range terms {
		byDoc := m.terms[term]
		docTerms := sortedKeys(byDoc)
		sw.s(term)
		sw.uvar(uint64(len(docTerms)))
		for _, did := range docTerms {
			tps := byDoc[did]
			sw.s(did)
			sw.uvar(uint64(len(tps)))
			for _, tp := range tps {
				sw.uvar(uint64(fieldOrd[tp.field]))
				sw.uvar(uint64(len(tp.pos)))
				for _, p := range tp.pos {
					sw.uvar(uint64(p))
				}
			}
		}
		if err := writeRec(); err != nil {
			return abort(err)
		}
	}

	// footer: magic(8) + bodyOffset 此处直接放固定尾部。
	footer := make([]byte, 12)
	copy(footer[:8], segFooterMagic)
	binary.BigEndian.PutUint32(footer[8:], formatVersion)
	if _, err := f.Write(footer); err != nil {
		return abort(errf(CodeCorruptData, "write segment footer", tmp, "%v", err))
	}
	if err := f.Sync(); err != nil {
		return abort(errf(CodeCorruptData, "sync segment", tmp, "%v", err))
	}
	if err := f.Close(); err != nil {
		return nil, segMeta{}, err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return nil, segMeta{}, errf(CodeCorruptData, "rename segment", path, "%v", err)
	}
	if err := fsyncDir(dir); err != nil {
		return nil, segMeta{}, err
	}

	meta := segMeta{ID: id, Name: name, Docs: len(liveDocs), HiddenDocs: hiddenDocs, HiddenLen: hiddenLen}
	seg, err := openSegment(dir, meta, false, nil)
	if err != nil {
		return nil, segMeta{}, err
	}
	return seg, meta, nil
}

func segmentName(id uint64) string {
	return filepath.Join(segPrefix + u64Hex(id) + ".seg")
}

func u64Hex(v uint64) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 16)
	for i := 15; i >= 0; i-- {
		out[i] = digits[v&0xf]
		v >>= 4
	}
	return string(out)
}

// ---- 读取 ----

type segReader struct {
	b   []byte
	off int
}

func (r *segReader) uvar() (uint64, error) {
	v, n := binary.Uvarint(r.b[r.off:])
	if n <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	r.off += n
	return v, nil
}

func (r *segReader) s() (string, error) {
	n, err := r.uvar()
	if err != nil {
		return "", err
	}
	if r.off+int(n) > len(r.b) {
		return "", io.ErrUnexpectedEOF
	}
	v := string(r.b[r.off : r.off+int(n)])
	r.off += int(n)
	return v, nil
}

// readRecord 返回一条记录的 body 切片；校验 CRC。
func readRecordAt(b []byte, off int) (body []byte, next int, err error) {
	r := segReader{b: b, off: off}
	n, e := r.uvar()
	if e != nil {
		return nil, off, e
	}
	bodyStart := r.off
	bodyEnd := bodyStart + int(n)
	if bodyEnd+4 > len(b) {
		return nil, off, io.ErrUnexpectedEOF
	}
	body = b[bodyStart:bodyEnd]
	want := binary.BigEndian.Uint32(b[bodyEnd : bodyEnd+4])
	if crc32.ChecksumIEEE(body) != want {
		return nil, off, errf(CodeCorruptData, "read segment", "", "record crc mismatch at offset %d", off)
	}
	return body, bodyEnd + 4, nil
}

// openSegment 读取并校验段文件：
//   - 文件不存在 -> CodeMissingFile；
//   - 头/尾魔数不符或长度不足 -> CodeCorruptData（截断）；
//   - 版本高于当前 -> CodeIncompatibleVersion；
//   - 任一记录 CRC 不符 -> CodeCorruptData；
//
// repair=true 时调用方负责隔离（段内不做部分恢复，段是最小完整性单位）。
func openSegment(dir string, meta segMeta, repair bool, notice func(error)) (*segment, error) {
	path := filepath.Join(dir, meta.Name)
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, errf(CodeMissingFile, "open segment", path, "%v", err)
		}
		return nil, errf(CodeCorruptData, "open segment", path, "%v", err)
	}
	if len(raw) < 24 {
		return nil, errf(CodeCorruptData, "open segment", path, "segment too short (%d bytes): truncated", len(raw))
	}
	if string(raw[:8]) != segHeaderMagic {
		return nil, errf(CodeCorruptData, "open segment", path, "bad header magic")
	}
	if string(raw[len(raw)-12:len(raw)-4]) != segFooterMagic {
		return nil, errf(CodeCorruptData, "open segment", path, "bad footer magic: truncated or not a segment")
	}
	hdrVer := binary.BigEndian.Uint32(raw[8:12])
	if hdrVer > formatVersion {
		return nil, errf(CodeIncompatibleVersion, "open segment", path,
			"segment version %d > supported version %d", hdrVer, formatVersion)
	}
	footerVer := binary.BigEndian.Uint32(raw[len(raw)-4:])
	if footerVer != hdrVer {
		return nil, errf(CodeCorruptData, "open segment", path, "header/footer version mismatch")
	}

	body := raw[12 : len(raw)-12]
	off := 0
	rec := func() ([]byte, error) {
		b, next, e := readRecordAt(body, off)
		if e != nil {
			return nil, e
		}
		off = next
		return b, nil
	}

	mb, err := rec()
	if err != nil {
		return nil, segErr(path, "metadata record", err)
	}
	mr := segReader{b: mb}
	segID, _ := mr.uvar()
	docCount, _ := mr.uvar()
	tombCount, _ := mr.uvar()
	viewSeq, _ := mr.uvar()
	hiddenDocs, _ := mr.uvar()
	hiddenLen, _ := mr.uvar()
	if segID != meta.ID {
		return nil, errf(CodeCorruptData, "open segment", path, "segment id mismatch: manifest=%d file=%d", meta.ID, segID)
	}

	fb, err := rec()
	if err != nil {
		return nil, segErr(path, "field record", err)
	}
	fr := segReader{b: fb}
	fc, err := fr.uvar()
	if err != nil {
		return nil, segErr(path, "field record", err)
	}
	fields := make([]string, fc)
	fieldIndex := make(map[string]int, fc)
	for i := range fields {
		fld, err := fr.s()
		if err != nil {
			return nil, segErr(path, "field record", err)
		}
		fields[i] = fld
		fieldIndex[fld] = i
	}

	// tombstones record
	tbb, err := rec()
	if err != nil {
		return nil, segErr(path, "tombstone record", err)
	}
	tbr := segReader{b: tbb}
	tcnt, err := tbr.uvar()
	if err != nil || tcnt != tombCount {
		return nil, errf(CodeCorruptData, "open segment", path, "tombstone count mismatch")
	}
	tombstones := make(map[string]uint64, tcnt)
	for i := uint64(0); i < tcnt; i++ {
		id, err := tbr.s()
		if err != nil {
			return nil, segErr(path, "tombstone record", err)
		}
		seq, err := tbr.uvar()
		if err != nil {
			return nil, segErr(path, "tombstone record", err)
		}
		if _, dup := tombstones[id]; dup {
			return nil, errf(CodeCorruptData, "open segment", path, "duplicate tombstone %q", id)
		}
		tombstones[id] = seq
	}

	docs := make([]storedDocView, 0, docCount)
	docIndex := make(map[string]int, docCount)
	for i := uint64(0); i < docCount; i++ {
		db, err := rec()
		if err != nil {
			return nil, segErr(path, "document record", err)
		}
		dr := segReader{b: db}
		did, err := dr.s()
		if err != nil {
			return nil, segErr(path, "document record", err)
		}
		seq, err := dr.uvar()
		if err != nil {
			return nil, segErr(path, "document record", err)
		}
		flCount, err := dr.uvar()
		if err != nil || int(flCount) != len(fields) {
			return nil, errf(CodeCorruptData, "open segment", path, "field length table size mismatch")
		}
		fieldLens := make(map[string]int, len(fields))
		for _, fld := range fields {
			l, err := dr.uvar()
			if err != nil {
				return nil, segErr(path, "document record", err)
			}
			fieldLens[fld] = int(l)
		}
		sfCount, err := dr.uvar()
		if err != nil {
			return nil, segErr(path, "document record", err)
		}
		stored := make([]StoredField, sfCount)
		for j := range stored {
			name, err := dr.s()
			if err != nil {
				return nil, segErr(path, "document record", err)
			}
			value, err := dr.s()
			if err != nil {
				return nil, segErr(path, "document record", err)
			}
			stored[j] = StoredField{Name: name, Value: value}
		}
		if _, dup := docIndex[did]; dup {
			return nil, errf(CodeCorruptData, "open segment", path, "duplicate document id %q", did)
		}
		docIndex[did] = len(docs)
		docs = append(docs, storedDocView{ID: did, Seq: seq, Stored: stored, FieldLens: fieldLens})
	}

	tb, err := rec()
	if err != nil {
		return nil, segErr(path, "term count record", err)
	}
	tr := segReader{b: tb}
	termCount, err := tr.uvar()
	if err != nil {
		return nil, segErr(path, "term count record", err)
	}
	postings := make(map[string][]segPosting, termCount)
	for i := uint64(0); i < termCount; i++ {
		rb, err := rec()
		if err != nil {
			return nil, segErr(path, "posting record", err)
		}
		rr := segReader{b: rb}
		term, err := rr.s()
		if err != nil {
			return nil, segErr(path, "posting record", err)
		}
		dcnt, err := rr.uvar()
		if err != nil {
			return nil, segErr(path, "posting record", err)
		}
		var list []segPosting
		for j := uint64(0); j < dcnt; j++ {
			did, err := rr.s()
			if err != nil {
				return nil, segErr(path, "posting record", err)
			}
			ord, ok := docIndex[did]
			if !ok {
				return nil, errf(CodeCorruptData, "open segment", path, "posting references unknown doc %q", did)
			}
			fpc, err := rr.uvar()
			if err != nil {
				return nil, segErr(path, "posting record", err)
			}
			for k := uint64(0); k < fpc; k++ {
				fo, err := rr.uvar()
				if err != nil {
					return nil, segErr(path, "posting record", err)
				}
				pc, err := rr.uvar()
				if err != nil {
					return nil, segErr(path, "posting record", err)
				}
				pos := make([]int, pc)
				for p := range pos {
					pv, err := rr.uvar()
					if err != nil {
						return nil, segErr(path, "posting record", err)
					}
					pos[p] = int(pv)
				}
				if int(fo) >= len(fields) {
					return nil, errf(CodeCorruptData, "open segment", path, "posting field ordinal out of range")
				}
				list = append(list, segPosting{DocOrd: ord, Field: int(fo), TF: len(pos), Pos: pos})
			}
		}
		postings[term] = list
	}
	if off != len(body) {
		return nil, errf(CodeCorruptData, "open segment", path, "trailing %d garbage bytes before footer", len(body)-off)
	}

	// 段级语料聚合（读一次）。
	var liveLen int64
	fl := make(map[string]int64, len(fields))
	fn := make(map[string]int, len(fields))
	for _, d := range docs {
		for f, l := range d.FieldLens {
			liveLen += int64(l)
			fl[f] += int64(l)
			if l > 0 {
				fn[f]++
			}
		}
	}

	s := &segment{
		meta:       meta,
		viewSeq:    viewSeq,
		path:       path,
		fields:     fields,
		fieldIndex: fieldIndex,
		docs:       docs,
		docIndex:   docIndex,
		tombstones: tombstones,
		postings:   postings,
		hiddenDocs: int(hiddenDocs),
		hiddenLen:  int64(hiddenLen),
	}
	if meta.Docs != 0 && meta.Docs != len(docs) {
		return nil, errf(CodeCorruptData, "open segment", path, "doc count mismatch: manifest=%d file=%d", meta.Docs, len(docs))
	}
	return s, nil
}

func segErr(path, where string, err error) error {
	var ie *IndexError
	if errors.As(err, &ie) {
		return err
	}
	return errf(CodeCorruptData, "open segment", path, "%s: %v", where, err)
}
