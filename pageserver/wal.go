package pageserver

import (
	"encoding/binary"
	"fmt"
)

type WALCommit struct {
	Frame, Size int64
	Pages       map[int64]int64
}
type WAL struct {
	Epoch                  any
	PageSize, Frames, Tail int64
	Commits                []WALCommit
}

func walChecksum(raw []byte, a, b uint32, order binary.ByteOrder) (uint32, uint32) {
	for k := 0; k+8 <= len(raw); k += 8 {
		a += order.Uint32(raw[k:k+4]) + b
		b += order.Uint32(raw[k+4:k+8]) + a
	}
	return a, b
}
func ParseWAL(raw []byte) WAL {
	w := WAL{Tail: int64(len(raw)), Commits: []WALCommit{}}
	if len(raw) < 32 {
		return w
	}
	magic := binary.BigEndian.Uint32(raw[:4])
	require(magic == 0x377f0682 || magic == 0x377f0683, "invalid WAL magic")
	var order binary.ByteOrder = binary.LittleEndian
	if magic == 0x377f0683 {
		order = binary.BigEndian
	}
	a, b := walChecksum(raw[:24], 0, 0, order)
	require(a == binary.BigEndian.Uint32(raw[24:28]) && b == binary.BigEndian.Uint32(raw[28:32]), "bad WAL header checksum")
	w.PageSize = int64(binary.BigEndian.Uint32(raw[8:12]))
	if w.PageSize == 1 {
		w.PageSize = 65536
	}
	require(w.PageSize >= 512 && w.PageSize <= 65536 && w.PageSize&(w.PageSize-1) == 0, "invalid WAL page size")
	s1, s2 := binary.BigEndian.Uint32(raw[16:20]), binary.BigEndian.Uint32(raw[20:24])
	w.Epoch = fmt.Sprintf("%08x%08x", s1, s2)
	pages, pending := map[int64]int64{}, map[int64]int64{}
	offset := int64(32)
	for offset+24+w.PageSize <= int64(len(raw)) {
		h := raw[offset : offset+24]
		pg, size := int64(binary.BigEndian.Uint32(h[:4])), int64(binary.BigEndian.Uint32(h[4:8]))
		ca, cb := walChecksum(h[:8], a, b, order)
		ca, cb = walChecksum(raw[offset+24:offset+24+w.PageSize], ca, cb, order)
		if pg == 0 || binary.BigEndian.Uint32(h[8:12]) != s1 || binary.BigEndian.Uint32(h[12:16]) != s2 || binary.BigEndian.Uint32(h[16:20]) != ca || binary.BigEndian.Uint32(h[20:24]) != cb {
			break
		}
		a, b = ca, cb
		w.Frames++
		pending[pg] = offset + 24
		offset += 24 + w.PageSize
		if size != 0 {
			for p, o := range pending {
				pages[p] = o
			}
			pending = map[int64]int64{}
			copyPages := map[int64]int64{}
			for p, o := range pages {
				if p <= size {
					copyPages[p] = o
				} else {
					delete(pages, p)
				}
			}
			w.Commits = append(w.Commits, WALCommit{w.Frames, size, copyPages})
		}
	}
	w.Tail = int64(len(raw)) - offset
	return w
}
func pageSize(header []byte, g Document) int64 {
	size := num(g["page_size"])
	if size == 0 {
		size = 4096
	}
	if len(header) > 0 {
		require(len(header) >= 18, "short baseline header")
		size = int64(binary.BigEndian.Uint16(header[16:18]))
		if size == 1 || size == 0 {
			size = 65536
		}
	}
	require(size >= 512 && size <= 65536 && size&(size-1) == 0, "invalid baseline page size")
	return size
}
func pageIndex(g Document, base, wal []byte) Document {
	size := pageSize(base, g)
	parsed := ParseWAL(wal)
	commits := []any{}
	for _, c := range parsed.Commits {
		if c.Frame > num(g["commit_frame"]) {
			break
		}
		pages := Document{}
		for pg, off := range c.Pages {
			pages[fmt.Sprint(pg)] = off
		}
		commits = append(commits, Document{"frame": c.Frame, "dbsize": c.Size, "pages": pages})
	}
	return Document{"generation": g, "page_size": size, "base_size": len(base), "wal_size": len(wal), "epoch": parsed.Epoch, "commits": commits}
}
