package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// This file holds Orion's on-the-wire and on-disk encodings for Raft entries.
// They are hand-rolled rather than protobuf for two reasons: the raft package
// stays dependency-free, and the format is byte-for-byte deterministic, which
// matters because the WAL is checksummed and replayed after a crash.
//
// All integers are little-endian, all variable-length fields are length-
// prefixed with a uint32. Every encoder writes a fixed field order.

func encodeConfChange(cc ConfChange) []byte {
	buf := make([]byte, 0, 9+len(cc.Context))
	buf = append(buf, byte(cc.Type))
	buf = binary.LittleEndian.AppendUint64(buf, cc.NodeID)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(cc.Context)))
	buf = append(buf, cc.Context...)
	return buf
}

// DecodeConfChange parses the payload of an EntryConfChange entry.
func DecodeConfChange(data []byte) (ConfChange, error) {
	var cc ConfChange
	if len(data) < 13 {
		return cc, fmt.Errorf("raft: conf change payload too short (%d bytes)", len(data))
	}
	cc.Type = ConfChangeType(data[0])
	cc.NodeID = binary.LittleEndian.Uint64(data[1:9])
	n := binary.LittleEndian.Uint32(data[9:13])
	if uint64(len(data)) < 13+uint64(n) {
		return cc, fmt.Errorf("raft: conf change context truncated")
	}
	cc.Context = append([]byte(nil), data[13:13+n]...)
	return cc, nil
}

func appendEntry(buf []byte, e Entry) []byte {
	buf = binary.LittleEndian.AppendUint64(buf, e.Term)
	buf = binary.LittleEndian.AppendUint64(buf, e.Index)
	buf = append(buf, byte(e.Type))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.Data)))
	return append(buf, e.Data...)
}

func decodeEntry(b []byte) (Entry, int, error) {
	var e Entry
	if len(b) < 21 {
		return e, 0, io.ErrUnexpectedEOF
	}
	e.Term = binary.LittleEndian.Uint64(b[0:8])
	e.Index = binary.LittleEndian.Uint64(b[8:16])
	e.Type = EntryType(b[16])
	n := int(binary.LittleEndian.Uint32(b[17:21]))
	if len(b) < 21+n {
		return e, 0, io.ErrUnexpectedEOF
	}
	if n > 0 {
		e.Data = append([]byte(nil), b[21:21+n]...)
	}
	return e, 21 + n, nil
}

func encodeConfiguration(c Configuration) []byte {
	buf := binary.LittleEndian.AppendUint32(nil, uint32(len(c.Voters)))
	for _, v := range c.Voters {
		buf = binary.LittleEndian.AppendUint64(buf, v)
	}
	return buf
}

func decodeConfiguration(b []byte) (Configuration, int, error) {
	if len(b) < 4 {
		return Configuration{}, 0, io.ErrUnexpectedEOF
	}
	n := int(binary.LittleEndian.Uint32(b[0:4]))
	if len(b) < 4+8*n {
		return Configuration{}, 0, io.ErrUnexpectedEOF
	}
	c := Configuration{}
	if n > 0 {
		c.Voters = make([]uint64, n)
		for i := 0; i < n; i++ {
			c.Voters[i] = binary.LittleEndian.Uint64(b[4+8*i : 12+8*i])
		}
	}
	return c, 4 + 8*n, nil
}

func encodeSnapshotMeta(s Snapshot) []byte {
	buf := binary.LittleEndian.AppendUint64(nil, s.Index)
	buf = binary.LittleEndian.AppendUint64(buf, s.Term)
	return append(buf, encodeConfiguration(s.Config)...)
}

func decodeSnapshotMeta(b []byte) (Snapshot, int, error) {
	if len(b) < 16 {
		return Snapshot{}, 0, io.ErrUnexpectedEOF
	}
	s := Snapshot{
		Index: binary.LittleEndian.Uint64(b[0:8]),
		Term:  binary.LittleEndian.Uint64(b[8:16]),
	}
	conf, n, err := decodeConfiguration(b[16:])
	if err != nil {
		return s, 0, err
	}
	s.Config = conf
	return s, 16 + n, nil
}

// castagnoli is used for record checksums. It is the same polynomial used by
// most storage systems and is hardware accelerated on amd64 and arm64.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func checksum(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// EncodeMessage serializes a Raft RPC for the wire. Field order is fixed and
// every variable-length section is length-prefixed, so a decoder never has to
// guess and a truncated frame fails cleanly.
func EncodeMessage(m Message) []byte {
	buf := make([]byte, 0, 128+len(m.Entries)*64)
	buf = append(buf, byte(m.Type))
	buf = binary.LittleEndian.AppendUint64(buf, m.From)
	buf = binary.LittleEndian.AppendUint64(buf, m.To)
	buf = binary.LittleEndian.AppendUint64(buf, m.Term)
	buf = binary.LittleEndian.AppendUint64(buf, m.PrevLogIndex)
	buf = binary.LittleEndian.AppendUint64(buf, m.PrevLogTerm)
	buf = binary.LittleEndian.AppendUint64(buf, m.LeaderCommit)
	buf = binary.LittleEndian.AppendUint64(buf, m.LastLogIndex)
	buf = binary.LittleEndian.AppendUint64(buf, m.LastLogTerm)
	buf = binary.LittleEndian.AppendUint64(buf, m.MatchIndex)
	buf = binary.LittleEndian.AppendUint64(buf, m.RejectHintIndex)
	buf = binary.LittleEndian.AppendUint64(buf, m.RejectHintTerm)
	buf = binary.LittleEndian.AppendUint64(buf, m.ReadIndex)
	var flags byte
	if m.Reject {
		flags |= 1
	}
	if m.Snapshot != nil {
		flags |= 2
	}
	buf = append(buf, flags)

	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(m.ReadCtx)))
	buf = append(buf, m.ReadCtx...)

	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(m.Entries)))
	for _, e := range m.Entries {
		buf = appendEntry(buf, e)
	}

	if m.Snapshot != nil {
		meta := encodeSnapshotMeta(*m.Snapshot)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(meta)))
		buf = append(buf, meta...)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(m.Snapshot.Data)))
		buf = append(buf, m.Snapshot.Data...)
	}
	return buf
}

// DecodeMessage parses a wire frame. Every length is validated against the
// remaining buffer: a hostile or corrupt frame must not be able to make the
// decoder allocate or read out of bounds.
func DecodeMessage(b []byte) (Message, error) {
	var m Message
	const fixed = 1 + 12*8 + 1
	if len(b) < fixed+8 {
		return m, io.ErrUnexpectedEOF
	}
	m.Type = MessageType(b[0])
	p := b[1:]
	read := func() uint64 {
		v := binary.LittleEndian.Uint64(p[:8])
		p = p[8:]
		return v
	}
	m.From, m.To, m.Term = read(), read(), read()
	m.PrevLogIndex, m.PrevLogTerm, m.LeaderCommit = read(), read(), read()
	m.LastLogIndex, m.LastLogTerm = read(), read()
	m.MatchIndex = read()
	m.RejectHintIndex, m.RejectHintTerm = read(), read()
	m.ReadIndex = read()

	flags := p[0]
	p = p[1:]
	m.Reject = flags&1 != 0
	hasSnapshot := flags&2 != 0

	if len(p) < 4 {
		return m, io.ErrUnexpectedEOF
	}
	n := int(binary.LittleEndian.Uint32(p[:4]))
	p = p[4:]
	if n < 0 || n > len(p) {
		return m, io.ErrUnexpectedEOF
	}
	if n > 0 {
		m.ReadCtx = append([]byte(nil), p[:n]...)
		p = p[n:]
	}

	if len(p) < 4 {
		return m, io.ErrUnexpectedEOF
	}
	count := int(binary.LittleEndian.Uint32(p[:4]))
	p = p[4:]
	// An entry costs at least 21 bytes on the wire, so a count larger than the
	// remaining buffer can only be corruption.
	if count < 0 || count > len(p)/21+1 {
		return m, fmt.Errorf("raft: entry count %d exceeds frame size", count)
	}
	if count > 0 {
		m.Entries = make([]Entry, 0, count)
		for i := 0; i < count; i++ {
			e, n, err := decodeEntry(p)
			if err != nil {
				return m, err
			}
			m.Entries = append(m.Entries, e)
			p = p[n:]
		}
	}

	if hasSnapshot {
		if len(p) < 4 {
			return m, io.ErrUnexpectedEOF
		}
		metaLen := int(binary.LittleEndian.Uint32(p[:4]))
		p = p[4:]
		if metaLen < 0 || metaLen > len(p) {
			return m, io.ErrUnexpectedEOF
		}
		s, _, err := decodeSnapshotMeta(p[:metaLen])
		if err != nil {
			return m, err
		}
		p = p[metaLen:]
		if len(p) < 4 {
			return m, io.ErrUnexpectedEOF
		}
		dataLen := int(binary.LittleEndian.Uint32(p[:4]))
		p = p[4:]
		if dataLen < 0 || dataLen > len(p) {
			return m, io.ErrUnexpectedEOF
		}
		s.Data = append([]byte(nil), p[:dataLen]...)
		m.Snapshot = &s
	}
	return m, nil
}
