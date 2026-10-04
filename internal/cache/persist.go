package cache

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/pierrec/lz4/v4"
)

// LoadStats reports statistics from a Load operation.
type LoadStats struct {
	Inserted       int
	Expired        int
	CompressedSize int64
	RawSize        int64
}

// Save writes all cache entries to path as a series of LZ4-compressed blocks.
// The file is written atomically via a temporary work file + rename.
func (c *Cache) Save(path string) error {
	start := time.Now()
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".*"+workFileSuffix)
	if err != nil {
		c.recordPersist(start, true, false)
		return fmt.Errorf("create work file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // clean up on failure

	now := time.Now()
	unixNow := now.Unix()
	monoNow := now.UnixNano()

	var buf []byte // accumulates raw entry data per shard

	for i := 0; i < c.numShards; i++ {
		shard := c.shards[i]
		shard.mu.RLock()

		// Write unix timestamp as first varint in block
		buf = appendUvarint(buf, uint64(unixNow))

		shard.m.iter(func(e *Entry) bool {
			expires := atomic.LoadInt64(&e.expireAt)
			if expires > 0 && expires < monoNow {
				return true // skip expired
			}

			var ttl int64
			if expires > 0 {
				ttl = expires - monoNow
				if ttl <= 0 {
					return true // expired
				}
			}

			buf = append(buf, 0) // kind=0 (k/v pair)
			origKey := e.Key() // decompresses if sixpacked
			buf = appendUvarint(buf, uint64(len(origKey)))
			buf = append(buf, origKey...)
			buf = appendUvarint(buf, uint64(len(e.value)))
			buf = append(buf, e.value...)
			buf = appendUvarint(buf, uint64(ttl))
			buf = appendUvarint(buf, uint64(e.flags))
			buf = appendUvarint(buf, e.cas)
			return true
		})

		shard.mu.RUnlock()

		// Flush this shard as a compressed block
		if len(buf) > 0 {
			if err := writeBlock(tmp, buf); err != nil {
				tmp.Close()
				return fmt.Errorf("write block: %w", err)
			}
			buf = buf[:0]
		}
	}

	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		c.recordPersist(start, true, false)
		return fmt.Errorf("rename: %w", err)
	}
	c.recordPersist(start, true, true)
	return nil
}

// workFileSuffix marks in-progress save files, matching pogocache's naming.
const workFileSuffix = ".pogocache.work"

// CleanWorkFiles removes work files left behind by an interrupted Save to
// path. It returns the paths of the files that were removed.
func CleanWorkFiles(path string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(path),
		globEscape(filepath.Base(path))+".*"+workFileSuffix))
	if err != nil {
		return nil, err
	}
	var removed []string
	for _, m := range matches {
		if err := os.Remove(m); err != nil {
			return removed, err
		}
		removed = append(removed, m)
	}
	return removed, nil
}

func globEscape(s string) string {
	var b []byte
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '*', '?', '[', '\\':
			b = append(b, '\\')
		}
		b = append(b, s[i])
	}
	return string(b)
}

// LoadFromFile reads cache entries from a persistence file at path.
func (c *Cache) LoadFromFile(path string) (stats *LoadStats, retErr error) {
	start := time.Now()
	defer func() {
		c.recordPersist(start, false, retErr == nil)
	}()

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	stats = &LoadStats{}
	unixNow := time.Now().Unix()

	for {
		raw, clen, err := readBlock(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return stats, fmt.Errorf("read block: %w", err)
		}
		stats.CompressedSize += int64(clen)
		stats.RawSize += int64(len(raw))

		p := raw

		// Read unix timestamp
		unixTime, n := readUvarint(p)
		if n <= 0 {
			return stats, fmt.Errorf("bad unix timestamp")
		}
		p = p[n:]

		for len(p) > 0 {
			// kind
			if len(p) < 1 {
				return stats, fmt.Errorf("truncated entry")
			}
			kind := p[0]
			p = p[1:]
			if kind != 0 {
				return stats, fmt.Errorf("unknown entry kind: %d", kind)
			}

			// key
			keyLen, n := readUvarint(p)
			if n <= 0 {
				return stats, fmt.Errorf("bad key length")
			}
			p = p[n:]
			if uint64(len(p)) < keyLen {
				return stats, fmt.Errorf("truncated key")
			}
			key := make([]byte, keyLen)
			copy(key, p[:keyLen])
			p = p[keyLen:]

			// value
			valLen, n := readUvarint(p)
			if n <= 0 {
				return stats, fmt.Errorf("bad value length")
			}
			p = p[n:]
			if uint64(len(p)) < valLen {
				return stats, fmt.Errorf("truncated value")
			}
			value := make([]byte, valLen)
			copy(value, p[:valLen])
			p = p[valLen:]

			// ttl (nanoseconds relative to save time)
			ttl, n := readUvarint(p)
			if n <= 0 {
				return stats, fmt.Errorf("bad ttl")
			}
			p = p[n:]

			// flags
			flags, n := readUvarint(p)
			if n <= 0 {
				return stats, fmt.Errorf("bad flags")
			}
			p = p[n:]

			// cas
			cas, n := readUvarint(p)
			if n <= 0 {
				return stats, fmt.Errorf("bad cas")
			}
			p = p[n:]

			if ttl > 0 {
				// Convert: the entry was saved with a TTL relative to unixTime.
				// Check if it has already expired relative to now.
				unixExpires := int64(unixTime) + int64(ttl)/int64(time.Second)
				if unixExpires < unixNow {
					stats.Expired++
					continue
				}
				// Remaining TTL in nanoseconds
				remainingNs := (unixExpires - unixNow) * int64(time.Second)
				opts := &StoreOptions{
					TTL:   time.Duration(remainingNs),
					Flags: uint32(flags),
					CAS:   cas,
				}
				c.Store(key, value, opts)
			} else {
				opts := &StoreOptions{
					Flags: uint32(flags),
					CAS:   cas,
				}
				c.Store(key, value, opts)
			}
			stats.Inserted++
		}
	}

	return stats, nil
}

// writeBlock compresses raw data and writes a POGO block to w.
func writeBlock(w io.Writer, raw []byte) error {
	// Compress
	bound := lz4.CompressBlockBound(len(raw))
	compressed := make([]byte, bound)
	n, err := lz4.CompressBlock(raw, compressed, nil)
	if err != nil {
		return err
	}
	if n == 0 {
		// lz4 returns 0 when data is incompressible; store raw
		compressed = raw
		n = len(raw)
	} else {
		compressed = compressed[:n]
	}

	// CRC32 of compressed data
	checksum := crc32.ChecksumIEEE(compressed)

	// 16-byte header: "POGO" + crc32 + decompressed_len + compressed_len
	var header [16]byte
	copy(header[:4], "POGO")
	binary.LittleEndian.PutUint32(header[4:8], checksum)
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(raw)))
	binary.LittleEndian.PutUint32(header[12:16], uint32(n))

	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.Write(compressed); err != nil {
		return err
	}
	return nil
}

// readBlock reads one POGO block from r.
// Returns decompressed data, compressed size, and error.
func readBlock(r io.Reader) ([]byte, int, error) {
	var header [16]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, 0, err
	}

	if string(header[:4]) != "POGO" {
		return nil, 0, fmt.Errorf("invalid block magic")
	}

	checksum := binary.LittleEndian.Uint32(header[4:8])
	dlen := binary.LittleEndian.Uint32(header[8:12])
	clen := binary.LittleEndian.Uint32(header[12:16])

	compressed := make([]byte, clen)
	if _, err := io.ReadFull(r, compressed); err != nil {
		return nil, 0, fmt.Errorf("short read: %w", err)
	}

	if crc32.ChecksumIEEE(compressed) != checksum {
		return nil, 0, fmt.Errorf("CRC mismatch")
	}

	decompressed := make([]byte, dlen)
	n, err := lz4.UncompressBlock(compressed, decompressed)
	if err != nil {
		return nil, 0, fmt.Errorf("decompress: %w", err)
	}
	if n != int(dlen) {
		return nil, 0, fmt.Errorf("decompress size mismatch: got %d, want %d", n, dlen)
	}

	return decompressed, int(clen), nil
}

// appendUvarint appends a varint-encoded uint64 to buf.
func appendUvarint(buf []byte, x uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], x)
	return append(buf, tmp[:n]...)
}

// readUvarint reads a varint from p. Returns the value and bytes consumed.
// Returns n<=0 on error.
func readUvarint(p []byte) (uint64, int) {
	val, n := binary.Uvarint(p)
	if n <= 0 {
		return 0, n
	}
	return val, n
}

// recordPersist records a save or load-from-file operation.
// isSave=true for Save, false for LoadFromFile.
func (c *Cache) recordPersist(start time.Time, isSave, success bool) {
	if c.metrics == nil {
		return
	}
	ms := float64(time.Since(start).Microseconds()) / 1000.0
	ctx := context.Background()
	if isSave {
		c.metrics.RecordSave(ctx, success, ms)
	} else {
		c.metrics.RecordLoadFile(ctx, success, ms)
	}
}
