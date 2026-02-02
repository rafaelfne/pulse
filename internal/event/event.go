package event

import (
	"encoding/binary"
	"fmt"
	"time"
)

// Event represents a single event in the system.
type Event struct {
	// Key is used for partitioning (can be empty)
	Key string `json:"key"`
	// Data is the event payload
	Data []byte `json:"data"`
	// Timestamp is when the event was received (set by server)
	Timestamp int64 `json:"timestamp"`
}

// Batch represents a batch of events for ingest.
type Batch struct {
	Events []Event `json:"events"`
}

// Entry represents a persisted log entry with offset and metadata.
type Entry struct {
	Offset    int64
	Timestamp int64
	Key       string
	Data      []byte
}

// Encode serializes an Entry to bytes for storage.
// Format: [offset:8][timestamp:8][keyLen:4][key:keyLen][dataLen:4][data:dataLen]
func (e *Entry) Encode() ([]byte, error) {
	keyLen := len(e.Key)
	dataLen := len(e.Data)
	totalLen := 8 + 8 + 4 + keyLen + 4 + dataLen

	buf := make([]byte, totalLen)
	pos := 0

	// Write offset
	binary.BigEndian.PutUint64(buf[pos:], uint64(e.Offset))
	pos += 8

	// Write timestamp
	binary.BigEndian.PutUint64(buf[pos:], uint64(e.Timestamp))
	pos += 8

	// Write key length and key
	binary.BigEndian.PutUint32(buf[pos:], uint32(keyLen))
	pos += 4
	copy(buf[pos:], e.Key)
	pos += keyLen

	// Write data length and data
	binary.BigEndian.PutUint32(buf[pos:], uint32(dataLen))
	pos += 4
	copy(buf[pos:], e.Data)

	return buf, nil
}

// Decode deserializes bytes into an Entry.
func Decode(buf []byte) (*Entry, error) {
	if len(buf) < 20 { // Minimum: 8+8+4+0+4+0
		return nil, fmt.Errorf("buffer too short: %d bytes", len(buf))
	}

	entry := &Entry{}
	pos := 0

	// Read offset
	entry.Offset = int64(binary.BigEndian.Uint64(buf[pos:]))
	pos += 8

	// Read timestamp
	entry.Timestamp = int64(binary.BigEndian.Uint64(buf[pos:]))
	pos += 8

	// Read key
	keyLen := int(binary.BigEndian.Uint32(buf[pos:]))
	pos += 4
	if pos+keyLen > len(buf) {
		return nil, fmt.Errorf("invalid key length: %d", keyLen)
	}
	if keyLen > 0 {
		entry.Key = string(buf[pos : pos+keyLen])
		pos += keyLen
	}

	// Read data
	if pos+4 > len(buf) {
		return nil, fmt.Errorf("buffer too short for data length")
	}
	dataLen := int(binary.BigEndian.Uint32(buf[pos:]))
	pos += 4
	if pos+dataLen > len(buf) {
		return nil, fmt.Errorf("invalid data length: %d", dataLen)
	}
	if dataLen > 0 {
		entry.Data = make([]byte, dataLen)
		copy(entry.Data, buf[pos:pos+dataLen])
	}

	return entry, nil
}

// NewEntry creates a new Entry from an Event with the given offset.
func NewEntry(offset int64, evt Event) *Entry {
	timestamp := evt.Timestamp
	if timestamp == 0 {
		timestamp = time.Now().UnixNano()
	}
	return &Entry{
		Offset:    offset,
		Timestamp: timestamp,
		Key:       evt.Key,
		Data:      evt.Data,
	}
}
