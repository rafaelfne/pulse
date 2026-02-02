package event

import (
	"bytes"
	"testing"
)

func TestEntryEncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		entry Entry
	}{
		{
			name: "full entry",
			entry: Entry{
				Offset:    42,
				Timestamp: 1234567890,
				Key:       "test-key",
				Data:      []byte("test-data"),
			},
		},
		{
			name: "empty key",
			entry: Entry{
				Offset:    100,
				Timestamp: 9876543210,
				Key:       "",
				Data:      []byte("data-only"),
			},
		},
		{
			name: "empty data",
			entry: Entry{
				Offset:    200,
				Timestamp: 1111111111,
				Key:       "key-only",
				Data:      []byte{},
			},
		},
		{
			name: "both empty",
			entry: Entry{
				Offset:    300,
				Timestamp: 2222222222,
				Key:       "",
				Data:      []byte{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded, err := tt.entry.Encode()
			if err != nil {
				t.Fatalf("Encode() error = %v", err)
			}

			// Decode
			decoded, err := Decode(encoded)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}

			// Compare
			if decoded.Offset != tt.entry.Offset {
				t.Errorf("Offset mismatch: got %d, want %d", decoded.Offset, tt.entry.Offset)
			}
			if decoded.Timestamp != tt.entry.Timestamp {
				t.Errorf("Timestamp mismatch: got %d, want %d", decoded.Timestamp, tt.entry.Timestamp)
			}
			if decoded.Key != tt.entry.Key {
				t.Errorf("Key mismatch: got %q, want %q", decoded.Key, tt.entry.Key)
			}
			if !bytes.Equal(decoded.Data, tt.entry.Data) {
				t.Errorf("Data mismatch: got %v, want %v", decoded.Data, tt.entry.Data)
			}
		})
	}
}

func TestDecodeInvalidBuffer(t *testing.T) {
	tests := []struct {
		name string
		buf  []byte
	}{
		{"empty", []byte{}},
		{"too short", []byte{1, 2, 3}},
		{"partial header", make([]byte, 15)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Decode(tt.buf)
			if err == nil {
				t.Error("Decode() expected error for invalid buffer")
			}
		})
	}
}

func TestNewEntry(t *testing.T) {
	evt := Event{
		Key:       "test",
		Data:      []byte("data"),
		Timestamp: 12345,
	}

	entry := NewEntry(10, evt)

	if entry.Offset != 10 {
		t.Errorf("Offset = %d, want 10", entry.Offset)
	}
	if entry.Timestamp != 12345 {
		t.Errorf("Timestamp = %d, want 12345", entry.Timestamp)
	}
	if entry.Key != "test" {
		t.Errorf("Key = %q, want %q", entry.Key, "test")
	}
	if !bytes.Equal(entry.Data, []byte("data")) {
		t.Errorf("Data = %v, want %v", entry.Data, []byte("data"))
	}
}

func TestNewEntryAutoTimestamp(t *testing.T) {
	evt := Event{
		Key:       "test",
		Data:      []byte("data"),
		Timestamp: 0, // Should auto-generate
	}

	entry := NewEntry(10, evt)

	if entry.Timestamp == 0 {
		t.Error("Timestamp should be auto-generated when zero")
	}
}
