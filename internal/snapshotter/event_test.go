// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapshotter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestEvent_MarshalRoundTrip(t *testing.T) {
	original := Event{
		Type:     EventTypePut,
		Key:      []byte("/registry/pods/default/nginx"),
		Value:    []byte(`{"apiVersion":"v1","kind":"Pod"}`),
		Revision: 42,
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded Event
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.Type != original.Type {
		t.Fatalf("Type: got %q, want %q", decoded.Type, original.Type)
	}
	if !bytes.Equal(decoded.Key, original.Key) {
		t.Fatalf("Key: got %q, want %q", decoded.Key, original.Key)
	}
	if !bytes.Equal(decoded.Value, original.Value) {
		t.Fatalf("Value: got %q, want %q", decoded.Value, original.Value)
	}
	if decoded.Revision != original.Revision {
		t.Fatalf("Revision: got %d, want %d", decoded.Revision, original.Revision)
	}
}

func TestEvent_MarshalDelete(t *testing.T) {
	ev := Event{
		Type:     EventTypeDelete,
		Key:      []byte("/registry/pods/default/nginx"),
		Value:    nil,
		Revision: 99,
	}

	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Value should be omitted from JSON for DELETE events (omitempty).
	if strings.Contains(string(data), `"value"`) {
		t.Fatalf("expected value field to be omitted for DELETE, got: %s", data)
	}

	var decoded Event
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.Type != EventTypeDelete {
		t.Fatalf("Type: got %q, want %q", decoded.Type, EventTypeDelete)
	}
	if decoded.Value != nil {
		t.Fatalf("Value: got %q, want nil", decoded.Value)
	}
	if decoded.Revision != 99 {
		t.Fatalf("Revision: got %d, want 99", decoded.Revision)
	}
}

func TestEvents_NDJSONRoundTrip(t *testing.T) {
	const count = 100
	events := make([]Event, count)
	for i := range count {
		typ := EventTypePut
		var val []byte
		if i%5 == 0 {
			typ = EventTypeDelete
		} else {
			val = []byte(fmt.Sprintf("value-%d", i))
		}
		events[i] = Event{
			Type:     typ,
			Key:      []byte(fmt.Sprintf("/key/%d", i)),
			Value:    val,
			Revision: int64(1000 + i),
		}
	}

	var buf bytes.Buffer
	if err := WriteEvents(&buf, events); err != nil {
		t.Fatalf("WriteEvents: %v", err)
	}

	// Verify NDJSON format: one line per event.
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != count {
		t.Fatalf("expected %d NDJSON lines, got %d", count, len(lines))
	}

	decoded, err := ReadEvents(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}

	if len(decoded) != count {
		t.Fatalf("expected %d events, got %d", count, len(decoded))
	}

	for i, got := range decoded {
		want := events[i]
		if got.Type != want.Type {
			t.Fatalf("event %d Type: got %q, want %q", i, got.Type, want.Type)
		}
		if !bytes.Equal(got.Key, want.Key) {
			t.Fatalf("event %d Key: got %q, want %q", i, got.Key, want.Key)
		}
		if !bytes.Equal(got.Value, want.Value) {
			t.Fatalf("event %d Value: got %q, want %q", i, got.Value, want.Value)
		}
		if got.Revision != want.Revision {
			t.Fatalf("event %d Revision: got %d, want %d", i, got.Revision, want.Revision)
		}
	}
}

func TestReadEvents_EmptyReader(t *testing.T) {
	events, err := ReadEvents(strings.NewReader(""))
	if err != nil {
		t.Fatalf("ReadEvents on empty reader: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events from empty reader, got %d", len(events))
	}
}
