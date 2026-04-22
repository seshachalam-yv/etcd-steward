// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapshotter

import (
	"bytes"
	"fmt"
	"testing"
)

func BenchmarkWriteEvents(b *testing.B) {
	events := make([]Event, 1000)
	for i := range events {
		events[i] = Event{
			Type:     EventTypePut,
			Key:      []byte(fmt.Sprintf("/registry/pods/default/pod-%d", i)),
			Value:    []byte(fmt.Sprintf(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"pod-%d"}}`, i)),
			Revision: int64(i + 1),
		}
	}
	b.ResetTimer()
	for range b.N {
		var buf bytes.Buffer
		if err := WriteEvents(&buf, events); err != nil {
			b.Fatalf("WriteEvents: %v", err)
		}
	}
}

func BenchmarkReadEvents(b *testing.B) {
	events := make([]Event, 1000)
	for i := range events {
		events[i] = Event{
			Type:     EventTypePut,
			Key:      []byte(fmt.Sprintf("/registry/pods/default/pod-%d", i)),
			Value:    []byte(fmt.Sprintf(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"pod-%d"}}`, i)),
			Revision: int64(i + 1),
		}
	}
	var buf bytes.Buffer
	if err := WriteEvents(&buf, events); err != nil {
		b.Fatalf("WriteEvents: %v", err)
	}
	data := buf.Bytes()

	b.ResetTimer()
	for range b.N {
		_, err := ReadEvents(bytes.NewReader(data))
		if err != nil {
			b.Fatalf("ReadEvents: %v", err)
		}
	}
}

func BenchmarkWriteEvents_SmallPayload(b *testing.B) {
	events := make([]Event, 100)
	for i := range events {
		events[i] = Event{
			Type:     EventTypePut,
			Key:      []byte(fmt.Sprintf("key-%d", i)),
			Value:    []byte("value"),
			Revision: int64(i + 1),
		}
	}
	b.ResetTimer()
	for range b.N {
		var buf bytes.Buffer
		if err := WriteEvents(&buf, events); err != nil {
			b.Fatalf("WriteEvents: %v", err)
		}
	}
}

func BenchmarkReadEvents_SmallPayload(b *testing.B) {
	events := make([]Event, 100)
	for i := range events {
		events[i] = Event{
			Type:     EventTypePut,
			Key:      []byte(fmt.Sprintf("key-%d", i)),
			Value:    []byte("value"),
			Revision: int64(i + 1),
		}
	}
	var buf bytes.Buffer
	if err := WriteEvents(&buf, events); err != nil {
		b.Fatalf("WriteEvents: %v", err)
	}
	data := buf.Bytes()

	b.ResetTimer()
	for range b.N {
		_, err := ReadEvents(bytes.NewReader(data))
		if err != nil {
			b.Fatalf("ReadEvents: %v", err)
		}
	}
}

func BenchmarkWriteEvents_MixedTypes(b *testing.B) {
	events := make([]Event, 1000)
	for i := range events {
		if i%5 == 0 {
			events[i] = Event{
				Type:     EventTypeDelete,
				Key:      []byte(fmt.Sprintf("/registry/key-%d", i)),
				Revision: int64(i + 1),
			}
		} else {
			events[i] = Event{
				Type:     EventTypePut,
				Key:      []byte(fmt.Sprintf("/registry/key-%d", i)),
				Value:    []byte(fmt.Sprintf("value-%d", i)),
				Revision: int64(i + 1),
			}
		}
	}
	b.ResetTimer()
	for range b.N {
		var buf bytes.Buffer
		if err := WriteEvents(&buf, events); err != nil {
			b.Fatalf("WriteEvents: %v", err)
		}
	}
}
