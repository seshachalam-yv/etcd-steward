// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapshotter

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

// EventType identifies the kind of mutation an Event represents.
type EventType string

const (
	// EventTypePut indicates a key was created or updated.
	EventTypePut EventType = "PUT"
	// EventTypeDelete indicates a key was deleted.
	EventTypeDelete EventType = "DELETE"
)

// Event represents a single etcd key mutation captured by watching the etcd
// event stream. Events are serialised as newline-delimited JSON (NDJSON).
type Event struct {
	// Type is the kind of mutation (PUT or DELETE).
	Type EventType `json:"type"`
	// Key is the etcd key that was mutated.
	Key []byte `json:"key"`
	// Value is the new value for PUT events. Nil for DELETE events.
	Value []byte `json:"value,omitempty"`
	// Revision is the etcd store revision at which this event occurred.
	Revision int64 `json:"revision"`
}

// WriteEvents serialises events as newline-delimited JSON to w.
// Each event is written as a single JSON line followed by a newline.
func WriteEvents(w io.Writer, events []Event) error {
	enc := json.NewEncoder(w)
	for i, ev := range events {
		if err := enc.Encode(ev); err != nil {
			return fmt.Errorf("encoding event %d: %w", i, err)
		}
	}
	return nil
}

// ReadEvents deserialises newline-delimited JSON events from r.
// An empty reader returns an empty slice and no error.
func ReadEvents(r io.Reader) ([]Event, error) {
	var events []Event
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			return nil, fmt.Errorf("unmarshalling event: %w", err)
		}
		events = append(events, ev)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning events: %w", err)
	}
	return events, nil
}
