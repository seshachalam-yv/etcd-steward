// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapshotter

import "encoding/json"

// EventType mirrors mvccpb.Event_EventType for serialization without importing the server package.
type EventType int32

const (
	// EventTypePut represents a key creation or update.
	EventTypePut EventType = 0
	// EventTypeDelete represents a key deletion.
	EventTypeDelete EventType = 1
)

// DeltaEvent is the unit serialized into incremental snapshot files.
// One JSON object per line (newline-delimited JSON / NDJSON).
type DeltaEvent struct {
	Type        EventType `json:"type"`
	Key         []byte    `json:"key"`
	Value       []byte    `json:"value,omitempty"`
	ModRevision int64     `json:"modRevision"`
	Version     int64     `json:"version"`
}

// MarshalDeltaEvent serializes a single DeltaEvent as a JSON line (no trailing newline).
func MarshalDeltaEvent(ev DeltaEvent) ([]byte, error) {
	return json.Marshal(ev)
}

// UnmarshalDeltaEvent deserializes a single DeltaEvent from a JSON line.
func UnmarshalDeltaEvent(line []byte) (DeltaEvent, error) {
	var ev DeltaEvent
	if err := json.Unmarshal(line, &ev); err != nil {
		return DeltaEvent{}, err
	}
	return ev, nil
}
