// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"gopkg.in/yaml.v3"
)

func unmarshalYAML(data []byte, v interface{}) error {
	return yaml.Unmarshal(data, v)
}

func marshalYAML(v interface{}) ([]byte, error) {
	return yaml.Marshal(v)
}
