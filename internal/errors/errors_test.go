// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package errors

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestNew_CreatesError(t *testing.T) {
	err := New(ErrCodeConfig, "bad config")
	if err == nil {
		t.Fatal("expected non-nil error")
	}
	if err.Code() != ErrCodeConfig {
		t.Fatalf("expected code %d, got %d", ErrCodeConfig, err.Code())
	}
	if !strings.Contains(err.Error(), "bad config") {
		t.Fatalf("expected message to contain 'bad config', got %q", err.Error())
	}
}

func TestNewWithOperation_SetsOperation(t *testing.T) {
	err := NewWithOperation(ErrCodeIO, "write failed", "SaveSnapshot")
	if err.Operation() != "SaveSnapshot" {
		t.Fatalf("expected operation 'SaveSnapshot', got %q", err.Operation())
	}
	if !strings.Contains(err.Error(), "[SaveSnapshot]") {
		t.Fatalf("expected error string to contain '[SaveSnapshot]', got %q", err.Error())
	}
}

func TestWrap_PreservesChain(t *testing.T) {
	root := fmt.Errorf("disk full")
	wrapped := Wrap(ErrCodeStorage, "storage write failed", root)

	if wrapped.Unwrap() != root {
		t.Fatal("Unwrap did not return the root cause")
	}

	if !errors.Is(wrapped, root) {
		t.Fatal("errors.Is failed to find root cause in chain")
	}

	if !strings.Contains(wrapped.Error(), "disk full") {
		t.Fatalf("expected error to contain root message, got %q", wrapped.Error())
	}
}

func TestWrapWithSubCode_SetsSubCode(t *testing.T) {
	cause := fmt.Errorf("connection refused")
	err := WrapWithSubCode(ErrCodeNetwork, 42, "etcd unreachable", cause)
	if err.SubCode() != 42 {
		t.Fatalf("expected subcode 42, got %d", err.SubCode())
	}
	if err.Code() != ErrCodeNetwork {
		t.Fatalf("expected code %d, got %d", ErrCodeNetwork, err.Code())
	}
	if !strings.Contains(err.Error(), "subcode=42") {
		t.Fatalf("expected error string to contain 'subcode=42', got %q", err.Error())
	}
}

func TestError_FormatWithAllFields(t *testing.T) {
	cause := fmt.Errorf("underlying")
	err := &Error{
		code:      ErrCodeSnapshot,
		subCode:   7,
		cause:     cause,
		message:   "snapshot failed",
		operation: "TakeSnapshot",
	}
	s := err.Error()
	if !strings.Contains(s, "[TakeSnapshot]") {
		t.Fatalf("missing operation in %q", s)
	}
	if !strings.Contains(s, "code=6") {
		t.Fatalf("missing code in %q", s)
	}
	if !strings.Contains(s, "subcode=7") {
		t.Fatalf("missing subcode in %q", s)
	}
	if !strings.Contains(s, "snapshot failed") {
		t.Fatalf("missing message in %q", s)
	}
	if !strings.Contains(s, "underlying") {
		t.Fatalf("missing cause in %q", s)
	}
}

func TestError_FormatMinimalFields(t *testing.T) {
	err := New(ErrCodeUnknown, "something happened")
	s := err.Error()
	if strings.Contains(s, "[") {
		t.Fatalf("minimal error should not contain operation brackets, got %q", s)
	}
	if strings.Contains(s, "subcode=") {
		t.Fatalf("minimal error should not contain subcode, got %q", s)
	}
	expected := "code=0: something happened"
	if s != expected {
		t.Fatalf("expected %q, got %q", expected, s)
	}
}

func TestUnwrap_ReturnsCause(t *testing.T) {
	cause := fmt.Errorf("root cause")
	err := Wrap(ErrCodeInternal, "wrapped", cause)
	if err.Unwrap() != cause {
		t.Fatal("Unwrap should return the original cause")
	}

	errNoCause := New(ErrCodeConfig, "no cause")
	if errNoCause.Unwrap() != nil {
		t.Fatal("Unwrap should return nil when there is no cause")
	}
}

func TestErrorCodes_Unique(t *testing.T) {
	codes := []ErrCode{
		ErrCodeUnknown,
		ErrCodeConfig,
		ErrCodeIO,
		ErrCodeNetwork,
		ErrCodeEtcd,
		ErrCodeCompaction,
		ErrCodeSnapshot,
		ErrCodeRestore,
		ErrCodeBackup,
		ErrCodeStorage,
		ErrCodeTimeout,
		ErrCodeNotFound,
		ErrCodeAlreadyExists,
		ErrCodePermission,
		ErrCodeValidation,
		ErrCodeInternal,
		ErrCodeUnavailable,
		ErrCodeCancelled,
		ErrCodeMissingRequired,
		ErrCodeInvalidConfig,
		ErrCodeInvalidTransition,
	}

	seen := make(map[ErrCode]bool)
	for _, c := range codes {
		if seen[c] {
			t.Fatalf("duplicate error code: %d", c)
		}
		seen[c] = true
	}

	if len(codes) != 21 {
		t.Fatalf("expected 21 unique error codes, got %d", len(codes))
	}
}
