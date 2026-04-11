// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package errors_test

import (
	"errors"
	"fmt"
	"testing"

	stderrors "github.com/gardener/etcd-steward/pkg/errors"
)

func TestNew_ErrorString(t *testing.T) {
	err := stderrors.New(stderrors.CodeSnapstore, stderrors.SubCodeUploadFailed, "upload to S3 failed")
	want := "[SNAPSTORE/UPLOAD_FAILED] upload to S3 failed"
	if err.Error() != want {
		t.Errorf("Error() = %q, want %q", err.Error(), want)
	}
}

func TestNew_NoMessage(t *testing.T) {
	err := stderrors.New(stderrors.CodeValidation, stderrors.SubCodeDBCorrupt, "")
	want := "[VALIDATION/DB_CORRUPT]"
	if err.Error() != want {
		t.Errorf("Error() = %q, want %q", err.Error(), want)
	}
}

func TestWrap_NilCauseReturnsNil(t *testing.T) {
	err := stderrors.Wrap(nil, stderrors.CodeInternal, stderrors.SubCodeTimeout, "should be nil")
	if err != nil {
		t.Errorf("Wrap(nil) = %v, want nil", err)
	}
}

func TestWrap_PreservesChain(t *testing.T) {
	cause := fmt.Errorf("original error")
	wrapped := stderrors.Wrap(cause, stderrors.CodeEtcdClient, stderrors.SubCodeTimeout, "connection timed out")
	if wrapped == nil {
		t.Fatal("Wrap returned nil for non-nil cause")
	}
	if !errors.Is(wrapped, cause) {
		t.Error("errors.Is: wrapped error should contain original cause")
	}
}

func TestWrapf_FormatMessage(t *testing.T) {
	cause := fmt.Errorf("rpc error")
	wrapped := stderrors.Wrapf(cause, stderrors.CodeEtcdClient, stderrors.SubCodeTimeout, "endpoint %s failed", "localhost:2379")
	if wrapped == nil {
		t.Fatal("Wrapf returned nil for non-nil cause")
	}
	want := "[ETCD_CLIENT/TIMEOUT] endpoint localhost:2379 failed: rpc error"
	if wrapped.Error() != want {
		t.Errorf("Error() = %q, want %q", wrapped.Error(), want)
	}
}

func TestIsCode(t *testing.T) {
	err := stderrors.New(stderrors.CodeGC, stderrors.SubCodeSnapshotNotFound, "")
	if !stderrors.IsCode(err, stderrors.CodeGC) {
		t.Error("IsCode should return true for matching code")
	}
	if stderrors.IsCode(err, stderrors.CodeSnapstore) {
		t.Error("IsCode should return false for non-matching code")
	}
	if stderrors.IsCode(nil, stderrors.CodeGC) {
		t.Error("IsCode(nil) should return false")
	}
}

func TestIsSubCode(t *testing.T) {
	err := stderrors.New(stderrors.CodeRestoration, stderrors.SubCodeSnapshotNotFound, "no snapshot")
	if !stderrors.IsSubCode(err, stderrors.SubCodeSnapshotNotFound) {
		t.Error("IsSubCode should return true for matching subCode")
	}
	if stderrors.IsSubCode(err, stderrors.SubCodeDBCorrupt) {
		t.Error("IsSubCode should return false for non-matching subCode")
	}
}

func TestError_ImplementsErrorInterface(t *testing.T) {
	err := stderrors.New(stderrors.CodeAlarm, stderrors.SubCodeLockAcquireFailed, "lock failed")
	if err.Error() == "" {
		t.Error("Error() should return a non-empty string")
	}
}

func TestWrap_ErrorStringWithCause(t *testing.T) {
	cause := fmt.Errorf("db corrupted")
	wrapped := stderrors.Wrap(cause, stderrors.CodeValidation, stderrors.SubCodeDBCorrupt, "")
	want := "[VALIDATION/DB_CORRUPT] db corrupted"
	if wrapped.Error() != want {
		t.Errorf("Error() = %q, want %q", wrapped.Error(), want)
	}
}
