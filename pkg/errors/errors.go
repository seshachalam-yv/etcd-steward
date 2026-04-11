// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package errors defines a structured error type and sentinel error codes for etcd-steward.
// All internal errors should be wrapped using New or Wrap so that code, subCode, and cause
// are preserved across package boundaries.
package errors

import (
	"errors"
	"fmt"
)

// Code is a machine-readable top-level error category.
type Code string

const (
	// CodeSnapstore indicates a failure reading or writing to the snapshot store.
	CodeSnapstore Code = "SNAPSTORE"
	// CodeValidation indicates a DB or data-directory validation failure.
	CodeValidation Code = "VALIDATION"
	// CodeRestoration indicates a snapshot restoration failure.
	CodeRestoration Code = "RESTORATION"
	// CodeEtcdClient indicates a failure communicating with the etcd API.
	CodeEtcdClient Code = "ETCD_CLIENT"
	// CodeInitializer indicates a failure in the DEP-04 initialization flow.
	CodeInitializer Code = "INITIALIZER"
	// CodeGC indicates a failure in snapshot garbage collection.
	CodeGC Code = "GC"
	// CodeAlarm indicates a failure in alarm monitoring or remediation.
	CodeAlarm Code = "ALARM"
	// CodeInternal indicates an unexpected internal error.
	CodeInternal Code = "INTERNAL"
)

// SubCode is a machine-readable secondary classification beneath a Code.
type SubCode string

const (
	// SubCodeDBCorrupt indicates the bbolt DB file is corrupt.
	SubCodeDBCorrupt SubCode = "DB_CORRUPT"
	// SubCodeSnapshotNotFound indicates no snapshot was found in the store.
	SubCodeSnapshotNotFound SubCode = "SNAPSHOT_NOT_FOUND"
	// SubCodeMemberNotLeader indicates the operation was skipped because this member is not leader.
	SubCodeMemberNotLeader SubCode = "NOT_LEADER"
	// SubCodeUploadFailed indicates a snapshot upload to object storage failed.
	SubCodeUploadFailed SubCode = "UPLOAD_FAILED"
	// SubCodeDownloadFailed indicates a snapshot download from object storage failed.
	SubCodeDownloadFailed SubCode = "DOWNLOAD_FAILED"
	// SubCodeLockAcquireFailed indicates that the distributed defrag/snapshot lock could not be acquired.
	SubCodeLockAcquireFailed SubCode = "LOCK_ACQUIRE_FAILED"
	// SubCodeTimeout indicates an operation exceeded its deadline.
	SubCodeTimeout SubCode = "TIMEOUT"
)

// Error is a structured error that carries a top-level code, optional sub-code, an
// optional human-readable message, and the underlying cause.
//
// Use New to create a new root error, and Wrap to annotate an existing error.
type Error struct {
	code    Code
	subCode SubCode
	cause   error
	message string
}

// New creates a new Error with the given code, subCode, and message.
func New(code Code, subCode SubCode, message string) *Error {
	return &Error{code: code, subCode: subCode, message: message}
}

// Wrap wraps an existing error with a code, subCode, and contextual message.
// If cause is nil, Wrap returns nil.
func Wrap(cause error, code Code, subCode SubCode, message string) *Error {
	if cause == nil {
		return nil
	}
	return &Error{code: code, subCode: subCode, cause: cause, message: message}
}

// Wrapf wraps an existing error with a formatted message.
// If cause is nil, Wrapf returns nil.
func Wrapf(cause error, code Code, subCode SubCode, format string, args ...interface{}) *Error {
	if cause == nil {
		return nil
	}
	return &Error{code: code, subCode: subCode, cause: cause, message: fmt.Sprintf(format, args...)}
}

// Code returns the top-level error code.
func (e *Error) Code() Code { return e.code }

// SubCode returns the secondary error classification.
func (e *Error) SubCode() SubCode { return e.subCode }

// Cause returns the underlying error, if any.
func (e *Error) Cause() error { return e.cause }

// Unwrap returns the underlying cause so errors.Is / errors.As work across the chain.
func (e *Error) Unwrap() error { return e.cause }

// Error implements the error interface.
func (e *Error) Error() string {
	if e.cause != nil {
		if e.message != "" {
			return fmt.Sprintf("[%s/%s] %s: %v", e.code, e.subCode, e.message, e.cause)
		}
		return fmt.Sprintf("[%s/%s] %v", e.code, e.subCode, e.cause)
	}
	if e.message != "" {
		return fmt.Sprintf("[%s/%s] %s", e.code, e.subCode, e.message)
	}
	return fmt.Sprintf("[%s/%s]", e.code, e.subCode)
}

// IsCode reports whether any error in the chain is an *Error with the given code.
func IsCode(err error, code Code) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.code == code
	}
	return false
}

// IsSubCode reports whether any error in the chain is an *Error with the given subCode.
func IsSubCode(err error, subCode SubCode) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.subCode == subCode
	}
	return false
}
