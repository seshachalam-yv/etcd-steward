// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package errors

import (
	"errors"
	"fmt"
)

// ErrCode represents an error code for categorizing errors.
type ErrCode int

const (
	// ErrCodeUnknown indicates an unknown error.
	ErrCodeUnknown ErrCode = iota
	// ErrCodeConfig indicates a configuration error.
	ErrCodeConfig
	// ErrCodeIO indicates an I/O error.
	ErrCodeIO
	// ErrCodeNetwork indicates a network error.
	ErrCodeNetwork
	// ErrCodeEtcd indicates an etcd client error.
	ErrCodeEtcd
	// ErrCodeCompaction indicates a compaction error.
	ErrCodeCompaction
	// ErrCodeSnapshot indicates a snapshot error.
	ErrCodeSnapshot
	// ErrCodeRestore indicates a restore error.
	ErrCodeRestore
	// ErrCodeBackup indicates a backup error.
	ErrCodeBackup
	// ErrCodeStorage indicates a storage backend error.
	ErrCodeStorage
	// ErrCodeTimeout indicates a timeout error.
	ErrCodeTimeout
	// ErrCodeNotFound indicates a not-found error.
	ErrCodeNotFound
	// ErrCodeAlreadyExists indicates a resource already exists.
	ErrCodeAlreadyExists
	// ErrCodePermission indicates a permission error.
	ErrCodePermission
	// ErrCodeValidation indicates a validation error.
	ErrCodeValidation
	// ErrCodeInternal indicates an internal error.
	ErrCodeInternal
	// ErrCodeUnavailable indicates a service unavailable error.
	ErrCodeUnavailable
	// ErrCodeCancelled indicates a cancelled operation error.
	ErrCodeCancelled
	// ErrCodeMissingRequired indicates a required field is missing.
	ErrCodeMissingRequired
	// ErrCodeInvalidConfig indicates an invalid configuration value.
	ErrCodeInvalidConfig
	// ErrCodeInvalidTransition indicates an invalid state machine transition.
	ErrCodeInvalidTransition
)

// Error represents a structured error with a code, optional sub-code, cause, message, and operation.
type Error struct {
	code      ErrCode
	subCode   int
	cause     error
	message   string
	operation string
}

// New creates a new Error with the given code and message.
func New(code ErrCode, message string) *Error {
	return &Error{
		code:    code,
		message: message,
	}
}

// NewWithOperation creates a new Error with the given code, message, and operation.
func NewWithOperation(code ErrCode, message string, operation string) *Error {
	return &Error{
		code:      code,
		message:   message,
		operation: operation,
	}
}

// Wrap wraps a cause error with a code and message.
func Wrap(code ErrCode, message string, cause error) *Error {
	return &Error{
		code:    code,
		message: message,
		cause:   cause,
	}
}

// WrapWithSubCode wraps a cause error with a code, sub-code, and message.
func WrapWithSubCode(code ErrCode, subCode int, message string, cause error) *Error {
	return &Error{
		code:    code,
		subCode: subCode,
		message: message,
		cause:   cause,
	}
}

// Error returns the string representation of the error.
func (e *Error) Error() string {
	s := ""
	if e.operation != "" {
		s = fmt.Sprintf("[%s] ", e.operation)
	}
	s += fmt.Sprintf("code=%d", e.code)
	if e.subCode != 0 {
		s += fmt.Sprintf(" subcode=%d", e.subCode)
	}
	s += fmt.Sprintf(": %s", e.message)
	if e.cause != nil {
		s += fmt.Sprintf(": %s", e.cause.Error())
	}
	return s
}

// Unwrap returns the underlying cause error.
func (e *Error) Unwrap() error {
	return e.cause
}

// Code returns the error code.
func (e *Error) Code() ErrCode {
	return e.code
}

// SubCode returns the error sub-code.
func (e *Error) SubCode() int {
	return e.subCode
}

// Operation returns the operation that caused the error.
func (e *Error) Operation() string {
	return e.operation
}

// Is delegates to errors.Is for error chain comparison.
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// As delegates to errors.As for error chain type assertion.
func As(err error, target interface{}) bool {
	return errors.As(err, target)
}
