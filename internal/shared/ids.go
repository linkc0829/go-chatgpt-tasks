// Package shared holds zero-dependency value objects used across features.
//
// RULE: This package must not import any internal/platform/* or third-party
// driver. Stdlib only (and trivially-pure libs like google/uuid).
package shared

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// ----------------------------------------------------------------------------
// Strongly-typed IDs
//
// Each ID type implements:
//   - encoding.TextMarshaler / TextUnmarshaler (for JSON)
//   - sql.Scanner / driver.Valuer            (for database/sql + pgx fallback)
//
// pgx's native codec works on `uuid.UUID` directly but does NOT auto-promote to
// named types defined as `type X uuid.UUID`. The Scan/Value pair makes pgx fall
// back to the database/sql interface and round-trip our IDs cleanly.
// ----------------------------------------------------------------------------

type UserID uuid.UUID
type JobID uuid.UUID
type JobRunID uuid.UUID
type RunEventID uuid.UUID

func NewUserID() UserID     { return UserID(uuid.New()) }
func NewJobID() JobID       { return JobID(uuid.New()) }
func NewJobRunID() JobRunID { return JobRunID(uuid.New()) }
func NewRunEventID() RunEventID {
	return RunEventID(uuid.New())
}

func (id UserID) String() string   { return uuid.UUID(id).String() }
func (id JobID) String() string    { return uuid.UUID(id).String() }
func (id JobRunID) String() string { return uuid.UUID(id).String() }
func (id RunEventID) String() string {
	return uuid.UUID(id).String()
}

func (id UserID) IsZero() bool   { return uuid.UUID(id) == uuid.Nil }
func (id JobID) IsZero() bool    { return uuid.UUID(id) == uuid.Nil }
func (id JobRunID) IsZero() bool { return uuid.UUID(id) == uuid.Nil }
func (id RunEventID) IsZero() bool {
	return uuid.UUID(id) == uuid.Nil
}

// ---------- JSON / text ----------

func (id UserID) MarshalText() ([]byte, error)   { return uuid.UUID(id).MarshalText() }
func (id JobID) MarshalText() ([]byte, error)    { return uuid.UUID(id).MarshalText() }
func (id JobRunID) MarshalText() ([]byte, error) { return uuid.UUID(id).MarshalText() }
func (id RunEventID) MarshalText() ([]byte, error) {
	return uuid.UUID(id).MarshalText()
}

func (id *UserID) UnmarshalText(b []byte) error {
	u, err := uuid.ParseBytes(b)
	if err != nil {
		return err
	}
	*id = UserID(u)
	return nil
}

func (id *JobID) UnmarshalText(b []byte) error {
	u, err := uuid.ParseBytes(b)
	if err != nil {
		return err
	}
	*id = JobID(u)
	return nil
}

func (id *JobRunID) UnmarshalText(b []byte) error {
	u, err := uuid.ParseBytes(b)
	if err != nil {
		return err
	}
	*id = JobRunID(u)
	return nil
}

func (id *RunEventID) UnmarshalText(b []byte) error {
	u, err := uuid.ParseBytes(b)
	if err != nil {
		return err
	}
	*id = RunEventID(u)
	return nil
}

// ---------- database/sql round-tripping ----------

func (id UserID) Value() (driver.Value, error)   { return uuid.UUID(id).Value() }
func (id JobID) Value() (driver.Value, error)    { return uuid.UUID(id).Value() }
func (id JobRunID) Value() (driver.Value, error) { return uuid.UUID(id).Value() }
func (id RunEventID) Value() (driver.Value, error) {
	return uuid.UUID(id).Value()
}

func (id *UserID) Scan(src any) error {
	var u uuid.UUID
	if err := u.Scan(src); err != nil {
		return fmt.Errorf("scan UserID: %w", err)
	}
	*id = UserID(u)
	return nil
}

func (id *JobID) Scan(src any) error {
	var u uuid.UUID
	if err := u.Scan(src); err != nil {
		return fmt.Errorf("scan JobID: %w", err)
	}
	*id = JobID(u)
	return nil
}

func (id *JobRunID) Scan(src any) error {
	var u uuid.UUID
	if err := u.Scan(src); err != nil {
		return fmt.Errorf("scan JobRunID: %w", err)
	}
	*id = JobRunID(u)
	return nil
}

func (id *RunEventID) Scan(src any) error {
	var u uuid.UUID
	if err := u.Scan(src); err != nil {
		return fmt.Errorf("scan RunEventID: %w", err)
	}
	*id = RunEventID(u)
	return nil
}

// ---------- Parsers ----------

var ErrInvalidID = errors.New("invalid id")

func ParseUserID(s string) (UserID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return UserID{}, ErrInvalidID
	}
	return UserID(u), nil
}

func ParseJobID(s string) (JobID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return JobID{}, ErrInvalidID
	}
	return JobID(u), nil
}

func ParseJobRunID(s string) (JobRunID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return JobRunID{}, ErrInvalidID
	}
	return JobRunID(u), nil
}

func ParseRunEventID(s string) (RunEventID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return RunEventID{}, ErrInvalidID
	}
	return RunEventID(u), nil
}
