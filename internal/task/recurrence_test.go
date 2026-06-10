package task

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNextOccurrence_TimezoneAndDST(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	daily := Rule{Freq: "DAILY", Interval: 1}

	tests := []struct {
		name      string
		localTime string
		after     time.Time
		want      time.Time
		wantNote  DSTNote
	}{
		{
			name:      "before_dst_uses_est",
			localTime: "08:00",
			after:     time.Date(2026, 3, 7, 14, 0, 0, 0, time.UTC),
			want:      time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC),
		},
		{
			name:      "after_dst_uses_edt",
			localTime: "08:00",
			after:     time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC),
			want:      time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC),
		},
		{
			name:      "skipped_time_shifts_forward",
			localTime: "02:30",
			after:     time.Date(2026, 3, 8, 5, 0, 0, 0, time.UTC),
			want:      time.Date(2026, 3, 8, 7, 30, 0, 0, time.UTC),
			wantNote:  DSTShiftedForward,
		},
		{
			name:      "ambiguous_time_uses_first",
			localTime: "01:30",
			after:     time.Date(2026, 11, 1, 4, 0, 0, 0, time.UTC),
			want:      time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC),
			wantNote:  DSTAmbiguousFirst,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, note, err := NextOccurrence(daily, tt.localTime, newYork, tt.after)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantNote, note)
		})
	}
}

func TestNewJob_RejectsInvalidScheduleConfiguration(t *testing.T) {
	base := ScheduleSpec{
		Type:           KindRecurring,
		RecurrenceRule: "FREQ=DAILY",
		LocalTime:      "08:00",
		TimezoneID:     "UTC",
	}

	t.Run("invalid_timezone", func(t *testing.T) {
		spec := base
		spec.TimezoneID = "Mars/Olympus"
		_, err := NewJob(testIdentity().TenantID, testIdentity().UserID, "test", spec)
		assert.True(t, errors.Is(err, ErrInvalidTimezone))
	})

	t.Run("unsupported_frequency", func(t *testing.T) {
		spec := base
		spec.RecurrenceRule = "FREQ=MONTHLY"
		_, err := NewJob(testIdentity().TenantID, testIdentity().UserID, "test", spec)
		assert.True(t, errors.Is(err, ErrInvalidRecurrence))
	})
}
