package task

import (
	"strconv"
	"strings"
	"time"
)

type Rule struct {
	Freq     string
	Interval int
}

type DSTNote string

const (
	DSTShiftedForward DSTNote = "shifted_forward"
	DSTAmbiguousFirst DSTNote = "ambiguous_first"
)

func ParseRule(s string) (Rule, error) {
	rule := Rule{Interval: 1}
	for _, part := range strings.Split(s, ";") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return Rule{}, ErrInvalidRecurrence
		}
		switch strings.ToUpper(key) {
		case "FREQ":
			rule.Freq = strings.ToUpper(value)
		case "INTERVAL":
			interval, err := strconv.Atoi(value)
			if err != nil || interval < 1 {
				return Rule{}, ErrInvalidRecurrence
			}
			rule.Interval = interval
		default:
			return Rule{}, ErrInvalidRecurrence
		}
	}
	if rule.Freq != "DAILY" && rule.Freq != "WEEKLY" {
		return Rule{}, ErrInvalidRecurrence
	}
	return rule, nil
}

func NextOccurrence(rule Rule, localTime string, tz *time.Location, after time.Time) (time.Time, DSTNote, error) {
	hour, minute, err := parseLocalTime(localTime)
	if err != nil || tz == nil || rule.Interval < 1 || (rule.Freq != "DAILY" && rule.Freq != "WEEKLY") {
		return time.Time{}, "", ErrInvalidRecurrence
	}

	stepDays := rule.Interval
	if rule.Freq == "WEEKLY" {
		stepDays *= 7
	}
	localAfter := after.In(tz)
	date := time.Date(localAfter.Year(), localAfter.Month(), localAfter.Day(), hour, minute, 0, 0, tz)
	if !date.After(after) {
		date = time.Date(localAfter.Year(), localAfter.Month(), localAfter.Day()+stepDays, hour, minute, 0, 0, tz)
	}
	return resolveWallTime(date, hour, minute, tz)
}

func parseLocalTime(s string) (int, int, error) {
	t, err := time.Parse("15:04", s)
	if err != nil {
		return 0, 0, ErrInvalidRecurrence
	}
	return t.Hour(), t.Minute(), nil
}

func resolveWallTime(candidate time.Time, hour, minute int, tz *time.Location) (time.Time, DSTNote, error) {
	local := candidate.In(tz)
	if local.Hour() != hour || local.Minute() != minute {
		wallDelta := time.Duration((hour-local.Hour())*60+(minute-local.Minute())) * time.Minute
		if wallDelta > 0 {
			candidate = candidate.Add(wallDelta)
		}
		return candidate.UTC(), DSTShiftedForward, nil
	}

	earliest := candidate
	for delta := -2 * time.Hour; delta <= 2*time.Hour; delta += 30 * time.Minute {
		other := candidate.Add(delta).In(tz)
		if other.Year() == local.Year() && other.YearDay() == local.YearDay() &&
			other.Hour() == hour && other.Minute() == minute && other.Before(earliest) {
			earliest = other
		}
	}
	if !earliest.Equal(candidate) {
		return earliest.UTC(), DSTAmbiguousFirst, nil
	}
	for delta := 30 * time.Minute; delta <= 2*time.Hour; delta += 30 * time.Minute {
		other := candidate.Add(delta).In(tz)
		if other.Year() == local.Year() && other.YearDay() == local.YearDay() &&
			other.Hour() == hour && other.Minute() == minute {
			return candidate.UTC(), DSTAmbiguousFirst, nil
		}
	}
	return candidate.UTC(), "", nil
}
