package helpers

import "time"

const layout = "2006-01-02 15:04:05"

func ParseTime(timestamp string) (time.Time, error) {
	return time.ParseInLocation(layout, timestamp, time.Local)
}

func FormatTime(t time.Time) string {
	return t.Format(layout)
}
