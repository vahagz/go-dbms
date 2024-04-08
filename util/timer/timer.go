package timer

import "time"

func SetInterval(duration time.Duration, f func()) *time.Ticker {
	t := time.NewTicker(duration)
	go func() {
		for range t.C {
			f()
		}
	}()
	return t
}

func SetTimeout(duration time.Duration, f func()) *time.Ticker {
	var t *time.Ticker
	t = SetInterval(duration, func() {
		f()
		t.Stop()
	})
	return t
}
