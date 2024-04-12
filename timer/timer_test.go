package timer

import (
	"fmt"
	"testing"
	"time"
)

func rec2(i int) {
	defer TimeFunction("rec2")()
	BusySleep(time.Millisecond * 10)
	rec(i)
}

func rec(i int) {
	if i > 0 {
		rec2(i - 1)
	}

	defer TimeFunction("rec")()
	BusySleep(time.Millisecond * 10)
}

func TestTimer(test *testing.T) {
	Begin()
	profile("outerloop")
	rec(1)
	profile("outerloop")
	End()

	// We're testing that the profiler is correctly handling recursive profiling
	profileRecMicroseconds := t.cyclesToMicroSeconds(t.profiles[t.getLabelIndex("rec")].timer)
	if profileRecMicroseconds < 19_800 || profileRecMicroseconds > 20_200 {
		test.Error(fmt.Sprintf("the label 'rec' totalCycles time was not correct, got : %s", prettyPrint(profileRecMicroseconds)))
	}
	Print()
}
