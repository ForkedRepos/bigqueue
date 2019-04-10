package bigqueue

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"
)

func TestSetPeriodicFlushOpsFail(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%v", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	_, err := NewMmapQueue(testDir, SetPeriodicFlushOps(0))
	if err != ErrMustBeGreaterThanZero {
		t.Fatalf("expected error ErrMustBeGreaterThanZero, got: %v", err)
	}
}

func TestSetPeriodicFlushDurationFail(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%v", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	_, err := NewMmapQueue(testDir, SetPeriodicFlushDuration(0))
	if err != ErrMustBeGreaterThanZero {
		t.Fatalf("expected error ErrMustBeGreaterThanZero, got: %v", err)
	}
}
