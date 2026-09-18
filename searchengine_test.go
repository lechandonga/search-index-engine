package searchengine

import "testing"

func TestSkeletonCompiles(t *testing.T) {
	if FormatVersion() == 0 {
		t.Fatal("format version must be non-zero")
	}
}
