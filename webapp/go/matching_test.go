package main

import "testing"

func TestMatchingDistanceLargeCoordinates(t *testing.T) {
	const low, high = -2147483648, 2147483647
	if got := matchingDistance(low, high, high, low); got != 8589934590 {
		t.Fatalf("distance = %d, want 8589934590", got)
	}
}
