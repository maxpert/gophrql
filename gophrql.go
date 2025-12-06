package gophrql

import (
	"errors"
	"fmt"
)

// ErrNotImplemented indicates a requested feature has not been built yet.
var ErrNotImplemented = errors.New("gophrql: compiler not implemented")

// Compile compiles a PRQL query into SQL following the PRQL book semantics.
// The implementation is currently a stub; it will evolve alongside the spec.
func Compile(prql string) (string, error) {
	if prql == "" {
		return "", fmt.Errorf("gophrql: prql input is empty")
	}

	return "", ErrNotImplemented
}
