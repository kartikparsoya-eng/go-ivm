// Package ivm is a 1:1 port of the Zero IVM (Incremental View Maintenance) engine.
// Source of truth: mono/packages/zql/src/ivm/
//
// This package preserves the TS architecture structurally and behaviorally.
// Generators become slice-returning functions. The 'yield' scheduling signal is dropped.
package ivm
