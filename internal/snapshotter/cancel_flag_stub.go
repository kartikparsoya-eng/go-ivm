//go:build !cgo

package snapshotter

import "database/sql"

type snapCancelFlag struct{}

func newSnapCancelFlag() *snapCancelFlag { return &snapCancelFlag{} }
func (f *snapCancelFlag) Free()          {}
func (f *snapCancelFlag) setCancel()     {}
func (f *snapCancelFlag) clearCancel()   {}
func (f *snapCancelFlag) setBudget(_ int32) {}
func (f *snapCancelFlag) registerOn(_ *sql.Conn) error { return nil }
