// Copyright 2024 github.com/ucirello and cirello.io. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgqueue

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// anyArg stands in for an argument whose value the error-path tests do not
// care about. pgx v5.12.0 added BatchResults.Tx, which the latest pgxmock
// release does not implement, so these tests use this double instead.
var anyArg any = struct{}{}

// stubConn is a PgxConn that returns scripted results.
type stubConn struct {
	mu    sync.Mutex
	execs []*stubCall
	qs    []*stubCall
	cps   []*stubCopy
}

type stubCall struct {
	sql       string
	checkArgs bool
	nargs     int
	err       error
	tag       pgconn.CommandTag
	hasTag    bool
	rows      *stubRows
}

type stubCopy struct {
	table pgx.Identifier
	cols  []string
	err   error
}

func newStub() *stubConn { return &stubConn{} }

func (s *stubConn) ExpectExec(sql string) *stubCall {
	c := &stubCall{sql: sql}
	s.execs = append(s.execs, c)
	return c
}

func (s *stubConn) ExpectQuery(sql string) *stubCall {
	c := &stubCall{sql: sql}
	s.qs = append(s.qs, c)
	return c
}

func (s *stubConn) ExpectCopyFrom(table pgx.Identifier, cols []string) *stubCopy {
	c := &stubCopy{table: table, cols: append([]string(nil), cols...)}
	s.cps = append(s.cps, c)
	return c
}

func (c *stubCall) WithArgs(args ...any) *stubCall {
	c.checkArgs = true
	c.nargs = len(args)
	return c
}

func (c *stubCall) WillReturnError(err error) *stubCall {
	c.err = err
	return c
}

func (c *stubCall) WillReturnResult(tag pgconn.CommandTag) *stubCall {
	c.tag = tag
	c.hasTag = true
	return c
}

func (c *stubCall) WillReturnRows(rows *stubRows) *stubCall {
	c.rows = rows
	return c
}

func (c *stubCopy) WillReturnError(err error) *stubCopy {
	c.err = err
	return c
}

func newResult(op string, n int64) pgconn.CommandTag {
	return pgconn.NewCommandTag(fmt.Sprintf("%s %d", op, n))
}

func (s *stubConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, err := takeCall(&s.execs, "Exec", sql, args)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	if c.err != nil {
		return pgconn.CommandTag{}, c.err
	}
	if !c.hasTag {
		return pgconn.CommandTag{}, fmt.Errorf("Exec %q has no result", collapseSQL(sql))
	}
	return c.tag, nil
}

func (s *stubConn) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, err := takeCall(&s.qs, "Query", sql, args)
	if err != nil {
		return nil, err
	}
	if c.err != nil {
		return nil, c.err
	}
	if c.rows == nil {
		return nil, fmt.Errorf("Query %q has no rows", collapseSQL(sql))
	}
	return c.rows, nil
}

func (s *stubConn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	rows, err := s.Query(ctx, sql, args...)
	if err != nil {
		return errRow{err: err}
	}
	return rows.(pgx.Row)
}

func (s *stubConn) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.cps) == 0 {
		return 0, fmt.Errorf("unexpected CopyFrom %s", tableName.Sanitize())
	}
	c := s.cps[0]
	s.cps = s.cps[1:]
	if c.table.Sanitize() != tableName.Sanitize() || strings.Join(c.cols, ",") != strings.Join(columnNames, ",") {
		return 0, fmt.Errorf("CopyFrom %s (%s), want %s (%s)", tableName.Sanitize(), strings.Join(columnNames, ","), c.table.Sanitize(), strings.Join(c.cols, ","))
	}
	return 0, c.err
}

func takeCall(q *[]*stubCall, kind, sql string, args []any) (*stubCall, error) {
	if len(*q) == 0 {
		return nil, fmt.Errorf("unexpected %s %q", kind, collapseSQL(sql))
	}
	c := (*q)[0]
	*q = (*q)[1:]
	if !strings.Contains(collapseSQL(sql), collapseSQL(c.sql)) {
		return nil, fmt.Errorf("%s %q does not contain %q", kind, collapseSQL(sql), collapseSQL(c.sql))
	}
	if c.checkArgs && len(args) != c.nargs {
		return nil, fmt.Errorf("%s %q: got %d args, want %d", kind, collapseSQL(c.sql), len(args), c.nargs)
	}
	return c, nil
}

func collapseSQL(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}

type errRow struct{ err error }

func (r errRow) Scan(...any) error { return r.err }

type stubRows struct {
	rows      [][]any
	i         int
	cur       []any
	started   bool
	exhausted bool
	closed    bool
	closeErr  error
}

func newRows(cols []string) *stubRows {
	return &stubRows{}
}

func (r *stubRows) AddRow(vals ...any) *stubRows {
	r.rows = append(r.rows, vals)
	return r
}

func (r *stubRows) CloseError(err error) *stubRows {
	r.closeErr = err
	return r
}

func (r *stubRows) Close() { r.closed = true }

func (r *stubRows) Err() error {
	if r.exhausted || r.closed {
		return r.closeErr
	}
	return nil
}

func (r *stubRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *stubRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *stubRows) Next() bool {
	if r.i < len(r.rows) {
		r.cur = r.rows[r.i]
		r.i++
		r.started = true
		return true
	}
	r.exhausted = true
	return false
}

func (r *stubRows) Scan(dest ...any) error {
	if !r.started {
		return fmt.Errorf("Scan called without Next")
	}
	if len(dest) != len(r.cur) {
		return fmt.Errorf("expected %d destination arguments in Scan, not %d", len(r.cur), len(dest))
	}
	for i, d := range dest {
		if err := assignScan(d, r.cur[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *stubRows) Values() ([]any, error) { return append([]any(nil), r.cur...), nil }

func (r *stubRows) RawValues() [][]byte { return nil }

func (r *stubRows) Conn() *pgx.Conn { return nil }

func (r *stubRows) TypeMap() *pgtype.Map { return nil }

func assignScan(dest, val any) error {
	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Pointer || dv.IsNil() {
		return fmt.Errorf("scan destination must be a non-nil pointer, got %T", dest)
	}
	target := dv.Elem()
	if val == nil {
		target.Set(reflect.Zero(target.Type()))
		return nil
	}
	sv := reflect.ValueOf(val)
	if !sv.Type().AssignableTo(target.Type()) {
		return fmt.Errorf("cannot scan %T into %s", val, target.Type())
	}
	target.Set(sv)
	return nil
}
