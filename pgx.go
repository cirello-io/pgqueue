// Copyright 2024 github.com/ucirello and cirello.io. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to writing, software distributed
// under the License is distributed on a "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied.
//
// See the License for the specific language governing permissions and
// limitations under the License.

package pgqueue

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Conn is an acquired *pgx.Conn from a Pool.
type Conn interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}

func quoteIdentifier(s string) string {
	return (pgx.Identifier{s}).Sanitize()
}

// connDo executes a function with a non-cancelable context.
func (c *Client) connDo(f func(*nonCancelableConn) error) error {
	return f(&nonCancelableConn{c.pool})
}

type nonCancelableConn struct {
	conn Conn
}

func (c *nonCancelableConn) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return c.conn.Query(context.WithoutCancel(ctx), sql, args...)
}

func (c *nonCancelableConn) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return c.conn.Exec(context.WithoutCancel(ctx), sql, arguments...)
}

func (c *nonCancelableConn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return c.conn.QueryRow(context.WithoutCancel(ctx), sql, args...)
}

func (c *nonCancelableConn) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return c.conn.CopyFrom(ctx, tableName, columnNames, rowSrc)
}
