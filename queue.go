// Copyright 2019 github.com/ucirello and cirello.io. All rights reserved.
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
	"database/sql"
	"fmt"
)

// reasonable defaults
const (
	defaultTableName = "queue"
)

// State indicates the possible states of a message
type State string

// Acceptable states for messages.
const (
	New  State = "new"
	Done State = "done"
)

// Queue uses a postgreSQL database to run a queue system.
type Queue struct {
	db *sql.DB

	tableName string
}

// Open uses the given database connection and start operating the queue system.
func Open(db *sql.DB) *Queue {
	return &Queue{
		db:        db,
		tableName: defaultTableName,
	}
}

func (q *Queue) tx(ctx context.Context) (*sql.Tx, error) {
	return q.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
}

// CreateTable prepares the underlying table for the queue system.
func (q *Queue) CreateTable() error {
	ctx := context.TODO()
	tx, err := q.tx(ctx)
	if err != nil {
		return fmt.Errorf("cannot create transaction for table creation: %w", err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS `+q.tableName+` (
	id serial,
	queue varchar,
	state varchar,
	content bytea
);`)
	if err != nil {
		return fmt.Errorf("cannot create queue table: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cannot commit table creation transaction: %w", err)
	}
	return nil
}

// Push enqueues the given content to the target queue.
func (q *Queue) Push(target string, content []byte) error {
	ctx := context.TODO()
	tx, err := q.tx(ctx)
	if err != nil {
		return fmt.Errorf("cannot create transaction for message push: %w", err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO `+q.tableName+` (queue, state, content) VALUES ($1, $2, $3)`, target, New, content)
	if err != nil {
		return fmt.Errorf("cannot store message: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cannot commit message push transaction: %w", err)
	}
	return nil
}

// ErrEmptyQueue indicates there isn't any message available at the head of the
// queue.
var ErrEmptyQueue = fmt.Errorf("empty queue")

// Pop retrieves the pending message from the queue, if any available. If the
// queue is empty, it returns ErrEmptyQueue.
func (q *Queue) Pop(target string) ([]byte, error) {
	ctx := context.TODO()
	tx, err := q.tx(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot create transaction for message pop: %w", err)
	}
	defer tx.Rollback()
	row := tx.QueryRowContext(ctx, `SELECT id, content FROM `+q.tableName+` WHERE state = $1`, New)
	var (
		id      uint64
		content []byte
	)
	if err := row.Scan(&id, &content); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("cannot read message: %w", err)
	} else if err == sql.ErrNoRows {
		return nil, ErrEmptyQueue
	}
	if _, err := tx.ExecContext(ctx, `UPDATE `+q.tableName+` SET state = $1 WHERE id = $2`, Done, id); err != nil {
		return nil, fmt.Errorf("cannot store message: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("cannot commit message pop transaction: %w", err)
	}
	return content, nil
}
