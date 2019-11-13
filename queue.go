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
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// reasonable defaults
const (
	defaultTableName = "queue"
)

// State indicates the possible states of a message
type State string

// Acceptable states for messages.
const (
	New        State = "new"
	InProgress State = "in-progress"
	Done       State = "done"
)

// Queue uses a postgreSQL database to run a queue system.
type Queue struct {
	tableName string
	db        *sql.DB
	listener  *pq.Listener
}

// Open uses the given database connection and start operating the queue system.
func Open(dsn string) (*Queue, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("cannot open database connection: %w", err)
	}
	return &Queue{
		tableName: defaultTableName,
		db:        db,
		listener:  pq.NewListener(dsn, 1*time.Second, 1*time.Second, func(pq.ListenerEventType, error) {}),
	}, nil
}

// Close stops the queue system.
func (q *Queue) Close() error {
	if err := q.listener.Close(); err != nil {
		return fmt.Errorf("cannot close listener: %w", err)
	}
	if err := q.db.Close(); err != nil {
		return fmt.Errorf("cannot close connection: %w", err)
	}
	return nil
}

func (q *Queue) tx() (*sql.Tx, error) {
	return q.db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
}

func (q *Queue) retry(f func() error) error {
	const serializationErrorCode = "40001"
	var err error
	for {
		err = f()
		if err == nil {
			return nil
		}
		var pqErr *pq.Error
		serializationRetry := errors.As(err, &pqErr) && pqErr.Code == serializationErrorCode
		if !serializationRetry {
			break
		}
	}
	return err
}

// CreateTable prepares the underlying table for the queue system.
func (q *Queue) CreateTable() error {
	tx, err := q.tx()
	if err != nil {
		return fmt.Errorf("cannot create transaction for table creation: %w", err)
	}
	defer tx.Rollback()
	_, err = tx.Exec(`
CREATE TABLE IF NOT EXISTS ` + pq.QuoteIdentifier(q.tableName) + ` (
	id serial,
	queue varchar,
	state varchar,
	tries int NOT NULL DEFAULT 0,
	leased_until TIMESTAMP WITHOUT TIME ZONE,
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
	if err := validate(content); err != nil {
		return err
	}
	return q.retry(func() error {
		tx, err := q.tx()
		if err != nil {
			return fmt.Errorf("cannot create transaction for message push: %w", err)
		}
		defer tx.Rollback()
		_, err = tx.Exec(`INSERT INTO `+pq.QuoteIdentifier(q.tableName)+` (queue, state, content) VALUES ($1, $2, $3)`, target, New, content)
		if err != nil {
			return fmt.Errorf("cannot store message: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("cannot commit message push transaction: %w", err)
		}
		_, err = q.db.Exec(`NOTIFY ` + pq.QuoteIdentifier(target))
		if err != nil {
			return fmt.Errorf("cannot send push notification: %w", err)
		}
		return nil
	})
}

// ErrEmptyQueue indicates there isn't any message available at the head of the
// queue.
var ErrEmptyQueue = fmt.Errorf("empty queue")

// Pop retrieves the pending message from the queue, if any available. If the
// queue is empty, it returns ErrEmptyQueue.
func (q *Queue) Pop(target string) ([]byte, error) {
	var content []byte
	err := q.retry(func() error {
		tx, err := q.tx()
		if err != nil {
			return fmt.Errorf("cannot create transaction for message pop: %w", err)
		}
		defer tx.Rollback()
		row := tx.QueryRow(`SELECT id, content FROM `+pq.QuoteIdentifier(q.tableName)+` WHERE queue = $1 AND state = $2 ORDER BY id ASC LIMIT 1`, target, New)
		var id uint64
		if err := row.Scan(&id, &content); err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("cannot read message: %w", err)
		} else if err == sql.ErrNoRows {
			return ErrEmptyQueue
		}
		if _, err := tx.Exec(`UPDATE `+pq.QuoteIdentifier(q.tableName)+` SET tries = tries + 1, state = $1 WHERE id = $2`, Done, id); err != nil {
			return fmt.Errorf("cannot store message: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("cannot commit message pop transaction: %w", err)
		}
		return nil
	})
	return content, err
}

// Reserve retrieves the pending message from the queue, if any available. It
// marks as it as InProgress until the defined lease duration. If the message
// is not marked as Done by the lease time, it is returned to the queue.
func (q *Queue) Reserve(target string, lease time.Duration) (*Message, error) {
	var message *Message
	err := q.retry(func() error {
		tx, err := q.tx()
		if err != nil {
			return fmt.Errorf("cannot create transaction for message pop: %w", err)
		}
		defer tx.Rollback()
		row := tx.QueryRow(`SELECT id, content FROM `+pq.QuoteIdentifier(q.tableName)+` WHERE queue = $1 AND state = $2 ORDER BY id ASC LIMIT 1`, target, New)
		var id uint64
		var content []byte
		leasedUntil := time.Now().UTC().Add(lease)
		if err := row.Scan(&id, &content); err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("cannot read message: %w", err)
		} else if err == sql.ErrNoRows {
			return ErrEmptyQueue
		}
		if _, err := tx.Exec(`UPDATE `+pq.QuoteIdentifier(q.tableName)+` SET tries = tries + 1, state = $1, leased_until = $2 WHERE id = $3`, InProgress, leasedUntil, id); err != nil {
			return fmt.Errorf("cannot store message: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("cannot commit message pop transaction: %w", err)
		}
		message = &Message{
			id:          id,
			Content:     content,
			LeasedUntil: leasedUntil,
			q:           q,
		}
		return nil
	})
	return message, err
}

func (q *Queue) done(id uint64) error {
	return q.retry(func() error {
		tx, err := q.tx()
		if err != nil {
			return fmt.Errorf("cannot create transaction for done message: %w", err)
		}
		defer tx.Rollback()
		if _, err := tx.Exec(`UPDATE `+pq.QuoteIdentifier(q.tableName)+` SET state = $1 WHERE id = $2`, Done, id); err != nil {
			return fmt.Errorf("cannot store message: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("cannot commit done message transaction: %w", err)
		}
		return nil
	})
}

func (q *Queue) release(id uint64) error {
	return q.retry(func() error {
		tx, err := q.tx()
		if err != nil {
			return fmt.Errorf("cannot create transaction for message release: %w", err)
		}
		defer tx.Rollback()
		if _, err := tx.Exec(`UPDATE `+pq.QuoteIdentifier(q.tableName)+` SET leased_until = null, state = $1 WHERE id = $2`, New, id); err != nil {
			return fmt.Errorf("cannot store message: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("cannot commit message release transaction: %w", err)
		}
		return nil
	})
}

func (q *Queue) touch(id uint64, extension time.Duration) error {
	return q.retry(func() error {
		tx, err := q.tx()
		if err != nil {
			return fmt.Errorf("cannot create transaction for message touch: %w", err)
		}
		defer tx.Rollback()
		leasedUntil := time.Now().UTC().Add(extension)
		if _, err := tx.Exec(`UPDATE `+pq.QuoteIdentifier(q.tableName)+` SET leased_until = $1 WHERE id = $2`, leasedUntil, id); err != nil {
			return fmt.Errorf("cannot store message: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("cannot commit message touch transaction: %w", err)
		}
		return nil
	})
}

// Vacuum cleans up the queue from done or dead messages.
func (q *Queue) Vacuum(target string) (VacuumStats, error) {
	var stats VacuumStats
	err := q.retry(func() error {
		tx, err := q.tx()
		if err != nil {
			return fmt.Errorf("cannot create transaction for queue vacuum: %w", err)
		}
		defer tx.Rollback()
		res, err := tx.Exec(`DELETE FROM `+pq.QuoteIdentifier(q.tableName)+` WHERE queue = $1 AND state = $2`, target, Done)
		if err != nil {
			return fmt.Errorf("cannot store message: %w", err)
		}
		stats.Done, err = res.RowsAffected()
		if err != nil {
			return fmt.Errorf("cannot calculate how many done messages were deleted: %w", err)
		}
		res, err = tx.Exec(`DELETE FROM `+pq.QuoteIdentifier(q.tableName)+` WHERE queue = $1 AND state = $2 AND leased_until < NOW()`, target, InProgress)
		if err != nil {
			return fmt.Errorf("cannot store message: %w", err)
		}
		stats.Deads, err = res.RowsAffected()
		if err != nil {
			return fmt.Errorf("cannot calculate how many done messages were deleted: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("cannot commit message push transaction: %w", err)
		}
		return nil
	})
	return stats, err
}

// VacuumStats reports the consequences of the clean up.
type VacuumStats struct {
	// Done reports how many messages marked as Done were deleted.
	Done int64
	// Deads reports how many messages marked as InProgress but with expired
	// leases were deleted.
	Deads int64
}

// ErrMessageTooLarge indicates the content to be pushed is too large.
var ErrMessageTooLarge = fmt.Errorf("message is too large")

// MaxMessageLength indicates the maximum content length acceptable for new
// messages. Although it is theoretically possible to use large messages, the
// idea here is to be conservative until the properties of PostgreSQL are fully
// mapped.
const MaxMessageLength = 65536

func validate(content []byte) error {
	if len(content) > MaxMessageLength {
		return ErrMessageTooLarge
	}
	return nil
}

// Watcher holds the pointer necessary to listen for postgreSQL events that
// indicates a new message has arrive in the pipe.
type Watcher struct {
	target string
	queue  *Queue
	msg    []byte
	err    error
}

// Watch observes new messages for the target queue.
func (q *Queue) Watch(target string) *Watcher {
	watcher := &Watcher{
		target: target,
		queue:  q,
	}
	watcher.err = q.listener.Listen(target)
	return watcher
}

// Next waits for the next message to arrive and store it into Watcher.
func (w *Watcher) Next() bool {
	if w.err != nil {
		return false
	}
	for {

		select {
		case _, ok := <-w.queue.listener.Notify:
			if !ok {
				return false
			}
		case <-time.After(5 * time.Second):
		}
		msg, err := w.queue.Pop(w.target)
		if err == sql.ErrConnDone {
			return false
		} else if err == ErrEmptyQueue {
			continue
		}
		w.msg = msg
		return true
	}
}

// Message returns the current message store in the Watcher.
func (w *Watcher) Message() []byte {
	return w.msg
}

// Err holds the last known error that might have happened in Watcher lifespan.
func (w *Watcher) Err() error {
	return w.err
}

// Message represents on message from the queue
type Message struct {
	id          uint64
	Content     []byte
	LeasedUntil time.Time
	q           *Queue
}

// Done mark message as done.
func (m *Message) Done() error {
	return m.q.done(m.id)
}

// Release put the message back to the queue.
func (m *Message) Release() error {
	return m.q.release(m.id)
}

// Touch extends the lease by the given duration
func (m *Message) Touch(extension time.Duration) error {
	return m.q.touch(m.id, extension)
}
