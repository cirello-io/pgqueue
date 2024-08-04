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
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/singleflight"
)

// ErrEmptyQueue indicates there isn't any message available at the head of the
// queue.
var ErrEmptyQueue = fmt.Errorf("empty queue")

// ErrAlreadyClosed indicates the queue is closed and all its watchers are going
// to report the queue is no longer available.
var ErrAlreadyClosed = errors.New("queue is already closed")

// ErrInvalidDuration indicates the duration used is too small. It must larger
// than a millisecond and be multiple of a millisecond.
var ErrInvalidDuration = errors.New("invalid duration")

// ErrInvalidDeadline indicates the target deadline may be in the past or zero.
var ErrInvalidDeadline = errors.New("invalid duration")

// ErrZeroSizedBulkOperation that the bulk operation size is zero.
var ErrZeroSizedBulkOperation = errors.New("zero sized bulk operation")

// ErrReleaseIncomplete indicates that not all messages were released.
var ErrReleaseIncomplete = errors.New("not all messages were released")

// ErrDeadLetterQueueDisabled indicates that is not possible to dump messages
// from the target deadletter queue because its support has been disabled.
var ErrDeadLetterQueueDisabled = errors.New("deadletter queue disabled")

// DefaultMaxDeliveriesCount is how many delivery attempt each message gets
// before getting skipped on Pop and Reserve calls.
const DefaultMaxDeliveriesCount = 5

// defaultDeadLetterQueueNamePrefix indicates the name of the dead letter queue.
const defaultDeadLetterQueueNamePrefix = "deadletter"

const (
	defaultTableName       = "queue"
	defaultVacuumFrequency = 6 * time.Second // 10x per minute
	vacuumPageSize         = 1000
)

// State indicates the possible states of a message.
type State string

// Acceptable states for messages.
const (
	New        State = "new"
	InProgress State = "in-progress"
	Done       State = "done"
	Dead       State = "dead"
)

// Client uses a postgreSQL database to run a queue system.
type Client struct {
	tableName          string
	pool               Conn
	queueMaxDeliveries int
	keepOnError        bool

	closeOnce sync.Once
	closed    chan struct{}
	cancel    context.CancelFunc

	vacuumTicker       *time.Ticker
	vacuumSingleflight singleflight.Group
}

// ClientOption reconfigures the behavior of the pgqueue Client.
type ClientOption func(*Client)

// WithCustomTable changes the name of the postgresql table used for the queue.
func WithCustomTable(tableName string) ClientOption {
	return func(c *Client) {
		c.tableName = tableName
	}
}

// WithMaxDeliveries indicates how many delivery attempts each message gets. If
// zero, the client retries the message forever.
func WithMaxDeliveries(maxDeliveries int) ClientOption {
	return func(c *Client) {
		c.queueMaxDeliveries = maxDeliveries
	}
}

// DisableAutoVacuum forces the use of manual queue clean up.
func DisableAutoVacuum() ClientOption {
	return func(c *Client) {
		if c.vacuumTicker != nil {
			c.vacuumTicker.Stop()
		}
	}
}

// EnableDeadLetterQueue keeps errored messages for later inspection.
func EnableDeadLetterQueue() ClientOption {
	return func(c *Client) {
		c.keepOnError = true
	}
}

// Open uses the given database connection and start operating the queue system.
func Open(ctx context.Context, pool *pgxpool.Pool, opts ...ClientOption) (*Client, error) {
	ctx, cancel := context.WithCancel(ctx)
	c := &Client{
		tableName: defaultTableName,
		pool:      pool,

		vacuumTicker:       time.NewTicker(defaultVacuumFrequency),
		queueMaxDeliveries: DefaultMaxDeliveriesCount,

		closed: make(chan struct{}),
		cancel: cancel,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.vacuumTicker != nil {
		go c.runAutoVacuum(ctx)
	}
	return c, ctx.Err()
}

func (c *Client) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

func (c *Client) runAutoVacuum(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.vacuumTicker.C:
			c.Vacuum(ctx)
		}
	}
}

// Close stops the queue system.
func (c *Client) Close() error {
	err := ErrAlreadyClosed
	c.closeOnce.Do(func() {
		err = nil
		close(c.closed)
		c.cancel()
	})
	return err
}

// DeadMessage represents one dead message from the queue.
type DeadMessage struct {
	id      uint64
	content []byte
}

// ID returns the unique identifier of the dead message.
func (m *DeadMessage) ID() uint64 {
	return m.id
}

// Content returns the content of the dead message.
func (m *DeadMessage) Content() []byte {
	return m.content
}

// DumpDeadLetterQueue writes the messages into the writer and remove them from
// the database.
func (c *Client) DumpDeadLetterQueue(ctx context.Context, queue string, n int) ([]*DeadMessage, error) {
	if c.isClosed() {
		return nil, ErrAlreadyClosed
	}
	if !c.keepOnError {
		return nil, ErrDeadLetterQueueDisabled
	}
	if n <= 0 {
		return nil, ErrZeroSizedBulkOperation
	}
	var msgs []*DeadMessage
	err := c.connDo(func(conn *nonCancelableConn) error {
		rows, err := conn.Query(ctx, `
			SELECT
				id, content
			FROM
				`+quoteIdentifier(c.tableName)+`
			WHERE
				queue = $1
				AND state = $2
			LIMIT $3
		`, defaultDeadLetterQueueNamePrefix+"-"+queue, Dead, n)
		if err != nil {
			return fmt.Errorf("cannot load dead letter queue messages: %w", err)
		}
		var (
			ids []uint64

			id      uint64
			content []byte
		)
		defer rows.Close()
		for rows.Next() {
			if err := rows.Scan(&id, &content); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("cannot read dead message: %w", err)
			} else if errors.Is(err, pgx.ErrNoRows) {
				return ErrEmptyQueue
			}
			ids = append(ids, id)
			msgs = append(msgs, &DeadMessage{
				id:      id,
				content: content,
			})
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("cannot read dead messages: %w", err)
		}
		if _, err := c.pool.Exec(ctx, `DELETE FROM `+quoteIdentifier(c.tableName)+` WHERE id = ANY($1)`, ids); err != nil {
			return fmt.Errorf("cannot delete dead messages: %w", err)
		}
		return nil
	})
	return msgs, err
}

func (c *Client) renderCreateTable() string {
	return `
CREATE TABLE IF NOT EXISTS ` + quoteIdentifier(c.tableName) + ` (
	id BIGSERIAL PRIMARY KEY,
	queue VARCHAR,
	state VARCHAR,
	deliveries INT NOT NULL DEFAULT 0,
	leased_until TIMESTAMP WITHOUT TIME ZONE,
	content BYTEA
);
CREATE INDEX IF NOT EXISTS ` + quoteIdentifier(c.tableName+"_pop") + ` ON ` + quoteIdentifier(c.tableName) + ` (queue, state);
CREATE INDEX IF NOT EXISTS ` + quoteIdentifier(c.tableName+"_vacuum") + ` ON ` + quoteIdentifier(c.tableName) + ` (queue, state, deliveries, leased_until);
`
}

// CreateTable prepares the underlying table for the queue system.
func (c *Client) CreateTable(ctx context.Context) error {
	if c.isClosed() {
		return ErrAlreadyClosed
	}
	return c.connDo(func(conn *nonCancelableConn) error {
		_, err := conn.Exec(ctx, c.renderCreateTable())
		if err != nil {
			return fmt.Errorf("cannot create table: %w", err)
		}
		return nil
	})
}

// VacuumStats reports the consequences of the clean up.
type VacuumStats struct {
	// LastRun indicates the time of the lastest vacuum cycle.
	LastRun time.Time
	// PageSize indicates how large the vacuum operation was in order to
	// keep it short and non-disruptive.
	PageSize int64

	errClosed error

	// DoneCount indicates how many messages were removed from the queue.
	DoneCount int64
	// ErrDone indicates why the cleaning up of complete messages failed. If
	// nil, it succeeded.
	ErrDone error

	// RestoreStaleCount indicates how many stale messages were restored
	// into the queue.
	RestoreStaleCount int64
	// ErrRestoreStale indicates why the restoration of stale messages
	// failed. If nil, it succeeded.
	ErrRestoreStale error

	// DeadLetterQueueCount indicates how many messages were diverted into
	// deadletter queues.
	DeadLetterQueueCount int64
	// ErrDeadLetterQueue indicates why the move of messages to deadletter
	// queue failed. If nil, it succeeded.
	ErrDeadLetterQueue error

	// BadMessagesDeleteCount indicates how many messages were deleted
	// because they have errored.
	BadMessagesDeleteCount int64
	// ErrBadMessagesDelete indicates why delete errored messages failed. If
	// nil, it succeeded.
	ErrBadMessagesDelete error

	// ErrTableVacuum indicates why the low-level vacuum operation on the
	// table failed.
	ErrTableVacuum error
}

func (vs VacuumStats) String() string {
	var s strings.Builder
	fmt.Fprintf(&s, "LastRun: %s\n", vs.LastRun)
	fmt.Fprintf(&s, "PageSize: %d\n", vs.PageSize)
	fmt.Fprintf(&s, "DoneCount: %d\n", vs.DoneCount)
	fmt.Fprintf(&s, "ErrDone: %v\n", vs.ErrDone)
	fmt.Fprintf(&s, "RestoreStaleCount: %d\n", vs.RestoreStaleCount)
	fmt.Fprintf(&s, "ErrRestoreStale: %v\n", vs.ErrRestoreStale)
	fmt.Fprintf(&s, "DeadLetterQueueCount: %d\n", vs.DeadLetterQueueCount)
	fmt.Fprintf(&s, "ErrDeadLetterQueue: %v\n", vs.ErrDeadLetterQueue)
	fmt.Fprintf(&s, "BadMessagesDeleteCount: %d\n", vs.BadMessagesDeleteCount)
	fmt.Fprintf(&s, "ErrBadMessagesDelete: %v\n", vs.ErrBadMessagesDelete)
	fmt.Fprintf(&s, "ErrTableVacuum: %v\n", vs.ErrTableVacuum)
	return s.String()
}

func (vs VacuumStats) Err() error {
	if vs.errClosed != nil {
		return vs.errClosed
	}

	return errors.Join(
		vs.ErrDone,
		vs.ErrRestoreStale,
		vs.ErrDeadLetterQueue,
		vs.ErrBadMessagesDelete,
		vs.ErrTableVacuum,
	)
}

// Vacuum cleans up the queue from done or dead messages.
func (c *Client) Vacuum(ctx context.Context) VacuumStats {
	if c.isClosed() {
		return VacuumStats{errClosed: ErrAlreadyClosed}
	}
	stats, _, _ := c.vacuumSingleflight.Do("vacuum", func() (interface{}, error) {
		s := c.vacuum(ctx)
		return s, nil
	})
	return stats.(VacuumStats)
}

func (c *Client) vacuum(ctx context.Context) (stats VacuumStats) {
	stats.LastRun = time.Now()
	stats.PageSize = vacuumPageSize
	_ = c.connDo(func(conn *nonCancelableConn) error {
		vacuumCurrentPageSize := vacuumPageSize
		res, err := conn.Exec(ctx, `
			DELETE FROM
				`+quoteIdentifier(c.tableName)+`
			WHERE
				id IN (
					SELECT
						id
					FROM
						`+quoteIdentifier(c.tableName)+`
					WHERE
						state = $1
					LIMIT $2
					FOR UPDATE SKIP LOCKED
				)
		`, Done, vacuumCurrentPageSize)
		if err != nil {
			stats.ErrDone = fmt.Errorf("cannot store message: %w", err)
			return nil
		}
		stats.DoneCount = res.RowsAffected()
		if c.queueMaxDeliveries == 0 {
			return nil
		}
		_, err = conn.Exec(ctx, `
			UPDATE
				`+quoteIdentifier(c.tableName)+`
			SET
				state = $1
			WHERE
				id IN (
					SELECT
						id
					FROM
						`+quoteIdentifier(c.tableName)+`
					WHERE
						state = $2
						AND deliveries < $3
						AND leased_until < NOW()
					LIMIT $4
					FOR UPDATE SKIP LOCKED
				)
		`, New, InProgress, c.queueMaxDeliveries, vacuumCurrentPageSize)
		if err != nil {
			stats.ErrDone = fmt.Errorf("cannot recover messages: %w", err)
			return nil
		}
		stats.DoneCount = res.RowsAffected()
		if !c.keepOnError {
			_, err := conn.Exec(ctx, `
				DELETE FROM
					`+quoteIdentifier(c.tableName)+`
				WHERE
					id IN (
						SELECT
							id
						FROM
							`+quoteIdentifier(c.tableName)+`
						WHERE
							state = $1
							AND deliveries >= $2
							AND leased_until < NOW()
						LIMIT $3
						FOR UPDATE SKIP LOCKED
					)
			`, InProgress, c.queueMaxDeliveries, vacuumCurrentPageSize)
			if err != nil {
				stats.ErrBadMessagesDelete = fmt.Errorf("cannot delete errored message from the queue: %w", err)
				return nil
			}
			stats.BadMessagesDeleteCount = res.RowsAffected()
		} else {
			res, err := conn.Exec(ctx, `
				UPDATE
					`+quoteIdentifier(c.tableName)+`
				SET
					queue = concat('`+defaultDeadLetterQueueNamePrefix+`-', queue),
					state = $1
				WHERE
					id IN (
						SELECT
							id
						FROM
							`+quoteIdentifier(c.tableName)+`
						WHERE
							state = $2
							AND deliveries >= $3
							AND leased_until < NOW()
						LIMIT $4
						FOR UPDATE SKIP LOCKED
					)
			`, Dead, InProgress, c.queueMaxDeliveries, vacuumCurrentPageSize)
			if err != nil {
				stats.ErrDeadLetterQueue = fmt.Errorf("cannot move message to dead letter queue: %w", err)
				return nil
			}
			stats.DeadLetterQueueCount = res.RowsAffected()
		}
		if _, err := c.pool.Exec(ctx, "VACUUM (SKIP_LOCKED true) "+quoteIdentifier(c.tableName)); err != nil {
			stats.ErrTableVacuum = fmt.Errorf("cannot vacuum table %q: %w", c.tableName, err)
			return nil
		}
		return nil
	})
	return stats
}

// ApproximateCount reports how many messages are available in the queue, for
// popping. It will skip messages that are currently being processed or stale.
func (c *Client) ApproximateCount(ctx context.Context, queueName string) (int, error) {
	if c.isClosed() {
		return 0, ErrAlreadyClosed
	}
	var count int
	err := c.connDo(func(conn *nonCancelableConn) error {
		row := conn.QueryRow(ctx, `
			SELECT
				COUNT(id)
			FROM
				`+quoteIdentifier(c.tableName)+`
			WHERE
				queue = $1
				AND state = $2
		`, queueName, New)
		if err := row.Scan(&count); err != nil {
			return fmt.Errorf("cannot count messages: %w", err)
		}
		return nil
	})
	return count, err
}

// Push enqueues the given content to the target queue.
func (c *Client) Push(ctx context.Context, queueName string, content []byte) error {
	return c.PushN(ctx, queueName, [][]byte{content})
}

// Push enqueues the given content batch to the target queue.
func (c *Client) PushN(ctx context.Context, queueName string, contents [][]byte) error {
	if c.isClosed() {
		return ErrAlreadyClosed
	}
	if len(contents) == 0 {
		return ErrZeroSizedBulkOperation
	}
	return c.connDo(func(conn *nonCancelableConn) error {
		_, err := conn.CopyFrom(ctx,
			pgx.Identifier{c.tableName},
			[]string{"queue", "state", "content"},
			pgx.CopyFromSlice(len(contents), func(i int) ([]any, error) {
				return []any{queueName, New, contents[i]}, nil
			}))
		if err != nil {
			return fmt.Errorf("cannot store messages: %w", err)
		}
		return nil
	})
}

// Reserve retrieves the pending message from the queue, if any available. It
// marks it as InProgress until the defined lease duration. If the message is
// not marked as Done by the lease time, it is returned to the queue. Lease
// duration must be multiple of milliseconds.
func (c *Client) Reserve(ctx context.Context, queueName string, lease time.Duration) (*Message, error) {
	msgs, err := c.ReserveN(ctx, queueName, lease, 1)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, ErrEmptyQueue
	}
	return msgs[0], nil
}

// ReserveN retrieves a batch of pending messages from the queue, if any
// available. It marks them as InProgress until the defined lease duration. If
// the message is not marked as Done by the lease time, it is returned to the
// queue. Lease duration must be multiple of milliseconds.
func (c *Client) ReserveN(ctx context.Context, queueName string, lease time.Duration, n int) ([]*Message, error) {
	if c.isClosed() {
		return nil, ErrAlreadyClosed
	}
	if n <= 0 {
		return nil, ErrZeroSizedBulkOperation
	}
	if err := validDuration(lease); err != nil {
		return nil, err
	}
	var msgs []*Message
	err := c.connDo(func(conn *nonCancelableConn) error {
		rows, err := conn.Query(ctx, `
			UPDATE `+quoteIdentifier(c.tableName)+`
			SET
				deliveries = deliveries + 1,
				state = $1,
				leased_until = now() + $2::interval
			WHERE
				id IN (
					SELECT
						id
					FROM
						`+quoteIdentifier(c.tableName)+`
					WHERE
						queue = $3
						AND state = $4
					ORDER BY
						id ASC
					LIMIT `+fmt.Sprint(n)+`
					FOR UPDATE SKIP LOCKED
				)
			RETURNING id, content, leased_until
		`, InProgress, lease.String(), queueName, New)
		if err != nil {
			return fmt.Errorf("cannot reserve messages: %w", err)
		}
		var (
			id          uint64
			content     []byte
			leasedUntil time.Time
		)
		defer rows.Close()
		for rows.Next() {
			if err := rows.Scan(&id, &content, &leasedUntil); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("cannot reserve message: %w", err)
			} else if errors.Is(err, pgx.ErrNoRows) {
				return ErrEmptyQueue
			}
			msgs = append(msgs, &Message{
				id:          id,
				content:     content,
				leasedUntil: leasedUntil,
				client:      c,
			})
		}
		return rows.Err()
	})
	return msgs, err

}

// Release puts one message back to the queue.
func (c *Client) Release(ctx context.Context, id uint64) error {
	return c.ReleaseN(ctx, []uint64{id})
}

// ReleaseN puts the messages back to the queue.
func (c *Client) ReleaseN(ctx context.Context, ids []uint64) error {
	if c.isClosed() {
		return ErrAlreadyClosed
	}
	return c.connDo(func(conn *nonCancelableConn) error {
		result, err := conn.Exec(ctx, `
			UPDATE
				`+quoteIdentifier(c.tableName)+`
			SET
				leased_until = null,
				state = $1
			WHERE
				id IN (
					SELECT
						id
					FROM
						`+quoteIdentifier(c.tableName)+`
					WHERE
						id = ANY($2)
						AND state = $3
						AND leased_until >= NOW()
					FOR UPDATE NOWAIT
				)
		`, New, ids, InProgress)
		if err != nil {
			return err
		}
		affectedRows := result.RowsAffected()
		if affectedRows != int64(len(ids)) {
			return ErrReleaseIncomplete
		}
		return nil
	})
}

// Extend extends the message lease by the given duration. The duration must be
// multiples of milliseconds.
func (c *Client) Extend(ctx context.Context, id uint64, extension time.Duration) error {
	return c.ExtendN(ctx, []uint64{id}, extension)
}

// ExtendN extends the messages lease by the given duration. The duration must
// be multiples of milliseconds.
func (c *Client) ExtendN(ctx context.Context, ids []uint64, extension time.Duration) error {
	if c.isClosed() {
		return ErrAlreadyClosed
	}
	if err := validDuration(extension); err != nil {
		return err
	}
	return c.connDo(func(conn *nonCancelableConn) error {
		_, err := conn.Exec(ctx, `
			UPDATE
				`+quoteIdentifier(c.tableName)+`
			SET
				leased_until = now() + $1::interval
			WHERE
				id IN (
					SELECT
						id
					FROM
						`+quoteIdentifier(c.tableName)+`
					WHERE
						id = ANY($2)
						AND leased_until >= NOW()
					FOR UPDATE NOWAIT
				)
		`, extension.String(), ids)
		if err != nil {
			return err
		}
		return nil
	})
}

// Pop retrieves the pending message from the queue, if any available. If the
// queue is empty, it returns ErrEmptyQueue.
func (c *Client) Pop(ctx context.Context, queueName string) ([]byte, error) {
	msgs, err := c.PopN(ctx, queueName, 1)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, ErrEmptyQueue
	}
	return msgs[0], nil
}

// Pop retrieves a batch pending message from the queue, if any available. If
// the queue is empty, it returns ErrEmptyQueue.
func (c *Client) PopN(ctx context.Context, queueName string, n int) ([][]byte, error) {
	if c.isClosed() {
		return nil, ErrAlreadyClosed
	}
	if n <= 0 {
		return nil, ErrZeroSizedBulkOperation
	}
	var contents [][]byte
	err := c.connDo(func(conn *nonCancelableConn) error {
		rows, err := conn.Query(ctx, `
			UPDATE `+quoteIdentifier(c.tableName)+`
			SET
				deliveries = deliveries + 1,
				state = $1
			WHERE
				id IN (
					SELECT
						id
					FROM
						`+quoteIdentifier(c.tableName)+`
					WHERE
						queue = $2
						AND state = $3
					ORDER BY
						id ASC
					LIMIT `+fmt.Sprint(n)+`
					FOR UPDATE SKIP LOCKED
				)
			RETURNING content
		`, Done, queueName, New)
		if err != nil {
			return fmt.Errorf("cannot pop messages: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var content []byte
			if err := rows.Scan(&content); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("cannot pop message: %w", err)
			} else if errors.Is(err, pgx.ErrNoRows) {
				return ErrEmptyQueue
			}
			contents = append(contents, content)
		}
		return rows.Err()
	})
	return contents, err
}

// Delete removes the message from the queue.
func (c *Client) Delete(ctx context.Context, id uint64) error {
	return c.DeleteN(ctx, []uint64{id})
}

// DeleteN removes the messages from the queue.
func (c *Client) DeleteN(ctx context.Context, ids []uint64) error {
	if c.isClosed() {
		return ErrAlreadyClosed
	}
	return c.connDo(func(conn *nonCancelableConn) error {
		_, err := conn.Exec(ctx, `
			DELETE FROM
				`+quoteIdentifier(c.tableName)+`
			WHERE
				id = ANY($1)
		`, ids)
		if err != nil {
			return fmt.Errorf("cannot delete messages: %w", err)
		}
		return nil
	})
}

// Message represents one message from the queue.
type Message struct {
	id          uint64
	content     []byte
	leasedUntil time.Time

	client *Client
}

// ID returns the unique identifier of the message.
func (m *Message) ID() uint64 {
	return m.id
}

// Content returns the content of the message.
func (m *Message) Content() []byte {
	return m.content
}

// LeaseDeadline returns the time when the lease is going to expire.
func (m *Message) LeaseDeadline() time.Time {
	return m.leasedUntil
}

func validDuration(d time.Duration) error {
	valid := d > time.Millisecond && d%time.Millisecond == 0
	if !valid {
		return ErrInvalidDuration
	}
	return nil
}
