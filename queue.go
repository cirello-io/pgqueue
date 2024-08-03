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
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"slices"
	"sync"
	"time"

	"cirello.io/pidctl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// ErrMessageExpired indicates the message deadline has been reached and the
// current message pointer can no longer be used to update it.
var ErrMessageExpired = errors.New("message expired")

// ErrDeadletterQueueDisabled indicates that is not possible to dump messages
// from the target deadletter queue because its support has been disabled.
var ErrDeadletterQueueDisabled = errors.New("deadletter queue disabled")

// DefaultMaxDeliveriesCount is how many delivery attempt each message gets
// before getting skipped on Pop and Reserve calls.
const DefaultMaxDeliveriesCount = 5

// DefaultDeadLetterQueueNamePrefix indicates the name of the dead letter queue.
const DefaultDeadLetterQueueNamePrefix = "deadletter"

// reasonable defaults
const (
	defaultTableName       = "queue"
	defaultVacuumFrequency = 6 * time.Second // 10x per minute
)

// vacuum settings
const (
	vacuumPIDControllerFakeCycle = time.Nanosecond
	vacuumInitialPageSize        = 1000
)

// State indicates the possible states of a message
type State string

// Acceptable states for messages.
const (
	New        State = "new"
	InProgress State = "in-progress"
	Done       State = "done"
)

// Client uses a postgreSQL database to run a queue system.
type Client struct {
	tableName          string
	pool               *pgxpool.Pool
	listener           *pgxpool.Conn
	queueMaxDeliveries int
	keepOnError        bool

	closeOnce sync.Once
	cancel    context.CancelFunc

	mu            sync.RWMutex
	subscriptions map[chan struct{}]string
	knownQueues   []*Queue

	vacuumTicker          *time.Ticker
	vacuumSingleflight    singleflight.Group
	vacuumPID             pidctl.Controller
	vacuumCurrentPageSize int64

	seqName string
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

// EnableDeadletterQueue keeps errored messages for later inspection.
func EnableDeadletterQueue() ClientOption {
	return func(c *Client) {
		c.keepOnError = true
	}
}

// Open uses the given database connection and start operating the queue system.
func Open(ctx context.Context, pool *pgxpool.Pool, opts ...ClientOption) (*Client, error) {
	ctx, cancel := context.WithCancel(ctx)
	c := &Client{
		tableName:     defaultTableName,
		pool:          pool,
		subscriptions: make(map[chan struct{}]string),

		vacuumTicker:       time.NewTicker(defaultVacuumFrequency),
		queueMaxDeliveries: DefaultMaxDeliveriesCount,
		vacuumPID: pidctl.Controller{
			// each adjustment step must be +/- 500 rows
			P:   big.NewRat(500, 1),
			I:   big.NewRat(3, 1),
			D:   big.NewRat(3, 1),
			Min: big.NewRat(-500, 1),
			Max: big.NewRat(+500, 1),
			// 1s every 6s cycle.
			// 10 cycles per minute.
			Setpoint: big.NewRat(1, 1),
		},
		vacuumCurrentPageSize: vacuumInitialPageSize,

		cancel: cancel,
	}
	for _, opt := range opts {
		opt(c)
	}
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot acquire connection: %w", err)
	}
	defer conn.Release()
	seqName, err := conn.Conn().PgConn().EscapeString(c.tableName + "_rvn")
	if err != nil {
		return nil, fmt.Errorf("cannot escape sequence name: %w", err)
	}
	c.seqName = `'` + seqName + `'`
	if err := c.setupListener(ctx); err != nil {
		return nil, err
	}
	go c.forwardNotifications(ctx)
	if c.vacuumTicker != nil {
		go c.runAutoVacuum(ctx)
	}
	return c, ctx.Err()
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

func (c *Client) add(q *Queue) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.knownQueues = append(c.knownQueues, q)
}

func (c *Client) remove(q *Queue) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var knownQueues []*Queue
	for _, knownQueue := range c.knownQueues {
		if knownQueue != q {
			knownQueues = append(knownQueues, q)
		}
	}
	c.knownQueues = knownQueues
}

func (c *Client) subscribe(sub chan struct{}, queue string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subscriptions[sub] = queue
}

func (c *Client) unsubscribe(sub chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subscriptions, sub)
}

func (c *Client) setupListener(ctx context.Context) error {
	listenerConn, err := c.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("cannot acquire listener connection: %w", err)
	}
	if _, err := listenerConn.Exec(ctx, "LISTEN "+quoteIdentifier(c.tableName)); err != nil {
		return fmt.Errorf("cannot subscribe for notifications: %w", err)
	}
	c.listener = listenerConn
	return nil
}

func (c *Client) forwardNotifications(ctx context.Context) {
	defer c.listener.Release()
	for {
		if ctx.Err() != nil {
			return
		}
		notification, err := c.listener.Conn().WaitForNotification(ctx)
		if err != nil {
			return
		}
		c.mu.RLock()
		for ch, queue := range c.subscriptions {
			dispatch := notification.Payload == queue
			if !dispatch {
				continue
			}
			select {
			case ch <- struct{}{}:
			default:
			}
		}
		c.mu.RUnlock()
	}
}

// Close stops the queue system.
func (c *Client) Close() error {
	err := ErrAlreadyClosed
	c.closeOnce.Do(func() {
		err = nil
		c.cancel()
		c.pool.Close()
	})
	return err
}

// Queue configures a queue.
func (c *Client) Queue(queue string) *Queue {
	q := &Queue{
		client:      c,
		queue:       queue,
		closed:      make(chan struct{}),
		keepOnError: c.keepOnError,
	}
	c.add(q)
	return q
}

// DumpDeadLetterQueue writes the messages into the writer and remove them from
// the database.
func (c *Client) DumpDeadLetterQueue(ctx context.Context, queue string, w io.Writer) error {
	if !c.keepOnError {
		return ErrDeadletterQueueDisabled
	}

	return c.acquireConnDo(ctx, func(conn *nonCancelableConn) error {
		rows, err := conn.Query(ctx, `
			SELECT
				id, content
			FROM
				`+quoteIdentifier(c.tableName)+`
			WHERE
				queue = $1
		`, DefaultDeadLetterQueueNamePrefix+"-"+queue)
		if err != nil {
			return fmt.Errorf("cannot load dead letter queue messages: %w", err)
		}
		defer rows.Close()
		enc := json.NewEncoder(w)
		for rows.Next() {
			var row struct {
				ID      uint64 `json:"id"`
				Content []byte `json:"content"`
			}
			if err := rows.Scan(&row.ID, &row.Content); err != nil {
				return fmt.Errorf("cannot parse message row: %w", err)
			}
			if err := enc.Encode(row); err != nil {
				return fmt.Errorf("cannot flush message row: %w", err)
			}
			if _, err := c.pool.Exec(ctx, `DELETE FROM `+quoteIdentifier(c.tableName)+` WHERE id = $1`, row.ID); err != nil {
				return fmt.Errorf("cannot delete flushed message: %w", err)
			}
		}
		return rows.Err()
	})
}

// CreateTable prepares the underlying table for the queue system.
func (c *Client) CreateTable(ctx context.Context) error {
	return c.acquireConnDo(ctx, func(conn *nonCancelableConn) error {
		_, err := conn.Exec(ctx, `
			CREATE SEQUENCE IF NOT EXISTS `+quoteIdentifier(c.tableName+"_rvn")+` AS BIGINT CYCLE;
			CREATE TABLE IF NOT EXISTS `+quoteIdentifier(c.tableName)+` (
				id SERIAL PRIMARY KEY,
				rvn BIGINT DEFAULT nextval(`+c.seqName+`),
				queue VARCHAR,
				state VARCHAR,
				deliveries INT NOT NULL DEFAULT 0,
				leased_until TIMESTAMP WITHOUT TIME ZONE,
				content BYTEA
			);
			CREATE INDEX IF NOT EXISTS `+quoteIdentifier(c.tableName+"_pop")+` ON `+quoteIdentifier(c.tableName)+` (queue, state);
			CREATE INDEX IF NOT EXISTS `+quoteIdentifier(c.tableName+"_vacuum")+` ON `+quoteIdentifier(c.tableName)+` (queue, state, deliveries, leased_until);
		`)
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
	// Err indicates why the vacuum cycle failed. If nil, it succeeded.
	Err error
}

// Vacuum cleans up the queue from done or dead messages.
func (c *Client) Vacuum(ctx context.Context) {
	c.vacuumSingleflight.Do("vacuum", func() (interface{}, error) {
		start := time.Now()
		c.mu.RLock()
		knownQueues := slices.Clone(c.knownQueues)
		c.mu.RUnlock()
		for _, q := range knownQueues {
			s := c.vacuum(ctx, q)
			q.vacuumStatsMu.Lock()
			q.vacuumStats = s
			q.vacuumStats.LastRun = start
			q.vacuumStats.PageSize = c.vacuumCurrentPageSize
			q.vacuumStatsMu.Unlock()
		}
		duration, _ := big.NewFloat(time.Since(start).Seconds()).Rat(nil)
		acc := c.vacuumPID.Accumulate(duration, vacuumPIDControllerFakeCycle)
		pageSizeBigInt, _ := big.NewFloat(0).SetRat(acc).Int(nil)
		c.vacuumCurrentPageSize += pageSizeBigInt.Int64()
		return nil, nil
	})
}

func (c *Client) vacuum(ctx context.Context, q *Queue) (stats VacuumStats) {
	err := c.acquireConnDo(ctx, func(conn *nonCancelableConn) error {
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
						queue = $1
						AND state = $2
					LIMIT CASE WHEN $3 < 0 THEN 0 ELSE $3 END
					FOR UPDATE SKIP LOCKED
				)
		`, q.queue, Done, c.vacuumCurrentPageSize)
		if err != nil {
			return fmt.Errorf("cannot store message: %w", err)
		}
		if c.queueMaxDeliveries == 0 {
			return nil
		}
		_, err = conn.Exec(ctx, `
			UPDATE
				`+quoteIdentifier(c.tableName)+`
			SET
				rvn = nextval(`+c.seqName+`),
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
						AND deliveries < $4
						AND leased_until < NOW()
					LIMIT CASE WHEN $5 < 0 THEN 0 ELSE $5 END
					FOR UPDATE SKIP LOCKED
				)
		`, New, q.queue, InProgress, c.queueMaxDeliveries, c.vacuumCurrentPageSize)
		if err != nil {
			return fmt.Errorf("cannot recover messages: %w", err)
		}
		if !q.keepOnError {
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
							queue = $1
							AND state = $2
							AND deliveries >= $3
							AND leased_until < NOW()
						LIMIT CASE WHEN $4 < 0 THEN 0 ELSE $4 END
						FOR UPDATE SKIP LOCKED
					)
			`, q.queue, InProgress, c.queueMaxDeliveries, c.vacuumCurrentPageSize)
			if err != nil {
				return fmt.Errorf("cannot delete errored message from the queue: %w", err)
			}
		} else {
			_, err := conn.Exec(ctx, `
				UPDATE
					`+quoteIdentifier(c.tableName)+`
				SET
					rvn = nextval(`+c.seqName+`),
					queue = $1
				WHERE
					id IN (
						SELECT
							id
						FROM
							`+quoteIdentifier(c.tableName)+`
						WHERE
							queue = $2
							AND state = $3
							AND deliveries >= $4
							AND leased_until < NOW()
						LIMIT CASE WHEN $5 < 0 THEN 0 ELSE $5 END
						FOR UPDATE SKIP LOCKED
					)
			`, DefaultDeadLetterQueueNamePrefix+"-"+q.queue, q.queue, InProgress, c.queueMaxDeliveries, c.vacuumCurrentPageSize)
			if err != nil {
				return fmt.Errorf("cannot move message to dead letter queue: %w", err)
			}
		}
		if _, err := c.pool.Exec(ctx, "VACUUM (SKIP_LOCKED true) "+quoteIdentifier(c.tableName)); err != nil {
			return fmt.Errorf("cannot vacuum table %q: %w", c.tableName, err)
		}
		return nil
	})
	if err != nil {
		stats.Err = err
	}
	return stats
}

// Queue holds the configuration definition for one queue.
type Queue struct {
	client *Client

	queue     string
	closeOnce sync.Once
	closed    chan struct{}

	vacuumStatsMu sync.RWMutex
	vacuumStats   VacuumStats

	keepOnError bool
}

// VacuumStats reports the result of the last vacuum cycle.
func (q *Queue) VacuumStats() VacuumStats {
	q.vacuumStatsMu.RLock()
	defer q.vacuumStatsMu.RUnlock()
	return q.vacuumStats
}

// Watch observes new messages for the target queue.
func (q *Queue) Watch(lease time.Duration) *Watcher {
	w := &Watcher{
		queue:         q,
		notifications: make(chan struct{}, 1),
		lease:         lease,
	}
	q.client.subscribe(w.notifications, q.queue)
	return w
}

// ApproximateCount reports how many messages are available in the queue, for
// popping. It will skip messages that are currently being processed or stale.
func (q *Queue) ApproximateCount(ctx context.Context) (int, error) {
	if q.isClosed() {
		return 0, ErrAlreadyClosed
	}
	var count int
	err := q.client.acquireConnDo(ctx, func(conn *nonCancelableConn) error {
		row := conn.QueryRow(ctx, `
			SELECT
				COUNT(*)
			FROM
				`+quoteIdentifier(q.client.tableName)+`
			WHERE
				queue = $1
				AND state = $2
		`, q.queue, New)
		if err := row.Scan(&count); err != nil {
			return fmt.Errorf("cannot count messages: %w", err)
		}
		return nil
	})
	return count, err
}

// Reserve retrieves the pending message from the queue, if any available. It
// marks as it as InProgress until the defined lease duration. If the message
// is not marked as Done by the lease time, it is returned to the queue. Lease
// duration must be multiple of milliseconds.
func (q *Queue) Reserve(ctx context.Context, lease time.Duration) (*Message, error) {
	if q.isClosed() {
		return nil, ErrAlreadyClosed
	}
	if err := validDuration(lease); err != nil {
		return nil, err
	}
	var msg *Message
	err := q.client.acquireConnDo(ctx, func(conn *nonCancelableConn) error {
		row := conn.QueryRow(ctx, `
			UPDATE `+quoteIdentifier(q.client.tableName)+`
			SET
				rvn = nextval(`+q.client.seqName+`),
				deliveries = deliveries + 1,
				state = $1,
				leased_until = now() + $2::interval
			WHERE
				id IN (
					SELECT
						id
					FROM
						`+quoteIdentifier(q.client.tableName)+`
					WHERE
						queue = $3
						AND state = $4
					ORDER BY
						id ASC
					LIMIT 1
					FOR UPDATE SKIP LOCKED
				)
			RETURNING id, content, leased_until, rvn
		`, InProgress, lease.String(), q.queue, New)
		var (
			id          uint64
			content     []byte
			leasedUntil time.Time
			rvn         int64
		)
		if err := row.Scan(&id, &content, &leasedUntil, &rvn); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("cannot reserve message: %w", err)
		} else if errors.Is(err, pgx.ErrNoRows) {
			return ErrEmptyQueue
		}
		msg = &Message{
			id:          id,
			Content:     content,
			LeasedUntil: leasedUntil,
			client:      q.client,
			rvn:         rvn,
		}
		return nil
	})
	return msg, err

}

// Push enqueues the given content to the target queue.
func (q *Queue) Push(ctx context.Context, content []byte) error {
	if q.isClosed() {
		return ErrAlreadyClosed
	}
	return q.client.acquireConnDo(ctx, func(conn *nonCancelableConn) error {
		queueName, err := conn.EscapeString(q.queue)
		if err != nil {
			return fmt.Errorf("cannot escape queue name: %w", err)
		}
		if _, err := conn.Exec(ctx, `INSERT INTO `+quoteIdentifier(q.client.tableName)+` (queue, state, content) VALUES ($1, $2, $3)`, q.queue, New, content); err != nil {
			return fmt.Errorf("cannot store message: %w", err)
		}
		if _, err := conn.Exec(ctx, `NOTIFY `+quoteIdentifier(q.client.tableName)+`, '`+queueName+`'`); err != nil {
			return fmt.Errorf("cannot send push notification: %w", err)
		}
		return nil
	})
}

// Pop retrieves the pending message from the queue, if any available. If the
// queue is empty, it returns ErrEmptyQueue.
func (q *Queue) Pop(ctx context.Context) ([]byte, error) {
	if q.isClosed() {
		return nil, ErrAlreadyClosed
	}
	var content []byte
	err := q.client.acquireConnDo(ctx, func(conn *nonCancelableConn) error {
		row := conn.QueryRow(ctx, `
			UPDATE `+quoteIdentifier(q.client.tableName)+`
			SET
				rvn = nextval(`+q.client.seqName+`),
				deliveries = deliveries + 1,
				state = $1
			WHERE
				id IN (
					SELECT
						id
					FROM
						`+quoteIdentifier(q.client.tableName)+`
					WHERE
						queue = $2
						AND state = $3
					ORDER BY
						id ASC
					LIMIT 1
					FOR UPDATE SKIP LOCKED
				)
			RETURNING content
		`, Done, q.queue, New)
		if err := row.Scan(&content); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("cannot pop message: %w", err)
		} else if errors.Is(err, pgx.ErrNoRows) {
			return ErrEmptyQueue
		}
		return nil
	})
	return content, err
}

// Close closes the queue.
func (q *Queue) Close() error {
	err := ErrAlreadyClosed
	q.closeOnce.Do(func() {
		close(q.closed)
		err = nil
		q.client.remove(q)
	})
	return err
}

func (q *Queue) isClosed() bool {
	select {
	case <-q.closed:
		return true
	default:
		return false
	}
}

// Watcher holds the pointer necessary to listen for postgreSQL events that
// indicates a new message has arrive in the pipe.
type Watcher struct {
	queue         *Queue
	notifications chan struct{}
	lease         time.Duration
	msg           *Message
	err           error
}

// how frequently the next call is going to ping the database if nothing
// comes from the notification channel.
const missedNotificationFrequency = 500 * time.Millisecond

// Next waits for the next message to arrive and store it into Watcher.
func (w *Watcher) Next(ctx context.Context) bool {
	unsub := func() {
		w.queue.client.unsubscribe(w.notifications)
	}
	if w.err != nil {
		unsub()
		return false
	}
	if w.queue.isClosed() {
		w.err = ErrAlreadyClosed
		unsub()
		return false
	}
	tick := time.NewTicker(missedNotificationFrequency)
	defer tick.Stop()
	for {
		switch msg, err := w.queue.Reserve(ctx, w.lease); err {
		case ErrEmptyQueue:
		case ErrAlreadyClosed:
			w.err = err
			unsub()
			return false
		default:
			w.msg = msg
			w.err = err
			return err == nil
		}
		select {
		case <-w.notifications:
		case <-tick.C:
		case <-w.queue.closed:
			w.err = ErrAlreadyClosed
			unsub()
			return false
		}
	}
}

// Message returns the current message store in the Watcher.
func (w *Watcher) Message() *Message {
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
	rvn         int64
	client      *Client
}

// Done mark message as done.
func (m *Message) Done(ctx context.Context) error {
	return m.client.acquireConnDo(ctx, func(conn *nonCancelableConn) error {
		result, err := conn.Exec(ctx, `
			UPDATE
				`+quoteIdentifier(m.client.tableName)+`
			SET
				rvn = nextval(`+m.client.seqName+`),
				state = $1
			WHERE
				id IN (
					SELECT
						id
					FROM
						`+quoteIdentifier(m.client.tableName)+`
					WHERE
						id = $2
						AND rvn = $3
						AND leased_until >= NOW()
					FOR UPDATE NOWAIT
				)
		`, Done, m.id, m.rvn)
		if err != nil {
			return err
		}
		affectedRows := result.RowsAffected()
		if affectedRows == 0 {
			return ErrMessageExpired
		}
		return nil
	})
}

// Release put the message back to the queue.
func (m *Message) Release(ctx context.Context) error {
	return m.client.acquireConnDo(ctx, func(conn *nonCancelableConn) error {
		result, err := conn.Exec(ctx, `
			UPDATE
				`+quoteIdentifier(m.client.tableName)+`
			SET
				rvn = nextval(`+m.client.seqName+`), leased_until = null, state = $1
			WHERE
				id IN (
					SELECT
						id
					FROM
						`+quoteIdentifier(m.client.tableName)+`
					WHERE
						id = $2
						AND rvn = $3
						AND leased_until >= NOW()
					FOR UPDATE NOWAIT
				)
		`, New, m.id, m.rvn)
		if err != nil {
			return err
		}
		affectedRows := result.RowsAffected()
		if affectedRows == 0 {
			return ErrMessageExpired
		}
		return nil
	})
}

// Touch extends the lease by the given duration. The duration must be multiples
// of milliseconds.
func (m *Message) Touch(ctx context.Context, extension time.Duration) error {
	if err := validDuration(extension); err != nil {
		return err
	}
	return m.client.acquireConnDo(ctx, func(conn *nonCancelableConn) error {
		row := conn.QueryRow(ctx, `
			UPDATE
				`+quoteIdentifier(m.client.tableName)+`
			SET
				rvn = nextval(`+m.client.seqName+`),
				leased_until = now() + $1::interval
			WHERE
				id IN (
					SELECT
						id
					FROM
						`+quoteIdentifier(m.client.tableName)+`
					WHERE
						id = $2
						AND rvn = $3
						AND leased_until >= NOW()
					FOR UPDATE NOWAIT
				)
			RETURNING rvn, leased_until
		`, extension.String(), m.id, m.rvn)
		if err := row.Scan(&m.rvn, &m.LeasedUntil); err == sql.ErrNoRows {
			return ErrMessageExpired
		} else if err != nil {
			return err
		}
		return nil
	})
}

func validDuration(d time.Duration) error {
	valid := d > time.Millisecond && d%time.Millisecond == 0
	if !valid {
		return ErrInvalidDuration
	}
	return nil
}

func quoteIdentifier(s string) string {
	return (pgx.Identifier{s}).Sanitize()
}

func (c *Client) acquireConnDo(ctx context.Context, f func(*nonCancelableConn) error) error {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("cannot acquire connection: %w", err)
	}
	defer conn.Release()
	return f(&nonCancelableConn{conn})
}

type nonCancelableConn struct {
	conn *pgxpool.Conn
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

func (c *nonCancelableConn) EscapeString(s string) (string, error) {
	return c.conn.Conn().PgConn().EscapeString(s)
}
