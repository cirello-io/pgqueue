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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sync"
	"time"

	"cirello.io/pidctl"
	"github.com/lib/pq"
	"golang.org/x/sync/singleflight"
)

// ErrEmptyQueue indicates there isn't any message available at the head of the
// queue.
var ErrEmptyQueue = fmt.Errorf("empty queue")

// ErrMessageTooLarge indicates the content to be pushed is too large.
var ErrMessageTooLarge = fmt.Errorf("message is too large")

// ErrAlreadyClosed indicates the queue is closed and all its watchers are going
// to report the queue is no longer available.
var ErrAlreadyClosed = errors.New("queue is already closed")

// ErrInvalidDuration indicates the duration used is too small. It must larger
// than a millisecond and be multiple of a millisecond.
var ErrInvalidDuration = errors.New("invalid duration")

// MaxMessageLength indicates the maximum content length acceptable for new
// messages. Although it is theoretically possible to use large messages, the
// idea here is to be conservative until the properties of PostgreSQL are fully
// mapped.
const MaxMessageLength = 65536

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
	db                 *sql.DB
	listener           *pq.Listener
	queueMaxDeliveries int

	closeOnce sync.Once
	closed    chan struct{}

	mu            sync.RWMutex
	subscriptions map[chan struct{}]string
	knownQueues   []*Queue

	vacuumTicker          *time.Ticker
	vacuumSingleflight    singleflight.Group
	vacuumPID             pidctl.Controller
	vacuumCurrentPageSize int64

	muVacuumStats sync.RWMutex
	vacuumStats   VacuumStats
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

// Open uses the given database connection and start operating the queue system.
func Open(dsn string, opts ...ClientOption) (*Client, error) {
	connector, err := pq.NewConnector(dsn)
	if err != nil {
		return nil, fmt.Errorf("bad DSN: %w", err)
	}
	db := sql.OpenDB(connector)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("cannot open database: %w", err)
	}
	listener := pq.NewListener(dsn, 1*time.Millisecond, 1*time.Millisecond, func(t pq.ListenerEventType, err error) {})

	c := &Client{
		tableName:     defaultTableName,
		db:            db,
		listener:      listener,
		subscriptions: make(map[chan struct{}]string),

		vacuumTicker:       time.NewTicker(defaultVacuumFrequency),
		queueMaxDeliveries: DefaultMaxDeliveriesCount,
		vacuumPID: pidctl.Controller{
			// each adjusment step must be +/- 500 rows
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

		closed: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}
	if err := listener.Listen(c.tableName); err != nil {
		return nil, fmt.Errorf("cannot subscribe for notifications: %w", err)
	}
	go c.forwardNotifications()
	if c.vacuumTicker != nil {
		go c.runAutoVacuum()
	}
	return c, nil
}

func (c *Client) runAutoVacuum() {
	for {
		select {
		case <-c.closed:
			return
		case <-c.vacuumTicker.C:
			stats := c.Vacuum()
			c.muVacuumStats.Lock()
			c.vacuumStats.PageSize = stats.PageSize
			c.vacuumStats.Done += stats.Done
			c.vacuumStats.Recovered += stats.Recovered
			c.vacuumStats.Dead += stats.Dead
			c.vacuumStats.Err = stats.Err
			c.muVacuumStats.Unlock()
		}
	}
}

// VacuumStats reports the result of the last vacuum cycle.
func (c *Client) VacuumStats() VacuumStats {
	c.muVacuumStats.RLock()
	defer c.muVacuumStats.RUnlock()
	return c.vacuumStats
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

func (c *Client) forwardNotifications() {
	for n := range c.listener.NotificationChannel() {
		c.mu.RLock()
		for ch, queue := range c.subscriptions {
			dispatch := n == nil || n.Extra == queue
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

// ClientCloseError reports all the errors that happened during client close.
type ClientCloseError struct {
	ListenerError error
	DriverError   error
}

// Is detects if the given target matches either the listener or the driver
// error.
func (e *ClientCloseError) Is(target error) bool {
	return errors.Is(e.ListenerError, target) || errors.Is(e.DriverError, target)
}

func (e *ClientCloseError) Error() string {
	return fmt.Sprintf("listener: %v | driver: %v", e.ListenerError, e.DriverError)
}

// Close stops the queue system.
func (c *Client) Close() error {
	err := ErrAlreadyClosed
	c.closeOnce.Do(func() {
		err = nil
		close(c.closed)
		listenerErr := c.listener.Close()
		driverErr := c.db.Close()
		if listenerErr != nil || driverErr != nil {
			err = &ClientCloseError{
				ListenerError: listenerErr,
				DriverError:   driverErr,
			}
		}
	})
	return err
}

// Queue configures a queue.
func (c *Client) Queue(queue string) *Queue {
	q := &Queue{
		client: c,
		queue:  queue,
		closed: make(chan struct{}),
	}
	c.add(q)
	return q
}

// DumpDeadLetterQueue writes the messages into the writer and remove them from
// the database.
func (c *Client) DumpDeadLetterQueue(queue string, w io.Writer) error {
	rows, err := c.db.Query(`
		SELECT
			id, content
		FROM
			`+pq.QuoteIdentifier(c.tableName)+`
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
		if _, err := c.db.Exec(`DELETE FROM `+pq.QuoteIdentifier(c.tableName)+` WHERE id = $1`, row.ID); err != nil {
			return fmt.Errorf("cannot delete flushed message: %w", err)
		}
	}
	return nil
}

func (c *Client) retry(f func(*sql.Tx) error) error {
	const serializationErrorCode = "40001"
	var err error
	for {
		err = func() error {
			tx, err := c.db.BeginTx(context.Background(), &sql.TxOptions{
				Isolation: sql.LevelSerializable,
			})
			if err != nil {
				return fmt.Errorf("cannot start transaction: %w", err)
			}
			defer tx.Rollback()
			if err := f(tx); err != nil {
				return err
			}
			return tx.Commit()
		}()
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
func (c *Client) CreateTable() error {
	return c.retry(func(tx *sql.Tx) error {
		_, err := tx.Exec(`
		CREATE SEQUENCE IF NOT EXISTS ` + pq.QuoteIdentifier(c.tableName+"_rvn") + ` AS BIGINT CYCLE;
		CREATE TABLE IF NOT EXISTS ` + pq.QuoteIdentifier(c.tableName) + ` (
			id SERIAL PRIMARY KEY,
			rvn BIGINT DEFAULT nextval(` + pq.QuoteLiteral(c.tableName+"_rvn") + `),
			queue VARCHAR,
			state VARCHAR,
			deliveries INT NOT NULL DEFAULT 0,
			leased_until TIMESTAMP WITHOUT TIME ZONE,
			content BYTEA
		);
		CREATE INDEX IF NOT EXISTS ` + pq.QuoteIdentifier(c.tableName+"_pop") + ` ON ` + pq.QuoteIdentifier(c.tableName) + ` (queue, state);
		CREATE INDEX IF NOT EXISTS ` + pq.QuoteIdentifier(c.tableName+"_vacuum") + ` ON ` + pq.QuoteIdentifier(c.tableName) + ` (queue, state, deliveries, leased_until);
		`)
		return err
	})
}

// VacuumStats reports the consequences of the clean up.
type VacuumStats struct {
	// PageSize indicates how large the vacuum operation was in order to
	// keep it short and non-disruptive.
	PageSize int64
	// Done reports how many messages marked as Done were deleted.
	Done int64
	// Recovered reports how many expired messages marked as InProgress but
	// with low delivery count were recovered.
	Recovered int64
	// Dead reports how many expired messages marked as InProgress but with
	// high delivery count were moved to the deadletter queue.
	Dead int64
	// Err indicates why the vacuum cycle failed. If nil, it succeeded.
	Err error
}

// Vacuum cleans up the queue from done or dead messages.
func (c *Client) Vacuum() VacuumStats {
	v, err, _ := c.vacuumSingleflight.Do("vacuum", func() (interface{}, error) {
		var stats VacuumStats
		stats.PageSize = c.vacuumCurrentPageSize
		start := time.Now()
		for _, q := range c.knownQueues {
			s, err := c.vacuum(q)
			stats.Done += s.Done
			stats.Recovered += s.Recovered
			stats.Dead += s.Dead
			if stats.Err == nil && err != nil {
				stats.Err = fmt.Errorf("vacuum error for %s: %w", q.queue, err)
			}
		}
		duration, _ := big.NewFloat(time.Since(start).Seconds()).Rat(nil)
		acc := c.vacuumPID.Accumulate(duration, vacuumPIDControllerFakeCycle)
		pageSizeBigInt, _ := big.NewFloat(0).SetRat(acc).Int(nil)
		c.vacuumCurrentPageSize += pageSizeBigInt.Int64()
		return stats, stats.Err
	})
	stats := v.(VacuumStats)
	stats.Err = err
	return stats
}

func (c *Client) vacuum(q *Queue) (stats VacuumStats, err error) {
	if q.isClosed() {
		return stats, ErrAlreadyClosed
	}
	err = c.retry(func(tx *sql.Tx) (err error) {
		stats = VacuumStats{}
		res, err := tx.Exec(`
			DELETE FROM
				`+pq.QuoteIdentifier(c.tableName)+`
			WHERE
				id IN (
					SELECT
						id
					FROM
						`+pq.QuoteIdentifier(c.tableName)+`
					WHERE
						queue = $1
						AND state = $2
					LIMIT CASE WHEN $3 < 0 THEN 0 ELSE $3 END
				)
			`, q.queue, Done, c.vacuumCurrentPageSize)
		if err != nil {
			return fmt.Errorf("cannot store message: %w", err)
		}
		stats.Done, err = res.RowsAffected()
		if err != nil {
			return fmt.Errorf("cannot calculate how many done messages were deleted: %w", err)
		}
		if c.queueMaxDeliveries > 0 {
			res, err = tx.Exec(`
				UPDATE
					`+pq.QuoteIdentifier(c.tableName)+`
				SET
					queue = $1
				WHERE
					id IN (
						SELECT
							id
						FROM
							`+pq.QuoteIdentifier(c.tableName)+`
						WHERE
							queue = $2
							AND state = $3
							AND deliveries > $4
							AND leased_until < NOW()
						LIMIT CASE WHEN $5 < 0 THEN 0 ELSE $5 END
					)
				`, DefaultDeadLetterQueueNamePrefix+"-"+q.queue, q.queue, InProgress, c.queueMaxDeliveries, c.vacuumCurrentPageSize)
			if err != nil {
				return fmt.Errorf("cannot move messaged to dead letter queue: %w", err)
			}
			stats.Dead, err = res.RowsAffected()
			if err != nil {
				return fmt.Errorf("cannot calculate how many messages were moved to the dead letter queue: %w", err)
			}
			res, err = tx.Exec(`
				UPDATE
					`+pq.QuoteIdentifier(c.tableName)+`
				SET
					state = $1
				WHERE
					id IN (
						SELECT
							id
						FROM
							`+pq.QuoteIdentifier(c.tableName)+`
						WHERE
							queue = $2
							AND state = $3
							AND deliveries <= $4
							AND leased_until < NOW()
						LIMIT CASE WHEN $5 < 0 THEN 0 ELSE $5 END
					)
				`, New, q.queue, InProgress, c.queueMaxDeliveries, c.vacuumCurrentPageSize)
			if err != nil {
				return fmt.Errorf("cannot recover messages: %w", err)
			}
			stats.Recovered, err = res.RowsAffected()
			if err != nil {
				return fmt.Errorf("cannot calculate how many messages were recovered: %w", err)
			}
		}
		return nil
	})
	return stats, err
}

// Queue holds the configuration definition for one queue.
type Queue struct {
	client *Client

	queue     string
	closeOnce sync.Once
	closed    chan struct{}
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

// Reserve retrieves the pending message from the queue, if any available. It
// marks as it as InProgress until the defined lease duration. If the message
// is not marked as Done by the lease time, it is returned to the queue. Lease
// duration must be multiple of milliseconds.
func (q *Queue) Reserve(lease time.Duration) (*Message, error) {
	if q.isClosed() {
		return nil, ErrAlreadyClosed
	}
	var message *Message
	if err := validDuration(lease); err != nil {
		return nil, err
	}
	err := q.client.retry(func(tx *sql.Tx) (err error) {
		var (
			id          uint64
			content     []byte
			leasedUntil time.Time
		)
		row := tx.QueryRow(`
			UPDATE `+pq.QuoteIdentifier(q.client.tableName)+`
			SET
				deliveries = deliveries + 1,
				state = $1,
				leased_until = now() + $2::interval
			WHERE
				id IN (
					SELECT
						id
					FROM
						`+pq.QuoteIdentifier(q.client.tableName)+`
					WHERE
						queue = $3
						AND state = $4
					ORDER BY
						id ASC
					LIMIT 1
					FOR UPDATE
				)
			RETURNING id, content, leased_until
		`, InProgress, lease.String(), q.queue, New)
		if err := row.Scan(&id, &content, &leasedUntil); err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("cannot read message: %w", err)
		} else if err == sql.ErrNoRows {
			return ErrEmptyQueue
		}
		message = &Message{
			id:          id,
			Content:     content,
			LeasedUntil: leasedUntil,
			client:      q.client,
		}
		return nil
	})
	return message, err
}

// Push enqueues the given content to the target queue.
func (q *Queue) Push(content []byte) error {
	if q.isClosed() {
		return ErrAlreadyClosed
	}
	if err := validMessageSize(content); err != nil {
		return err
	}
	return q.client.retry(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO `+pq.QuoteIdentifier(q.client.tableName)+` (queue, state, content) VALUES ($1, $2, $3)`, q.queue, New, content); err != nil {
			return fmt.Errorf("cannot store message: %w", err)
		}
		if _, err := tx.Exec(`NOTIFY ` + pq.QuoteIdentifier(q.client.tableName) + `, ` + pq.QuoteLiteral(q.queue)); err != nil {
			return fmt.Errorf("cannot send push notification: %w", err)
		}
		return nil
	})
}

// Pop retrieves the pending message from the queue, if any available. If the
// queue is empty, it returns ErrEmptyQueue.
func (q *Queue) Pop() ([]byte, error) {
	if q.isClosed() {
		return nil, ErrAlreadyClosed
	}
	var content []byte
	err := q.client.retry(func(tx *sql.Tx) error {
		row := tx.QueryRow(`
			UPDATE `+pq.QuoteIdentifier(q.client.tableName)+`
			SET
				deliveries = deliveries + 1,
				state = $1
			WHERE
				id IN (
					SELECT
						id
					FROM
						`+pq.QuoteIdentifier(q.client.tableName)+`
					WHERE
						queue = $2
						AND state = $3
					ORDER BY
						id ASC
					LIMIT 1
					FOR UPDATE
				)
			RETURNING content
		`, Done, q.queue, New)
		if err := row.Scan(&content); err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("cannot read message: %w", err)
		} else if err == sql.ErrNoRows {
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
func (w *Watcher) Next() bool {
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
		switch msg, err := w.queue.Reserve(w.lease); err {
		case ErrEmptyQueue:
		case sql.ErrConnDone, ErrAlreadyClosed:
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
			go w.queue.client.listener.Ping()
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
	client      *Client
}

// Done mark message as done.
func (m *Message) Done() error {
	return m.client.retry(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE `+pq.QuoteIdentifier(m.client.tableName)+` SET state = $1 WHERE id = $2`, Done, m.id)
		return err
	})
}

// Release put the message back to the queue.
func (m *Message) Release() error {
	return m.client.retry(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE `+pq.QuoteIdentifier(m.client.tableName)+` SET leased_until = null, state = $1 WHERE id = $2`, New, m.id)
		return err
	})
}

// Touch extends the lease by the given duration. The duration must be multiples
// of milliseconds.
func (m *Message) Touch(extension time.Duration) error {
	if err := validDuration(extension); err != nil {
		return err
	}
	return m.client.retry(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE `+pq.QuoteIdentifier(m.client.tableName)+` SET leased_until = now() + $1::interval WHERE id = $2`, extension.String(), m.id)
		return err
	})
}

func validDuration(d time.Duration) error {
	valid := d > time.Millisecond && d%time.Millisecond == 0
	if !valid {
		return ErrInvalidDuration
	}
	return nil
}

func validMessageSize(content []byte) error {
	if len(content) > MaxMessageLength {
		return ErrMessageTooLarge
	}
	return nil
}
