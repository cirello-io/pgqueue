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
	"bytes"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

var dsn = os.Getenv("PGQUEUE_TEST_DSN")

func TestOverload(t *testing.T) {
	t.Run("popPush", func(t *testing.T) {
		t.Parallel()
		client, err := Open(dsn, WithCustomTable("overloadpoppush"))
		if err != nil {
			t.Fatal("cannot open database connection:", err)
		}
		defer client.Close()
		if err := client.CreateTable(); err != nil {
			t.Fatal("cannot create queue table:", err)
		}
		queue := client.Queue("queue-overload-pop-push", DisableAutoVacuum())
		defer queue.Close()
		t.Log("vacuuming the queue")
		if stats := queue.Vacuum(); stats.Err != nil {
			t.Fatal("cannot clean up queue before overload test:", stats.Err)
		}
		t.Log("zeroing the queue")
		for {
			if _, err := queue.Pop(); err == ErrEmptyQueue {
				break
			} else if err != nil {
				t.Fatal("cannot zero queue before overload test:", err)
			}
		}

		t.Log("pushing messages")
		for i := 0; i < 1_000; i++ {
			content := []byte("content")
			if err := queue.Push(content); err != nil {
				t.Fatal("cannot push message to queue:", err)
			}
		}
		t.Log("popping messages")
		var (
			g errgroup.Group

			mu       sync.Mutex
			totalMsg int
		)
		for i := 0; i < 10; i++ {
			g.Go(func() error {
				for {
					_, err := queue.Pop()
					if err == ErrEmptyQueue {
						return nil
					} else if err != nil {
						t.Log(err)
						return err
					}
					mu.Lock()
					totalMsg++
					mu.Unlock()
				}
			})
		}
		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
		if totalMsg != 1_000 {
			t.Fatal("messages lost?", totalMsg)
		}
	})
	t.Run("popReserveDone", func(t *testing.T) {
		t.Parallel()
		client, err := Open(dsn, WithCustomTable("overloadpopreservedone"))
		if err != nil {
			t.Fatal("cannot open database connection:", err)
		}
		defer client.Close()
		if err := client.CreateTable(); err != nil {
			t.Fatal("cannot create queue table:", err)
		}
		queue := client.Queue("queue-overload-pop-reserve-done", DisableAutoVacuum())
		defer queue.Close()
		t.Log("vacuuming the queue")
		if stats := queue.Vacuum(); stats.Err != nil {
			t.Fatal("cannot clean up queue before overload test:", stats.Err)
		}
		t.Log("zeroing the queue")
		for {
			if _, err := queue.Pop(); err == ErrEmptyQueue {
				break
			} else if err != nil {
				t.Fatal("cannot zero queue before overload test:", err)
			}
		}

		t.Log("pushing messages")
		for i := 0; i < 1_000; i++ {
			content := []byte("content")
			if err := queue.Push(content); err != nil {
				t.Fatal("cannot push message to queue:", err)
			}
		}
		t.Log("reserving messages")
		var (
			g errgroup.Group

			mu       sync.Mutex
			totalMsg int
		)
		for i := 0; i < 10; i++ {
			g.Go(func() error {
				for {
					m, err := queue.Reserve(5 * time.Minute)
					if err == ErrEmptyQueue {
						return nil
					} else if err != nil {
						t.Log(err)
						return err
					}
					m.Done()
					mu.Lock()
					totalMsg++
					mu.Unlock()
				}
			})
		}
		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
		if totalMsg != 1_000 {
			t.Fatal("messages lost?", totalMsg)
		}
	})
}

func TestCustomVacuum(t *testing.T) {
	t.Run("small autovacuum", func(t *testing.T) {
		client, err := Open(dsn)
		if err != nil {
			t.Fatal("cannot open database connection:", err)
		}
		defer client.Close()
		if err := client.CreateTable(); err != nil {
			t.Fatal("cannot create queue table:", err)
		}
		const freq = 100 * time.Millisecond
		ticker := time.NewTicker(freq)
		queue := client.Queue("queue-custom-small-autovacuum", WithAutoVacuum(ticker))
		defer queue.Close()
		if err := queue.Push([]byte("content")); err != nil {
			t.Fatal("cannot push content:", err)
		}
		if _, err := queue.Pop(); err != nil {
			t.Fatal("cannot pop content:", err)
		}
		time.Sleep(freq * 2)
		stats := queue.VacuumStats()
		if stats.Err != nil {
			t.Fatal("unexpected error found on vacuum stats:", err)
		}
		t.Log(stats.Done)
		t.Log(stats.Recovered)
		t.Log(stats.Dead)
		if stats.Done != 1 || stats.Recovered != 0 || stats.Dead != 0 {
			t.Fatal("auto-vacuum failed")
		}
	})
	t.Run("large manual vacuum", func(t *testing.T) {
		client, err := Open(dsn)
		if err != nil {
			t.Fatal("cannot open database connection:", err)
		}
		defer client.Close()
		if err := client.CreateTable(); err != nil {
			t.Fatal("cannot create queue table:", err)
		}
		queue := client.Queue("queue-custom-large-vacuum")
		defer queue.Close()
		for i := 0; i < 1_000; i++ {
			if err := queue.Push([]byte("content")); err != nil {
				t.Fatal("cannot push message to queue:", err)
			}
			if _, err := queue.Pop(); err != nil {
				t.Fatal("cannot pop message from queue:", err)
			}
		}
		for {
			stats := queue.Vacuum()
			t.Logf("%d %#v", stats.PageSize, stats)
			if stats.Done == 0 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	})
}

type badWriter struct{}

func (w *badWriter) Write([]byte) (int, error) {
	return 0, errors.New("wrote nothing")
}

func TestDeadletterDump(t *testing.T) {
	const reservationTime = 500 * time.Millisecond
	client, err := Open(dsn)
	if err != nil {
		t.Fatal("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(); err != nil {
		t.Fatal("cannot create queue table:", err)
	}
	t.Run("good dump", func(t *testing.T) {
		queue := client.Queue(
			"example-deadletter-queue",
			WithMaxDeliveries(1),
			DisableAutoVacuum(),
		)
		defer queue.Close()
		content := []byte("the message")
		if err := queue.Push(content); err != nil {
			t.Fatal("cannot push message to queue:", err)
		}
		if _, err := queue.Reserve(reservationTime); err != nil {
			t.Fatal("cannot reserve message from the queue (try):", err)
		}
		time.Sleep(2 * reservationTime)
		if stats := queue.Vacuum(); stats.Err != nil {
			t.Fatal("cannot clean up queue:", err)
		}
		if _, err := queue.Reserve(reservationTime); err != nil {
			t.Fatal("cannot reserve message from the queue (retry):", err)
		}
		time.Sleep(2 * reservationTime)
		if stats := queue.Vacuum(); stats.Err != nil {
			t.Fatal("cannot clean up queue:", err)
		}

		var buf bytes.Buffer
		if err := client.DumpDeadLetterQueue("example-deadletter-queue", &buf); err != nil {
			t.Fatal("cannot dump dead letter queue")
		}
		t.Log(buf.String())
		if !strings.Contains(buf.String(), "dGhlIG1lc3NhZ2U=") {
			t.Fatal("bad dump found")
		}
	})
	t.Run("bad io.Writer", func(t *testing.T) {
		queue := client.Queue(
			"example-deadletter-queue-bad-writer",
			WithMaxDeliveries(1),
			DisableAutoVacuum(),
		)
		defer queue.Close()
		content := []byte("the message")
		if err := queue.Push(content); err != nil {
			t.Fatal("cannot push message to queue:", err)
		}
		if _, err := queue.Reserve(reservationTime); err != nil {
			t.Fatal("cannot reserve message from the queue (try):", err)
		}
		time.Sleep(2 * reservationTime)
		if stats := queue.Vacuum(); stats.Err != nil {
			t.Fatal("cannot clean up queue:", err)
		}
		if _, err := queue.Reserve(reservationTime); err != nil {
			t.Fatal("cannot reserve message from the queue (retry):", err)
		}
		time.Sleep(2 * reservationTime)
		if stats := queue.Vacuum(); stats.Err != nil {
			t.Fatal("cannot clean up queue:", err)
		}
		err := client.DumpDeadLetterQueue("example-deadletter-queue-bad-writer", &badWriter{})
		if err == nil {
			t.Fatal("expected error not found")
		}
		t.Log(err)
	})
}

func TestReconfiguredClient(t *testing.T) {
	t.Run("custom table", func(t *testing.T) {
		client, err := Open(dsn, WithCustomTable("queue2"))
		if err != nil {
			t.Fatal("cannot open database connection:", err)
		}
		defer client.Close()
		if err := client.CreateTable(); err != nil {
			t.Fatal("cannot create queue table:", err)
		}
		queue := client.Queue("queue-reconfigured-client")
		defer queue.Close()
		content := []byte("content")
		if err := queue.Push(content); err != nil {
			t.Fatal("cannot push message to queue:", err)
		}
		poppedContent, err := queue.Pop()
		if err != nil {
			t.Fatal("cannot pop message from the queue:", err)
		}
		if !bytes.Equal(poppedContent, content) {
			t.Errorf("unexpected output: %s", poppedContent)
		}
	})
	t.Run("valid DSN to bad target", func(t *testing.T) {
		client, err := Open("postgresql://server-404")
		if err == nil {
			client.Close()
			t.Fatal("expected error not found")
		}
		t.Log("error:", err)
	})
	t.Run("bad DSN", func(t *testing.T) {
		client, err := Open("postgresql://bad-target?client_encoding=absurd")
		if err == nil {
			client.Close()
			t.Fatal("expected error not found")
		}
		t.Log("error:", err)
	})
	t.Run("bad listener", func(t *testing.T) {
		client, err := Open(dsn, func(c *Client) {
			// sabotage the client during setup
			c.Close()
		})
		if err == nil {
			client.Close()
			t.Fatal("expected error missing")
		}
		t.Log("error:", err)
	})
}

func TestCloseError(t *testing.T) {
	client, err := Open(dsn)
	if err != nil {
		t.Fatal("cannot open database connection:", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal("first close should always be clean:", err)
	}
	if err := client.Close(); err == nil {
		t.Fatal("second close should always be dirty:", err)
	} else {
		t.Log("expected error found:", err)
	}
}

func TestClosedQueue(t *testing.T) {
	client, err := Open(dsn)
	if err != nil {
		t.Fatal("cannot open database connection:", err)
	}
	defer client.Close()
	q := client.Queue("closed-client-queue")
	q.Close()
	if stats := q.VacuumStats(); stats.Err != ErrAlreadyClosed {
		t.Error("auto-vacuum VacuumStats.Err on closed queue must error with ErrAlreadyClosed:", stats.Err)
	}
	if stats := q.Vacuum(); stats.Err != ErrAlreadyClosed {
		t.Error("vacuum VacuumStats.Err on closed queue must error with ErrAlreadyClosed:", stats.Err)
	}
	if err := q.Push(nil); err != ErrAlreadyClosed {
		t.Error("push on closed queue must fail with ErrAlreadyClose:", err)
	}
	if _, err := q.Pop(); err != ErrAlreadyClosed {
		t.Error("pop on closed queue must fail with ErrAlreadyClose:", err)
	}
	if _, err := q.Reserve(0); err != ErrAlreadyClosed {
		t.Error("reserve on closed queue must fail with ErrAlreadyClose:", err)
	}
	w := q.Watch(0)
	if w.Next() {
		t.Error("Watcher.Next on closed queue must be false")
	}
	if err := w.Err(); err != ErrAlreadyClosed {
		t.Error("Watcher.Err() on closed queue must error with ErrAlreadyClosed:", err)
	}
	if stats := q.VacuumStats(); stats.Err != ErrAlreadyClosed {
		t.Error("VacuumStats.Err on closed queue must error with ErrAlreadyClosed:", stats.Err)
	}

	if err := q.Close(); err != ErrAlreadyClosed {
		t.Error("close on closed queue must error with ErrAlreadyClosed:", err)
	}
}

func TestValidationErrors(t *testing.T) {
	client, err := Open(dsn)
	if err != nil {
		t.Fatal("cannot open database connection:", err)
	}
	defer client.Close()
	q := client.Queue("closed-client-queue", DisableAutoVacuum())
	defer q.Close()
	if err := q.Push(bytes.Repeat([]byte("A"), MaxMessageLength+1)); err != ErrMessageTooLarge {
		t.Error("expected ErrMessageTooLarge:", err)
	}
	if _, err := q.Reserve(0); err != ErrInvalidDuration {
		t.Error("expected ErrInvalidDuration:", err)
	}
	var m Message
	if err := m.Touch(0); err != ErrInvalidDuration {
		t.Error("expected ErrInvalidDuration:", err)
	}
}

func TestWatchNextErrors(t *testing.T) {
	client, err := Open(dsn)
	if err != nil {
		t.Fatal("cannot open database connection:", err)
	}
	defer client.Close()
	t.Run("close while next", func(t *testing.T) {
		q := client.Queue("close while next", DisableAutoVacuum())
		w := q.Watch(1 * time.Second)
		go func() {
			time.Sleep(1 * time.Second)
			q.Close()
		}()
		if w.Next() {
			t.Error("unexpected first next=true while closing - should have detected close from Reserve")
		}
		if w.Next() {
			t.Error("unexpected second next=true while closing - should have detected close from w.err")
		}
		if err := w.Err(); err != ErrAlreadyClosed {
			t.Error("expected error not found:", err)
		}
	})
	t.Run("next after close", func(t *testing.T) {
		q := client.Queue("next after close", DisableAutoVacuum())
		w := q.Watch(1 * time.Second)
		q.Close()
		if w.Next() {
			t.Error("unexpected first next=true after close - should have detected close from queue")
		}
		if w.Next() {
			t.Error("unexpected second next=true after close - should have detected close from w.err")
		}
		if err := w.Err(); err != ErrAlreadyClosed {
			t.Error("expected error not found:", err)
		}
	})
}

func TestCrossQueueBump(t *testing.T) {
	var wg sync.WaitGroup
	defer wg.Wait()
	client, err := Open(dsn)
	if err != nil {
		t.Fatal("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(); err != nil {
		t.Fatal("cannot create queue table:", err)
	}
	qAlpha := client.Queue("cross-queue-bump-alpha", DisableAutoVacuum())
	defer qAlpha.Close()
	qBravo := client.Queue("cross-queue-bump-bravo", DisableAutoVacuum())
	defer qBravo.Close()
	watchAlpha := qAlpha.Watch(time.Minute)
	alphaGotMessage := make(chan bool, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		alphaGotMessage <- watchAlpha.Next()
		t.Log("watchAlpha got a message")
	}()
	if err := qBravo.Push([]byte("message-bravo")); err != nil {
		t.Fatal("cannot push message to qBravo:", err)
	}
	select {
	case <-alphaGotMessage:
		msg := watchAlpha.Message().Content
		t.Logf("msg: %s", msg)
		t.Fatal("wrong bump")
	case <-time.After(missedNotificationFrequency):
		t.Log("watchAlpa.Next() was not affected by a message dispatched to qBravo")
	}
	if err := qAlpha.Push([]byte("message-alpha")); err != nil {
		t.Fatal("cannot push message to qAlpha:", err)
	}
	select {
	case next := <-alphaGotMessage:
		msg := watchAlpha.Message().Content
		t.Logf("next: %v", next)
		t.Logf("msg: %s", msg)
		if !bytes.Equal([]byte("message-alpha"), msg) {
			t.Error("unexpected message found")
		}
	case <-time.After(missedNotificationFrequency):
		t.Error("missed bump")
	}
}

func TestSaturatedNotifications(t *testing.T) {
	var wg sync.WaitGroup
	defer wg.Wait()
	client, err := Open(dsn)
	if err != nil {
		t.Fatal("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(); err != nil {
		t.Fatal("cannot create queue table:", err)
	}
	q := client.Queue("saturated-notifications", DisableAutoVacuum())
	defer q.Close()
	// force Client.forwardNotifications to start dropping messages.
	w := q.Watch(time.Minute)
	if err := q.Push([]byte("message-1")); err != nil {
		t.Fatal("cannot push message (1) to the queue:", err)
	}
	if err := q.Push([]byte("message-2")); err != nil {
		t.Fatal("cannot push messages (2) to the queue:", err)
	}
	if !w.Next() {
		t.Error("there are two messages in the queue, w.Next() must be true")
	}
	t.Logf("%s", w.Message().Content)
	if !w.Next() {
		t.Error("there is one message in the queue, w.Next() must be true")
	}
	t.Logf("%s", w.Message().Content)
	next := make(chan bool, 1)
	go func() {
		next <- w.Next()
	}()
	time.Sleep(time.Second)
	q.Close()
	if <-next {
		t.Error("next should have been false after close")
	}
}
