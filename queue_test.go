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
	client, err := Open(dsn)
	if err != nil {
		t.Fatal("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(); err != nil {
		t.Fatal("cannot create queue table:", err)
	}
	queue := client.Queue("queue-overload", DisableAutoVacuum())
	defer queue.Close()
	t.Log("vacuuming the queue")
	if stats := queue.Vacuum(); stats.Error != nil {
		t.Fatal("cannot clean up queue before overload test:", stats.Error)
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
}

func TestCustomAutoVacuum(t *testing.T) {
	client, err := Open(dsn)
	if err != nil {
		t.Fatal("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(); err != nil {
		t.Fatal("cannot create queue table:", err)
	}
	const freq = 100 * time.Millisecond
	timer := time.NewTimer(freq)
	queue := client.Queue("queue-custom-autovacuum", WithCustomAutoVacuum(timer))
	defer queue.Close()
	if err := queue.Push([]byte("content")); err != nil {
		t.Fatal("cannot push content:", err)
	}
	if _, err := queue.Pop(); err != nil {
		t.Fatal("cannot pop content:", err)
	}
	time.Sleep(freq * 2)
	stats := queue.VacuumStats()
	if stats.Error != nil {
		t.Fatal("unexpected error found on vacuum stats:", err)
	}
	t.Log(stats.Done)
	t.Log(stats.Recovered)
	t.Log(stats.Dead)
	if stats.Done != 1 || stats.Recovered != 0 || stats.Dead != 0 {
		t.Fatal("auto-vacuum failed")
	}
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
	if stats := queue.Vacuum(); stats.Error != nil {
		t.Fatal("cannot clean up queue:", err)
	}
	if _, err := queue.Reserve(reservationTime); err != nil {
		t.Fatal("cannot reserve message from the queue (retry):", err)
	}
	time.Sleep(2 * reservationTime)
	if stats := queue.Vacuum(); stats.Error != nil {
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
	if err := q.Push(nil); err == nil {
		t.Error("push on closed queue must fail")
	}
	if _, err := q.Pop(); err == nil {
		t.Error("pop on closed queue must fail")
	}
	if _, err := q.Reserve(0); err == nil {
		t.Error("pop on closed queue must fail")
	}
	w := q.Watch(0)
	if w.Next() {
		t.Error("Watcher.Next on closed queue must be false")
	}
	if err := w.Err(); err != ErrAlreadyClosed {
		t.Error("Watcher.Err() on closed queue must errors with ErrAlreadyClosed:", err)
	}
	if err := q.Close(); err != ErrAlreadyClosed {
		t.Error("close on closed queue must error with ErrAlreadyClosed:", err)
	}
}
