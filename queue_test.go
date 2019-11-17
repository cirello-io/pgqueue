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

package pgqueue_test

import (
	"flag"
	"sync"
	"testing"

	"cirello.io/pgqueue"
	_ "github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

var dsn = flag.String("dsn", "postgres://postgres@localhost/postgres?sslmode=disable", "connection string to the test database server")

func TestOverload(t *testing.T) {
	client, err := pgqueue.Open(*dsn)
	if err != nil {
		t.Fatal("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(); err != nil {
		t.Fatal("cannot create queue table:", err)
	}
	queue := client.Queue("queue-overload", pgqueue.DisableAutoVacuum())
	defer queue.Close()
	t.Parallel()
	t.Log("vacuuming the queue")
	if stats := queue.Vacuum(); stats.Error != nil {
		t.Fatal("cannot clean up queue before overload test:", stats.Error)
	}
	t.Log("zeroing the queue")
	for {
		if _, err := queue.Pop(); err == pgqueue.ErrEmptyQueue {
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
				if err == pgqueue.ErrEmptyQueue {
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
