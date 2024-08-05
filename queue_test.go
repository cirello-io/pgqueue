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
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

var dsn = os.Getenv("PGQUEUE_TEST_DSN")

func setupPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal("cannot open database connection:", err)
	}
	return pool
}

func TestOverload(t *testing.T) {
	t.Run("popPush", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client, err := Open(ctx, setupPool(t), WithCustomTable("overloadpoppush"), DisableAutoVacuum())
		if err != nil {
			t.Fatal("cannot open database connection:", err)
		}
		defer client.Close()
		if err := client.CreateTable(ctx); err != nil {
			t.Fatal("cannot create queue table:", err)
		}
		queueName := fmt.Sprintf("queue-overload-pop-push-%v", time.Now().UnixNano())
		t.Log("vacuuming the queue")
		client.Vacuum(ctx)
		t.Log("zeroing the queue")
		for {
			if _, err := client.Pop(ctx, queueName); errors.Is(err, ErrEmptyQueue) {
				break
			} else if err != nil {
				t.Fatal("cannot zero queue before overload test:", err)
			}
		}

		t.Log("pushing messages")
		for i := 0; i < 1_000; i++ {
			if err := client.Push(ctx, queueName, []byte("content")); err != nil {
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
					_, err := client.Pop(ctx, queueName)
					if errors.Is(err, ErrEmptyQueue) {
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
	t.Run("popReserveDelete", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client, err := Open(ctx, setupPool(t), WithCustomTable("overloadpopreservedelete"), DisableAutoVacuum())
		if err != nil {
			t.Fatal("cannot open database connection:", err)
		}
		defer client.Close()
		if err := client.CreateTable(ctx); err != nil {
			t.Fatal("cannot create queue table:", err)
		}
		queueName := fmt.Sprintf("queue-overload-pop-reserve-done-%v", time.Now().UnixNano())
		t.Log("vacuuming the queue")
		client.Vacuum(ctx)
		t.Log("zeroing the queue")
		for {
			if _, err := client.Pop(ctx, queueName); errors.Is(err, ErrEmptyQueue) {
				break
			} else if err != nil {
				t.Fatal("cannot zero queue before overload test:", err)
			}
		}

		t.Log("pushing messages")
		for i := 0; i < 1_000; i++ {
			if err := client.Push(ctx, queueName, []byte("content")); err != nil {
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
					m, err := client.Reserve(ctx, queueName, 5*time.Minute)
					if errors.Is(err, ErrEmptyQueue) {
						return nil
					} else if err != nil {
						t.Log(err)
						return err
					}
					if err := client.Delete(ctx, m.ID()); err != nil {
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
}

func TestDeadLetterDump(t *testing.T) {
	t.Run("badBulkSizeOperation", func(t *testing.T) {
		t.Parallel()
		client := &Client{}
		EnableDeadLetterQueue()(client)
		if _, err := client.DumpDeadLetterQueue(context.Background(), "", 0); !errors.Is(err, ErrZeroSizedBulkOperation) {
			t.Fatalf("expected error missing: %v", err)
		}
	})
	t.Run("deadLetterQueue", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		const reservationTime = 500 * time.Millisecond
		client, err := Open(ctx, setupPool(t),
			WithMaxDeliveries(2),
			DisableAutoVacuum(),
			EnableDeadLetterQueue(),
			WithCustomTable("deadletter-dump-dead-letter-queue"),
		)
		if err != nil {
			t.Fatal("cannot open database connection:", err)
		}
		defer client.Close()
		if err := client.CreateTable(ctx); err != nil {
			t.Fatal("cannot create queue table:", err)
		}
		expectedMessage := []byte("the message")
		queueName := "example-deadletter-queue"
		if err := client.Push(ctx, queueName, expectedMessage); err != nil {
			t.Fatal("cannot push message to queue:", err)
		}
		if _, err := client.Reserve(ctx, queueName, reservationTime); err != nil {
			t.Fatal("cannot reserve message from the queue (try):", err)
		}
		time.Sleep(2 * reservationTime)
		client.Vacuum(ctx)
		if _, err := client.Reserve(ctx, queueName, reservationTime); err != nil {
			t.Fatal("cannot reserve message from the queue (retry):", err)
		}
		time.Sleep(2 * reservationTime)
		stats := client.Vacuum(ctx)
		t.Logf("vacuum stats\n%s", stats)
		msgs, err := client.DumpDeadLetterQueue(ctx, queueName, 1)
		if err != nil {
			t.Fatal("cannot dump dead letter queue")
		}
		if len(msgs) != 1 {
			t.Fatal("unexpected number of messages found:", len(msgs))
		}
		dead := msgs[0]
		if content := dead.Content(); !bytes.Contains(content, expectedMessage) {
			t.Logf("%s", content)
			t.Fatal("dump not found")
		}
		t.Logf("dumped message: %v %s", dead.ID(), dead.Content())
	})
	t.Run("closedClient", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		const reservationTime = 500 * time.Millisecond
		client, err := Open(ctx, setupPool(t),
			WithMaxDeliveries(2),
			DisableAutoVacuum(),
			EnableDeadLetterQueue(),
			WithCustomTable("deadletter-dump-closed-client"),
		)
		if err != nil {
			t.Fatal("cannot open database connection:", err)
		}
		defer client.Close()
		if err := client.CreateTable(ctx); err != nil {
			t.Fatal("cannot create queue table:", err)
		}
		expectedMessage := []byte("the message")
		queueName := "example-deadletter-queue"
		if err := client.Push(ctx, queueName, expectedMessage); err != nil {
			t.Fatal("cannot push message to queue:", err)
		}
		if _, err := client.Reserve(ctx, queueName, reservationTime); err != nil {
			t.Fatal("cannot reserve message from the queue (try):", err)
		}
		time.Sleep(2 * reservationTime)
		client.Vacuum(ctx)
		if _, err := client.Reserve(ctx, queueName, reservationTime); err != nil {
			t.Fatal("cannot reserve message from the queue (retry):", err)
		}
		time.Sleep(2 * reservationTime)
		stats := client.Vacuum(ctx)
		t.Logf("vacuum stats\n%s", stats)
		if err := client.Close(); err != nil {
			t.Fatal("cannot close client:", err)
		}
		if _, err := client.DumpDeadLetterQueue(ctx, queueName, 1); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatal("unexpected error:", err)
		}
	})
	t.Run("emptyQueue", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client, err := Open(ctx, setupPool(t),
			WithMaxDeliveries(2),
			DisableAutoVacuum(),
			EnableDeadLetterQueue(),
			WithCustomTable("deadletter-empty-queue"),
		)
		if err != nil {
			t.Fatal("cannot open database connection:", err)
		}
		defer client.Close()
		if err := client.CreateTable(ctx); err != nil {
			t.Fatal("cannot create queue table:", err)
		}
		queueName := "example-deadletter-queue"
		if _, err := client.DumpDeadLetterQueue(ctx, queueName, 1); err != nil {
			t.Fatal("unexpected error:", err)
		}
	})
}

func TestReconfiguredClient(t *testing.T) {
	t.Parallel()
	t.Run("customTable", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client, err := Open(ctx, setupPool(t), WithCustomTable("queue2"))
		if err != nil {
			t.Fatal("cannot open database connection:", err)
		}
		defer client.Close()
		if err := client.CreateTable(ctx); err != nil {
			t.Fatal("cannot create queue table:", err)
		}
		queueName := "queue-reconfigured-client"
		expectedContent := []byte("content")
		if err := client.Push(ctx, queueName, expectedContent); err != nil {
			t.Fatal("cannot push message to queue:", err)
		}
		poppedContent, err := client.Pop(ctx, queueName)
		if err != nil {
			t.Fatal("cannot pop message from the queue:", err)
		}
		if !bytes.Equal(poppedContent, expectedContent) {
			t.Errorf("unexpected output: %s", poppedContent)
		}
	})
	t.Run("badListener", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client, err := Open(ctx, setupPool(t), func(c *Client) {
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
	t.Parallel()
	ctx := context.Background()
	client, err := Open(ctx, setupPool(t), WithCustomTable("close-error"))
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

func TestValidationErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, err := Open(ctx, setupPool(t), WithCustomTable("validation-errors"), DisableAutoVacuum())
	if err != nil {
		t.Fatal("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		t.Fatal("cannot create queue table:", err)
	}
	queueName := "closed-client-queue"
	if _, err := client.Reserve(ctx, queueName, 0); !errors.Is(err, ErrInvalidDuration) {
		t.Error("expected ErrInvalidDuration:", err)
	}
	if err := client.Extend(ctx, 0, 0); !errors.Is(err, ErrInvalidDuration) {
		t.Error("expected ErrInvalidDuration:", err)
	}
}

func TestDisableDeadLetterQueue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, err := Open(ctx, setupPool(t),
		WithMaxDeliveries(1),
		DisableAutoVacuum(),
		WithCustomTable("disable-deadletter-queue"),
	)
	if err != nil {
		t.Fatal("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		t.Fatal("cannot create queue table:", err)
	}
	queueName := fmt.Sprintf("disabled_dl_queue_%s", time.Now())
	if err := client.Push(ctx, queueName, []byte("hello")); err != nil {
		t.Fatal("cannot push message:", err)
	}
	if _, err := client.Reserve(ctx, queueName, 1*time.Second); err != nil {
		t.Fatal("cannot reserve message:", err)
	}
	client.Vacuum(ctx)
	time.Sleep(2 * time.Second)
	if _, err := client.DumpDeadLetterQueue(ctx, queueName, 1); !errors.Is(err, ErrDeadLetterQueueDisabled) {
		t.Fatal("expected error missing, got:", err)
	}
	if m, err := client.Reserve(ctx, queueName, 1*time.Second); !errors.Is(err, ErrEmptyQueue) {
		t.Logf("m: %s", m.Content())
		t.Fatal("queue should be empty:", err)
	}
}

func TestQueueApproximateCount(t *testing.T) {
	t.Run("fullQueue", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatalf("cannot open database connection pool: %v", err)
		}
		client, err := Open(ctx, pool, WithCustomTable("queue-approximate-count-full-queue"))
		if err != nil {
			t.Fatalf("cannot create queue handler: %v", err)
		}
		defer client.Close()
		if err := client.CreateTable(ctx); err != nil {
			t.Fatalf("cannot create queue table: %v", err)
		}
		queueName := fmt.Sprintf("queue-approximate-message-count-%s", time.Now())
		const (
			pushedCount          = 10000
			reservedCount        = 100
			expectedMessageCount = pushedCount - reservedCount
		)
		for i := 0; i < pushedCount; i++ {
			if err := client.Push(ctx, queueName, []byte("content")); err != nil {
				t.Fatalf("cannot push message to queue: %v", err)
			}
		}
		for i := 0; i < reservedCount; i++ {
			if _, err := client.Reserve(ctx, queueName, 1*time.Minute); err != nil {
				t.Fatalf("cannot reserve message from queue: %v", err)
			}
		}
		count, err := client.ApproximateCount(ctx, queueName)
		if err != nil {
			t.Fatalf("cannot get approximate message count: %v", err)
		}
		if count != expectedMessageCount {
			t.Fatalf("unexpected approximate message count: %d", count)
		}
		_ = client.Close()
		if _, err := client.ApproximateCount(ctx, queueName); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("emptyQueue", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatalf("cannot open database connection pool: %v", err)
		}
		client, err := Open(ctx, pool, WithCustomTable("queue-approximate-count-empty-queue"))
		if err != nil {
			t.Fatalf("cannot create queue handler: %v", err)
		}
		defer client.Close()
		if err := client.CreateTable(ctx); err != nil {
			t.Fatalf("cannot create queue table: %v", err)
		}
		queueName := fmt.Sprintf("queue-approximate-message-count-%s", time.Now())
		count, err := client.ApproximateCount(ctx, queueName)
		if err != nil {
			t.Fatalf("cannot get approximate message count: %v", err)
		}
		if count != 0 {
			t.Fatalf("unexpected approximate message count: %d", count)
		}
	})
}

func TestErrZeroSizedBulkOperation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := &Client{}
	if err := client.PushN(ctx, "", nil); !errors.Is(err, ErrZeroSizedBulkOperation) {
		t.Fatalf("expected error missing (PushN): %v", err)
	}
	if _, err := client.ReserveN(ctx, "", 1*time.Second, 0); !errors.Is(err, ErrZeroSizedBulkOperation) {
		t.Fatalf("expected error missing (ReserveN): %v", err)
	}
	if _, err := client.PopN(ctx, "", 0); !errors.Is(err, ErrZeroSizedBulkOperation) {
		t.Fatalf("expected error missing (PopN): %v", err)
	}
}

func TestClientClose(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("cannot open database connection pool:", err)
	}
	client, err := Open(ctx, pool, DisableAutoVacuum(), WithCustomTable("client-close"))
	if err != nil {
		t.Fatal("cannot open database connection:", err)
	}
	t.Run("closeClient", func(t *testing.T) {
		if err := client.Close(); err != nil {
			t.Fatal("cannot close client:", err)
		}
		if err := client.Close(); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatal("expected error missing:", err)
		}
	})

	queueName := "client-close-queue"
	content := []byte("content")

	t.Run("dumpDeadLetterQueue", func(t *testing.T) {
		if _, err := client.DumpDeadLetterQueue(ctx, queueName, 1); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatal("expected error missing:", err)
		}
	})
	t.Run("createTable", func(t *testing.T) {
		if err := client.CreateTable(ctx); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatal("expected error missing:", err)
		}
	})
	t.Run("vacuum", func(t *testing.T) {
		if stats := client.Vacuum(ctx); !errors.Is(stats.Err(), ErrAlreadyClosed) {
			t.Fatal("expected error missing:", err)
		}
	})
	t.Run("approximateCount", func(t *testing.T) {
		if _, err := client.ApproximateCount(ctx, queueName); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatal("expected error missing:", err)
		}
	})
	t.Run("push", func(t *testing.T) {
		if err := client.Push(ctx, queueName, content); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatal("expected error missing:", err)
		}
	})
	t.Run("reserve", func(t *testing.T) {
		if _, err := client.Reserve(ctx, queueName, time.Hour); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatal("expected error missing:", err)
		}
	})
	t.Run("release", func(t *testing.T) {
		if err := client.Release(ctx, 0); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatal("expected error missing:", err)
		}
	})
	t.Run("extend", func(t *testing.T) {
		if err := client.Extend(ctx, 0, time.Hour); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatal("expected error missing:", err)
		}
	})
	t.Run("pop", func(t *testing.T) {
		if _, err := client.Pop(ctx, queueName); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatal("expected error missing:", err)
		}
	})
	t.Run("delete", func(t *testing.T) {
		if err := client.Delete(ctx, 0); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatal("expected error missing:", err)
		}
	})
}

func TestMessageAttributes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, err := Open(ctx, setupPool(t), WithCustomTable("message-attributes"), DisableAutoVacuum())
	if err != nil {
		t.Fatal("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		t.Fatal("cannot create queue table:", err)
	}
	queueName := fmt.Sprintf("msg-attributes-%v", time.Now().UnixNano())
	if err := client.Push(ctx, queueName, []byte("content")); err != nil {
		t.Fatal("cannot push message to queue:", err)
	}
	m, err := client.Reserve(ctx, queueName, 5*time.Minute)
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	t.Logf("%v %s %v", m.ID(), m.Content(), m.LeaseDeadline())
	if err := client.Delete(ctx, m.ID()); err != nil {
		t.Fatal("cannot delete message:", err)
	}
	if m.ID() == 0 {
		t.Error("missing message ID")
	}
	if len(m.Content()) == 0 {
		t.Error("missing message content")
	}
	if m.LeaseDeadline().IsZero() {
		t.Error("missing message lease deadline")
	}
}

func TestPurge(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("cannot open database connection pool:", err)
	}
	client, err := Open(ctx, pool, DisableAutoVacuum(), WithCustomTable("purge"))
	if err != nil {
		t.Fatal("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		t.Fatal("cannot create queue table:", err)
	}
	msg := bytes.Repeat([]byte("A"), 65536)
	msgs := make([][]byte, 10_000)
	for i := range msgs {
		msgs[i] = msg
	}
	queueName := "queue-purge"
	if err := client.PushN(ctx, queueName, msgs); err != nil {
		t.Fatal("cannot push message:", err)
	}
	if err := client.Purge(ctx, queueName); err != nil {
		t.Fatal("cannot purge queue:", err)
	}
	count, err := client.ApproximateCount(ctx, queueName)
	if err != nil {
		t.Fatal("cannot get approximate message count:", err)
	}
	if count != 0 {
		t.Fatalf("unexpected approximate message count: %d", count)
	}
}
