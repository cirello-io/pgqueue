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
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func BenchmarkThroughput(b *testing.B) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		b.Fatal("cannot open database connection pool:", err)
	}
	client, err := Open(ctx, pool, DisableAutoVacuum())
	if err != nil {
		b.Fatal("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		b.Fatal("cannot create queue table:", err)
	}
	msg := bytes.Repeat([]byte("A"), 65536)
	b.Run("push", func(b *testing.B) {
		msgs := make([][]byte, b.N)
		for i := range msgs {
			msgs[i] = msg
		}
		b.SetBytes(int64(len(msg)))
		b.ResetTimer()
		queue := client.Queue("queue-benchmark-push")
		defer queue.Close()
		for _, msg := range msgs {
			if err := queue.Push(ctx, msg); err != nil {
				b.Fatal("cannot push message:", err)
			}
		}
	})
	b.Run("pushN", func(b *testing.B) {
		msgs := make([][]byte, b.N)
		for i := range msgs {
			msgs[i] = msg
		}
		b.SetBytes(int64(len(msg)))
		b.ResetTimer()
		queue := client.Queue("queue-benchmark-push-n")
		defer queue.Close()
		if err := queue.PushN(ctx, msgs); err != nil {
			b.Fatal("cannot push message:", err)
		}
	})
	b.Run("pop", func(b *testing.B) {
		msgs := make([][]byte, b.N)
		for i := range msgs {
			msgs[i] = msg
		}
		queue := client.Queue("queue-benchmark-pop")
		defer queue.Close()
		if err := queue.PushN(ctx, msgs); err != nil {
			b.Fatal("cannot push messages:", err)
		}
		b.SetBytes(int64(len(msg)))
		b.ResetTimer()
		for range msgs {
			if _, err := queue.Pop(ctx); err != nil {
				b.Fatal("cannot pop message:", err)
			}
		}
	})
	b.Run("popN", func(b *testing.B) {
		msgs := make([][]byte, b.N)
		for i := range msgs {
			msgs[i] = msg
		}
		queue := client.Queue("queue-benchmark-pop-n")
		defer queue.Close()
		if err := queue.PushN(ctx, msgs); err != nil {
			b.Fatal("cannot push messages:", err)
		}
		b.SetBytes(int64(len(msg)))
		b.ResetTimer()
		if _, err := queue.PopN(ctx, len(msgs)); err != nil {
			b.Fatal("cannot pop message:", err)
		}
	})
	b.Run("pushNPopN", func(b *testing.B) {
		msgs := make([][]byte, b.N)
		for i := range msgs {
			msgs[i] = msg
		}
		queue := client.Queue("queue-benchmark-push-n-pop-n")
		defer queue.Close()
		b.SetBytes(2 * int64(len(msg)))
		b.ResetTimer()
		if err := queue.PushN(ctx, msgs); err != nil {
			b.Fatal("cannot push messages:", err)
		}
		if _, err := queue.PopN(ctx, len(msgs)); err != nil {
			b.Fatal("cannot pop message:", err)
		}
	})
	b.Run("pushReserveDone", func(b *testing.B) {
		msgs := make([][]byte, b.N)
		for i := range msgs {
			msgs[i] = msg
		}
		queue := client.Queue("queue-benchmark-pushReserveDone")
		defer queue.Close()
		b.SetBytes(2 * int64(len(msg)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := queue.Push(ctx, msg); err != nil {
				b.Fatal("cannot push message:", err)
			}
		}
		for i := 0; i < b.N; i++ {
			msg, err := queue.Reserve(ctx, time.Minute)
			if err != nil {
				b.Fatal("cannot reserve message:", err)
			}
			if err := msg.Done(ctx); err != nil {
				b.Fatalf("cannot mark message (%d) as done: %s", msg.id, err)
			}
		}
	})
	b.Run("pushNReserveNDone", func(b *testing.B) {
		msgs := make([][]byte, b.N)
		for i := range msgs {
			msgs[i] = msg
		}
		queue := client.Queue("queue-benchmark-pushReserveDone")
		defer queue.Close()
		b.SetBytes(2 * int64(len(msg)))
		b.ResetTimer()
		if err := queue.PushN(ctx, msgs); err != nil {
			b.Fatal("cannot push messages:", err)
		}
		reservedMessages, err := queue.ReserveN(ctx, time.Minute, len(msgs))
		if err != nil {
			b.Fatal("cannot reserve message:", err)
		}
		for _, msg := range reservedMessages {
			if err := msg.Done(ctx); err != nil {
				b.Fatalf("cannot mark message (%d) as done: %s", msg.id, err)
			}
		}
	})
	b.Run("pushNReserveNDelete", func(b *testing.B) {
		msgs := make([][]byte, b.N)
		for i := range msgs {
			msgs[i] = msg
		}
		queue := client.Queue("queue-benchmark-push-n-reserve-n-delete")
		defer queue.Close()
		b.SetBytes(2 * int64(len(msg)))
		b.ResetTimer()
		if err := queue.PushN(ctx, msgs); err != nil {
			b.Fatal("cannot push messages:", err)
		}
		reservedMessages, err := queue.ReserveN(ctx, time.Minute, len(msgs))
		if err != nil {
			b.Fatal("cannot reserve message:", err)
		}
		for _, msg := range reservedMessages {
			if err := queue.Delete(ctx, msg.ID()); err != nil {
				b.Fatalf("cannot delete message (%d) as done: %s", msg.ID(), err)
			}
		}
	})
	b.Run("pushNReserveNDeleteN", func(b *testing.B) {
		msgs := make([][]byte, b.N)
		for i := range msgs {
			msgs[i] = msg
		}
		queue := client.Queue("queue-benchmark-push-n-reserve-n-delete-n")
		defer queue.Close()
		b.SetBytes(2 * int64(len(msg)))
		b.ResetTimer()
		if err := queue.PushN(ctx, msgs); err != nil {
			b.Fatal("cannot push messages:", err)
		}
		reservedMessages, err := queue.ReserveN(ctx, time.Minute, len(msgs))
		if err != nil {
			b.Fatal("cannot reserve message:", err)
		}
		ids := make([]uint64, len(reservedMessages))
		for i, msg := range reservedMessages {
			ids[i] = msg.ID()
		}
		if err := queue.DeleteN(ctx, ids); err != nil {
			b.Fatal("cannot delete messages:", err)
		}
	})
}
