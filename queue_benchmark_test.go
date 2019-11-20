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
	"testing"
	"time"
)

func BenchmarkThroughput(b *testing.B) {
	msg := bytes.Repeat([]byte("A"), 65536)
	client, err := Open(dsn)
	if err != nil {
		b.Fatal("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(); err != nil {
		b.Fatal("cannot create queue table:", err)
	}
	b.Run("push", func(b *testing.B) {
		queue := client.Queue("queue-benchmark-push", DisableAutoVacuum())
		defer queue.Close()
		for i := 0; i < b.N; i++ {
			if err := queue.Push(msg); err != nil {
				b.Fatal("cannot push message:", err)
			}
			b.SetBytes(int64(len(msg)))
		}
	})
	b.Run("pop", func(b *testing.B) {
		queue := client.Queue("queue-benchmark-pop", DisableAutoVacuum())
		defer queue.Close()
		for i := 0; i < b.N; i++ {
			if err := queue.Push(msg); err != nil {
				b.Fatal("cannot push message:", err)
			}
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := queue.Pop(); err != nil {
				b.Fatal("cannot pop message:", err)
			}
			b.SetBytes(int64(len(msg)))
		}
	})
	b.Run("pushPop", func(b *testing.B) {
		queue := client.Queue("queue-benchmark-pushPop", DisableAutoVacuum())
		defer queue.Close()
		for i := 0; i < b.N; i++ {
			if err := queue.Push(msg); err != nil {
				b.Fatal("cannot push message:", err)
			}
			b.SetBytes(int64(len(msg)))
		}
		for i := 0; i < b.N; i++ {
			if _, err := queue.Pop(); err != nil {
				b.Fatal("cannot pop message:", err)
			}
			b.SetBytes(int64(len(msg)))
		}
	})
	b.Run("pushReserveDone", func(b *testing.B) {
		queue := client.Queue("queue-benchmark-pushReserveDone", DisableAutoVacuum())
		defer queue.Close()
		for i := 0; i < b.N; i++ {
			if err := queue.Push(msg); err != nil {
				b.Fatal("cannot push message:", err)
			}
			b.SetBytes(int64(len(msg)))
		}
		for i := 0; i < b.N; i++ {
			msg, err := queue.Reserve(time.Minute)
			if err != nil {
				b.Fatal("cannot reserve message:", err)
			}
			b.SetBytes(int64(len(msg.Content)))
			if err := msg.Done(); err != nil {
				b.Fatalf("cannot mark message (%d) as done: %s", msg.id, err)
			}
		}
	})
}
