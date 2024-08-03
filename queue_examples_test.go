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

package pgqueue_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"cirello.io/pgqueue"
	"github.com/jackc/pgx/v5/pgxpool"
)

var dsn = os.Getenv("PGQUEUE_TEST_DSN")

func Example_basic() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalln("cannot open database connection pool:", err)
	}
	client, err := pgqueue.Open(ctx, pool)
	if err != nil {
		log.Fatalln("cannot create queue handler:", err)
	}
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queue := client.Queue("example-queue-name")
	defer queue.Close()
	content := []byte("content")
	if err := queue.Push(ctx, content); err != nil {
		log.Fatalln("cannot push message to queue:", err)
	}
	poppedContent, err := queue.Pop(ctx)
	if err != nil {
		log.Fatalln("cannot pop message from the queue:", err)
	}
	fmt.Printf("content: %s\n", poppedContent)
	// Output:
	// content: content
}

func Example_emptyQueue() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalln("cannot open database connection pool:", err)
	}
	client, err := pgqueue.Open(ctx, pool)
	if err != nil {
		log.Fatalln("cannot create queue handler:", err)
	}
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queue := client.Queue("empty-name")
	defer queue.Close()
	_, err = queue.Pop(ctx)
	fmt.Println("err:", err)
	// Output:
	// err: empty queue
}

func Example_listen() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalln("cannot open database connection pool:", err)
	}
	client, err := pgqueue.Open(ctx, pool)
	if err != nil {
		log.Fatalln("cannot create queue handler:", err)
	}
	defer client.Close()
	queue := client.Queue("example-queue-listen")
	defer queue.Close()
	go queue.Push(ctx, []byte("content"))
	watch := queue.Watch(time.Minute)
	for watch.Next(ctx) {
		msg := watch.Message()
		fmt.Printf("msg: %s\n", msg.Content)
		msg.Done(ctx)
		queue.Close()
	}
	if err := watch.Err(); err != nil && err != pgqueue.ErrAlreadyClosed {
		log.Fatalln("cannot observe queue:", err)
	}
	// Output:
	// msg: content
}

func Example_reservation() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalln("cannot open database connection pool:", err)
	}
	client, err := pgqueue.Open(ctx, pool)
	if err != nil {
		log.Fatalln("cannot create queue handler:", err)
	}
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queue := client.Queue("example-queue-reservation")
	defer queue.Close()
	content := []byte("content")
	if err := queue.Push(ctx, content); err != nil {
		log.Fatalln("cannot push message to queue:", err)
	}
	r, err := queue.Reserve(ctx, 1*time.Minute)
	if err != nil {
		log.Fatalln("cannot reserve message from the queue:", err)
	}
	fmt.Printf("content: %s\n", r.Content)
	if err := r.Done(ctx); err != nil {
		log.Fatalln("cannot mark message as done:", err)
	}
	// Output:
	// content: content
}

func Example_reservedReleased() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalln("cannot open database connection pool:", err)
	}
	client, err := pgqueue.Open(ctx, pool)
	if err != nil {
		log.Fatalln("cannot create queue handler:", err)
	}
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queue := client.Queue("example-queue-release")
	defer queue.Close()
	content := []byte("content")
	if err := queue.Push(ctx, content); err != nil {
		log.Fatalln("cannot push message to queue:", err)
	}
	r, err := queue.Reserve(ctx, 1*time.Minute)
	if err != nil {
		log.Fatalln("cannot pop message from the queue:", err)
	}
	fmt.Printf("content: %s\n", r.Content)
	if err := r.Release(ctx); err != nil {
		log.Fatalln("cannot release the message back to the queue:", err)
	}
	// Output:
	// content: content
}

func Example_reservedTouch() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalln("cannot open database connection pool:", err)
	}
	client, err := pgqueue.Open(ctx, pool)
	if err != nil {
		log.Fatalln("cannot create queue handler:", err)
	}
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queue := client.Queue("example-queue-touch")
	defer queue.Close()
	content := []byte("content")
	if err := queue.Push(ctx, content); err != nil {
		log.Fatalln("cannot push message to queue:", err)
	}
	r, err := queue.Reserve(ctx, 10*time.Second)
	if err != nil {
		log.Fatalln("cannot pop message from the queue:", err)
	}
	fmt.Printf("content: %s\n", r.Content)
	time.Sleep(5 * time.Second)
	if err := r.Touch(ctx, 1*time.Minute); err != nil {
		log.Fatalln("cannot extend message lease:", err)
	}
	// Output:
	// content: content
}

func Example_vacuum() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalln("cannot open database connection pool:", err)
	}
	client, err := pgqueue.Open(ctx, pool, pgqueue.WithMaxDeliveries(1), pgqueue.DisableAutoVacuum())
	if err != nil {
		log.Fatalln("cannot create queue handler:", err)
	}
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queue := client.Queue("example-queue-vacuum")
	defer queue.Close()
	for i := 0; i < 10; i++ {
		content := []byte("content")
		if err := queue.Push(ctx, content); err != nil {
			log.Fatalln("cannot push message to queue:", err)
		}
		if _, err := queue.Pop(ctx); err != nil {
			log.Fatalln("cannot pop message from the queue:", err)
		}
	}
	client.Vacuum(ctx)
	stats := queue.VacuumStats()
	if err := stats.Err; err != nil {
		log.Fatalln("cannot clean up queue:", err)
	}
}
