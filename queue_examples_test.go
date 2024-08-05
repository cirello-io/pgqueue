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
	client := pgqueue.Open(pool)
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queueName := "example-queue-name"
	if err := client.Push(ctx, queueName, []byte("content")); err != nil {
		log.Fatalln("cannot push message to queue:", err)
	}
	poppedContent, err := client.Pop(ctx, queueName, 1)
	if err != nil {
		log.Fatalln("cannot pop message from the queue:", err)
	}
	fmt.Printf("content: %s\n", poppedContent[0])
	// Output:
	// content: content
}

func Example_emptyQueue() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalln("cannot open database connection pool:", err)
	}
	client := pgqueue.Open(pool)
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	_, err = client.Pop(ctx, "empty-name", 1)
	fmt.Println("err:", err)
	// Output:
	// err: empty queue
}

func Example_reservation() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalln("cannot open database connection pool:", err)
	}
	client := pgqueue.Open(pool)
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queueName := "example-queue-reservation"
	if err := client.Push(ctx, queueName, []byte("content")); err != nil {
		log.Fatalln("cannot push message to queue:", err)
	}
	msgs, err := client.Reserve(ctx, queueName, 1*time.Minute, 1)
	if err != nil {
		log.Fatalln("cannot reserve message from the queue:", err)
	}
	fmt.Printf("content: %s\n", msgs[0].Content())
	if err := client.Delete(ctx, msgs[0].ID()); err != nil {
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
	client := pgqueue.Open(pool)
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queueName := "example-queue-release"
	if err := client.Push(ctx, queueName, []byte("content")); err != nil {
		log.Fatalln("cannot push message to queue:", err)
	}
	msgs, err := client.Reserve(ctx, queueName, 1*time.Minute, 1)
	if err != nil {
		log.Fatalln("cannot pop message from the queue:", err)
	}
	fmt.Printf("content: %s\n", msgs[0].Content())
	if err := client.Release(ctx, msgs[0].ID()); err != nil {
		log.Fatalln("cannot release the message back to the queue:", err)
	}
	// Output:
	// content: content
}

func Example_reservedReleasedDeleted() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalln("cannot open database connection pool:", err)
	}
	client := pgqueue.Open(pool)
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queueName := "example-queue-release"
	if err := client.Push(ctx, queueName, []byte("content")); err != nil {
		log.Fatalln("cannot push message to queue:", err)
	}
	msgs, err := client.Reserve(ctx, queueName, 1*time.Minute, 1)
	if err != nil {
		log.Fatalln("cannot pop message from the queue:", err)
	}
	fmt.Printf("content: %s\n", msgs[0].Content())
	if err := client.Delete(ctx, msgs[0].ID()); err != nil {
		log.Fatalln("cannot remove the message from the queue:", err)
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
	client := pgqueue.Open(pool)
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queueName := "example-queue-touch"
	if err := client.Push(ctx, queueName, []byte("content")); err != nil {
		log.Fatalln("cannot push message to queue:", err)
	}
	msgs, err := client.Reserve(ctx, queueName, 10*time.Second, 1)
	if err != nil {
		log.Fatalln("cannot pop message from the queue:", err)
	}
	fmt.Printf("content: %s\n", msgs[0].Content())
	time.Sleep(5 * time.Second)
	if err := client.Extend(ctx, 1*time.Minute, msgs[0].ID()); err != nil {
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
	client := pgqueue.Open(pool, pgqueue.WithMaxDeliveries(1), pgqueue.DisableAutoVacuum())
	defer client.Close()
	if err := client.CreateTable(ctx); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queueName := "example-queue-vacuum"
	for i := 0; i < 10; i++ {
		if err := client.Push(ctx, queueName, []byte("content")); err != nil {
			log.Fatalln("cannot push message to queue:", err)
		}
		if _, err := client.Pop(ctx, queueName, 1); err != nil {
			log.Fatalln("cannot pop message from the queue:", err)
		}
	}
	stats := client.Vacuum(ctx)
	if err := stats.Err(); err != nil {
		log.Fatalln("cannot clean up:", err)
	}
	fmt.Println("vacuum succeeded")
	// Output:
	// vacuum succeeded
}
