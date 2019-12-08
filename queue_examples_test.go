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
	"bytes"
	"fmt"
	"log"
	"os"
	"time"

	"cirello.io/pgqueue"
	_ "github.com/lib/pq"
)

var dsn = os.Getenv("PGQUEUE_TEST_DSN")

func Example_basic() {
	client, err := pgqueue.Open(dsn)
	if err != nil {
		log.Fatalln("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queue := client.Queue("example-queue-name")
	defer queue.Close()
	content := []byte("content")
	if err := queue.Push(content); err != nil {
		log.Fatalln("cannot push message to queue:", err)
	}
	poppedContent, err := queue.Pop()
	if err != nil {
		log.Fatalln("cannot pop message from the queue:", err)
	}
	fmt.Printf("content: %s\n", poppedContent)
	// Output:
	// content: content
}

func Example_emptyQueue() {
	client, err := pgqueue.Open(dsn)
	if err != nil {
		log.Fatalln("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queue := client.Queue("empty-name")
	defer queue.Close()
	_, err = queue.Pop()
	fmt.Println("err:", err)
	// Output:
	// err: empty queue
}

func Example_largeMessage() {
	client, err := pgqueue.Open(dsn)
	if err != nil {
		log.Fatalln("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queue := client.Queue("example-queue-large-message")
	defer queue.Close()
	content := bytes.Repeat([]byte{0}, pgqueue.MaxMessageLength+1)
	err = queue.Push(content)
	fmt.Println("err:", err)
	// Output:
	// err: message is too large
}

func Example_listen() {
	client, err := pgqueue.Open(dsn)
	if err != nil {
		log.Fatalln("cannot open database connection:", err)
	}
	defer client.Close()
	queue := client.Queue("example-queue-listen")
	defer queue.Close()
	go queue.Push([]byte("content"))
	watch := queue.Watch(time.Minute)
	for watch.Next() {
		msg := watch.Message()
		fmt.Printf("msg: %s\n", msg.Content)
		msg.Done()
		queue.Close()
	}
	if err := watch.Err(); err != nil && err != pgqueue.ErrAlreadyClosed {
		log.Fatalln("cannot observe queue:", err)
	}
	// Output:
	// msg: content
}

func Example_reservation() {
	client, err := pgqueue.Open(dsn)
	if err != nil {
		log.Fatalln("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queue := client.Queue("example-queue-reservation")
	defer queue.Close()
	content := []byte("content")
	if err := queue.Push(content); err != nil {
		log.Fatalln("cannot push message to queue:", err)
	}
	r, err := queue.Reserve(1 * time.Minute)
	if err != nil {
		log.Fatalln("cannot reserve message from the queue:", err)
	}
	fmt.Printf("content: %s\n", r.Content)
	if err := r.Done(); err != nil {
		log.Fatalln("cannot mark message as done:", err)
	}
	// Output:
	// content: content
}

func Example_reservedReleased() {
	client, err := pgqueue.Open(dsn)
	if err != nil {
		log.Fatalln("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queue := client.Queue("example-queue-release")
	defer queue.Close()
	content := []byte("content")
	if err := queue.Push(content); err != nil {
		log.Fatalln("cannot push message to queue:", err)
	}
	r, err := queue.Reserve(1 * time.Minute)
	if err != nil {
		log.Fatalln("cannot pop message from the queue:", err)
	}
	fmt.Printf("content: %s\n", r.Content)
	if err := r.Release(); err != nil {
		log.Fatalln("cannot release the message back to the queue:", err)
	}
	// Output:
	// content: content
}

func Example_reservedTouch() {
	client, err := pgqueue.Open(dsn)
	if err != nil {
		log.Fatalln("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queue := client.Queue("example-queue-touch")
	defer queue.Close()
	content := []byte("content")
	if err := queue.Push(content); err != nil {
		log.Fatalln("cannot push message to queue:", err)
	}
	r, err := queue.Reserve(10 * time.Second)
	if err != nil {
		log.Fatalln("cannot pop message from the queue:", err)
	}
	fmt.Printf("content: %s\n", r.Content)
	time.Sleep(5 * time.Second)
	if err := r.Touch(1 * time.Minute); err != nil {
		log.Fatalln("cannot extend message lease:", err)
	}
	// Output:
	// content: content
}

func Example_vacuum() {
	const reservationTime = 500 * time.Millisecond
	client, err := pgqueue.Open(dsn, pgqueue.WithMaxDeliveries(1), pgqueue.DisableAutoVacuum())
	if err != nil {
		log.Fatalln("cannot open database connection:", err)
	}
	defer client.Close()
	if err := client.CreateTable(); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	queue := client.Queue("example-queue-vacuum")
	defer queue.Close()
	for i := 0; i < 10; i++ {
		content := []byte("content")
		if err := queue.Push(content); err != nil {
			log.Fatalln("cannot push message to queue:", err)
		}
		if _, err := queue.Pop(); err != nil {
			log.Fatalln("cannot pop message from the queue:", err)
		}
	}
	client.Vacuum()
	stats := queue.VacuumStats()
	if err := stats.Err; err != nil {
		log.Fatalln("cannot clean up queue:", err)
	}
}
