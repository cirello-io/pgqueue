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
	"time"

	"cirello.io/pgqueue"
	_ "github.com/lib/pq"
)

func Example_basic() {
	queue, err := pgqueue.Open(*dsn)
	if err != nil {
		log.Fatalln("cannot open database connection:", err)
	}
	if err := queue.CreateTable(); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	content := []byte("content")
	if err := queue.Push("queue-name", content); err != nil {
		log.Fatalln("cannot push message to queue:", err)
	}
	poppedContent, err := queue.Pop("queue-name")
	if err != nil {
		log.Fatalln("cannot pop message from the queue:", err)
	}
	fmt.Printf("content: %s\n", poppedContent)
	// Output:
	// content: content
}

func Example_emptyQueue() {
	queue, err := pgqueue.Open(*dsn)
	if err != nil {
		log.Fatalln("cannot open database connection:", err)
	}
	if err := queue.CreateTable(); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	_, err = queue.Pop("empty-name")
	fmt.Println("err:", err)
	// Output:
	// err: empty queue
}

func Example_largeMessage() {
	queue, err := pgqueue.Open(*dsn)
	if err != nil {
		log.Fatalln("cannot open database connection:", err)
	}
	if err := queue.CreateTable(); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	content := bytes.Repeat([]byte{0}, pgqueue.MaxMessageLength+1)
	err = queue.Push("queue-large-message", content)
	fmt.Println("err:", err)
	// Output:
	// err: message is too large
}

func Example_listen() {
	queue, err := pgqueue.Open(*dsn)
	if err != nil {
		log.Fatalln("cannot open database connection:", err)
	}
	go queue.Push("queue-listen", []byte("content"))
	watch := queue.Watch("queue-listen")
	for watch.Next() {
		fmt.Printf("msg: %s\n", watch.Message())
		queue.Close()
	}
	if err := watch.Err(); err != nil {
		log.Fatalln("cannot observe queue:", err)
	}
	// Output:
	// msg: content
}

func Example_reservation() {
	queue, err := pgqueue.Open(*dsn)
	if err != nil {
		log.Fatalln("cannot open database connection:", err)
	}
	if err := queue.CreateTable(); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	content := []byte("content")
	if err := queue.Push("queue-reservation", content); err != nil {
		log.Fatalln("cannot push message to queue:", err)
	}
	r, err := queue.Reserve("queue-reservation", 1*time.Minute)
	if err != nil {
		log.Fatalln("cannot pop message from the queue:", err)
	}
	fmt.Printf("content: %s\n", r.Content)
	if err := r.Done(); err != nil {
		log.Fatalln("cannot mark message as done:", err)
	}
	// Output:
	// content: content
}

func Example_reservedReleased() {
	queue, err := pgqueue.Open(*dsn)
	if err != nil {
		log.Fatalln("cannot open database connection:", err)
	}
	if err := queue.CreateTable(); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	content := []byte("content")
	if err := queue.Push("queue-release", content); err != nil {
		log.Fatalln("cannot push message to queue:", err)
	}
	r, err := queue.Reserve("queue-release", 1*time.Minute)
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
	queue, err := pgqueue.Open(*dsn)
	if err != nil {
		log.Fatalln("cannot open database connection:", err)
	}
	if err := queue.CreateTable(); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	content := []byte("content")
	if err := queue.Push("queue-touch", content); err != nil {
		log.Fatalln("cannot push message to queue:", err)
	}
	r, err := queue.Reserve("queue-touch", 10*time.Second)
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
	queue, err := pgqueue.Open(*dsn)
	if err != nil {
		log.Fatalln("cannot open database connection:", err)
	}
	if err := queue.CreateTable(); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	for i := 0; i < 10; i++ {
		content := []byte("content")
		if err := queue.Push("queue-vacuum", content); err != nil {
			log.Fatalln("cannot push message to queue:", err)
		}
		if _, err := queue.Pop("queue-vacuum"); err != nil {
			log.Fatalln("cannot pop message from the queue:", err)
		}
	}
	for i := 0; i < 10; i++ {
		content := []byte("content")
		if err := queue.Push("queue-vacuum", content); err != nil {
			log.Fatalln("cannot push message to queue:", err)
		}
		if _, err := queue.Reserve("queue-vacuum", time.Second); err != nil {
			log.Fatalln("cannot reserve message from the queue:", err)
		}
	}
	time.Sleep(time.Second)
	stats, err := queue.Vacuum("queue-vacuum")
	if err != nil {
		log.Fatalln("cannot clean up queue:", err)
	}
	fmt.Println("done message count:", stats.Done)
	fmt.Println("dead message count:", stats.Deads)
	// Output:
	// done message count: 10
	// dead message count: 10
}
