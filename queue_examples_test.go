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
	"database/sql"
	"fmt"
	"log"

	"cirello.io/pgqueue"
	_ "github.com/lib/pq"
)

func Example_basic() {
	db, err := sql.Open("postgres", "postgres://postgres:mysecretpassword@localhost:5412/postgres?sslmode=disable")
	if err != nil {
		log.Fatalln("cannot dial to the database:", err)
	}
	defer db.Close()
	queue := pgqueue.Open(db)
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
	db, err := sql.Open("postgres", "postgres://postgres:mysecretpassword@localhost:5412/postgres?sslmode=disable")
	if err != nil {
		log.Fatalln("cannot dial to the database:", err)
	}
	defer db.Close()
	queue := pgqueue.Open(db)
	if err := queue.CreateTable(); err != nil {
		log.Fatalln("cannot create queue table:", err)
	}
	_, err = queue.Pop("empty-name")
	fmt.Println("err:", err)
	// Output:
	// err: empty queue
}
