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

// Package pgqueue is a library allows to use a single PostgreSQL instance as a
// queue server.
//
//	ctx := context.Background()
//	pool, err := pgxpool.New(ctx, dsn)
//	if err != nil {
//		log.Fatalln("cannot open database connection pool:", err)
//	}
//	client := pgqueue.Open(pool)
//	defer client.Close()
//	if err := client.CreateTable(ctx); err != nil {
//		log.Fatalln("cannot create queue table:", err)
//	}
//	queueName := "example-queue-name"
//	if err := client.Push(ctx, queueName, []byte("content")); err != nil {
//		log.Fatalln("cannot push message to queue:", err)
//	}
//	poppedContent, err := client.Pop(ctx, queueName, 1)
//	if err != nil {
//		log.Fatalln("cannot pop message from the queue:", err)
//	}
//	fmt.Printf("content: %s\n", poppedContent[0])
package pgqueue
