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

// Package pgqueue is a library allows to use a single PostgreSQL instance as a
// low-throughput queue server.
//
//     client, err := pgqueue.Open(dsn)
//     if err != nil {
//         log.Fatalln("cannot open database connection:", err)
//     }
//     defer client.Close()
//     if err := client.CreateTable(); err != nil {
//         log.Fatalln("cannot create queue table:", err)
//     }
//     queue := client.Queue("example-queue-reservation")
//     defer queue.Close()
//     content := []byte("content")
//     if err := queue.Push(content); err != nil {
//         log.Fatalln("cannot push message to queue:", err)
//     }
//     r, err := queue.Reserve(1 * time.Minute)
//     if err != nil {
//         log.Fatalln("cannot reserve message from the queue:", err)
//     }
//     fmt.Printf("content: %s\n", r.Content)
//     if err := r.Done(); err != nil {
//         log.Fatalln("cannot mark message as done:", err)
//     }
//
package pgqueue
