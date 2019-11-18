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
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func Test_validDuration(t *testing.T) {
	type args struct {
		d time.Duration
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{"good", args{time.Second}, nil},
		{"bad", args{time.Second + time.Nanosecond}, ErrInvalidDuration},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotErr := validDuration(tt.args.d); gotErr != tt.wantErr {
				t.Errorf("validDuration() = %v, want %v", gotErr, tt.wantErr)
			}
		})
	}
}

func TestDBErrorHandling(t *testing.T) {
	setup := func() (*Client, sqlmock.Sqlmock) {
		client, err := Open(dsn)
		if err != nil {
			t.Fatal("cannot open database connection:", err)
		}
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal("cannot create mock:", err)
		}
		client.db = db
		return client, mock
	}
	t.Run("dump dead letter queue", func(t *testing.T) {
		t.Run("bad select", func(t *testing.T) {
			client, mock := setup()
			badQuery := errors.New("cannot run SELECT")
			mock.ExpectQuery("SELECT id, content FROM").WillReturnError(badQuery)
			if err := client.DumpDeadLetterQueue("some bad queue", nil); !errors.Is(err, badQuery) {
				t.Errorf("expected SELECT error missing: %v", err)
			}
		})
		t.Run("bad row", func(t *testing.T) {
			client, mock := setup()
			mock.ExpectQuery("SELECT id, content FROM").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(0))
			var buf bytes.Buffer
			if err := client.DumpDeadLetterQueue("some bad queue", &buf); err == nil {
				t.Errorf("expected Scan error missing: %v", err)
			}
		})
		t.Run("bad delete", func(t *testing.T) {
			client, mock := setup()
			mock.ExpectQuery("SELECT id, content FROM").WillReturnRows(sqlmock.NewRows([]string{"id", "content"}).AddRow(1, "dead message"))
			badDelete := errors.New("cannot execute DELETE")
			mock.ExpectExec("DELETE").WillReturnError(badDelete)
			var buf bytes.Buffer
			if err := client.DumpDeadLetterQueue("some bad queue", &buf); !errors.Is(err, badDelete) {
				t.Errorf("expected Exec error missing: %v", err)
			}
		})
	})
}
