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
	"errors"
	"io/ioutil"
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
		client, err := Open(dsn, DisableAutoVacuum())
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
	t.Run("bad close", func(t *testing.T) {
		client, mock := setup()
		badClose := errors.New("cannot close")
		mock.ExpectClose().WillReturnError(badClose)
		err := client.Close()
		if !errors.Is(err, badClose) {
			t.Errorf("expected closer error missing: %v", err)
		}
		t.Log("got:", err)
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet expectation error: %s", err)
		}
	})
	t.Run("push", func(t *testing.T) {
		t.Run("bad TX", func(t *testing.T) {
			client, mock := setup()
			badTx := errors.New("cannot start transaction")
			mock.ExpectBegin().WillReturnError(badTx)
			q := client.Queue("queue")
			defer q.Close()
			if err := q.Push(nil); !errors.Is(err, badTx) {
				t.Errorf("expected error not found: %s", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectation error: %s", err)
			}
		})
		t.Run("bad exec", func(t *testing.T) {
			client, mock := setup()
			badExec := errors.New("cannot insert message")
			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO").WillReturnError(badExec)
			q := client.Queue("queue")
			defer q.Close()
			if err := q.Push(nil); !errors.Is(err, badExec) {
				t.Errorf("expected error not found: %s", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectation error: %s", err)
			}
		})
		t.Run("bad notify", func(t *testing.T) {
			client, mock := setup()
			badExec := errors.New("cannot dispatch notification")
			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("NOTIFY").WillReturnError(badExec)
			q := client.Queue("queue")
			defer q.Close()
			if err := q.Push(nil); !errors.Is(err, badExec) {
				t.Errorf("expected error not found: %s", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectation error: %s", err)
			}
		})
	})
	t.Run("dump dead letter queue", func(t *testing.T) {
		t.Run("bad query", func(t *testing.T) {
			client, mock := setup()
			badQuery := errors.New("cannot run query")
			mock.ExpectQuery("SELECT id, content").WillReturnError(badQuery)
			if err := client.DumpDeadLetterQueue("queue", ioutil.Discard); !errors.Is(err, badQuery) {
				t.Errorf("expected error not found: %s", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectation error: %s", err)
			}
		})
		t.Run("bad scan", func(t *testing.T) {
			client, mock := setup()
			mock.ExpectQuery("SELECT id, content").
				WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(0)).
				RowsWillBeClosed()
			if err := client.DumpDeadLetterQueue("queue", ioutil.Discard); err == nil {
				t.Errorf("expected error not found: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectation error: %s", err)
			}
		})
		t.Run("bad exec", func(t *testing.T) {
			client, mock := setup()
			badDelete := errors.New("cannot delete row")
			mock.ExpectQuery("SELECT id, content").
				WillReturnRows(sqlmock.NewRows([]string{"id", "content"}).AddRow(0, []byte("content")))
			mock.ExpectExec("DELETE FROM").WillReturnError(badDelete)
			if err := client.DumpDeadLetterQueue("queue", ioutil.Discard); !errors.Is(err, badDelete) {
				t.Errorf("expected error not found: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectation error: %s", err)
			}
		})
	})
}
