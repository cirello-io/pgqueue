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
	"github.com/lib/pq"
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
	t.Run("queue", func(t *testing.T) {
		t.Run("bad TX", func(t *testing.T) {
			client, mock := setup()
			q := client.Queue("queue")
			badTx := errors.New("transaction begin error")
			mock.ExpectBegin().WillReturnError(badTx)
			if err := q.Push([]byte("msg")); !errors.Is(err, badTx) {
				t.Errorf("expected TX error missing: %v", err)
			}
		})
		t.Run("bad INSERT", func(t *testing.T) {
			client, mock := setup()
			q := client.Queue("queue")
			badInsert := errors.New("insert error")
			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO").WillReturnError(badInsert)
			if err := q.Push([]byte("msg")); !errors.Is(err, badInsert) {
				t.Errorf("expected exec error missing: %v", err)
			}
		})
		t.Run("bad NOTIFY", func(t *testing.T) {
			client, mock := setup()
			q := client.Queue("queue")
			badNotify := errors.New("notify error")
			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec("NOTIFY").WillReturnError(badNotify)
			if err := q.Push([]byte("msg")); !errors.Is(err, badNotify) {
				t.Errorf("expected exec error missing: %v", err)
			}
		})
		t.Run("bad commit", func(t *testing.T) {
			client, mock := setup()
			q := client.Queue("queue")
			badCommit := errors.New("notify error")
			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec("NOTIFY").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectCommit().WillReturnError(badCommit)
			if err := q.Push([]byte("msg")); !errors.Is(err, badCommit) {
				t.Errorf("expected commit error missing: %v", err)
			}
		})
		t.Run("deadlocked commit", func(t *testing.T) {
			client, mock := setup()
			q := client.Queue("queue")
			deadLockCommit := &pq.Error{Code: "40001"}
			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec("NOTIFY").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectCommit().WillReturnError(deadLockCommit)
			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec("NOTIFY").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectCommit()
			if err := q.Push([]byte("msg")); err != nil {
				t.Errorf("unexpected commit error: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectation error: %s", err)
			}
		})
	})
	t.Run("vacuum", func(t *testing.T) {
		t.Run("bad DELETE", func(t *testing.T) {
			client, mock := setup()
			q := client.Queue("queue", DisableAutoVacuum())
			badDelete := errors.New("delete error")
			mock.ExpectBegin()
			mock.ExpectExec("DELETE FROM").WillReturnError(badDelete)
			if stats := q.Vacuum(); !errors.Is(stats.Err, badDelete) {
				t.Errorf("expected exec error missing: %v", stats.Err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error("mock expectation error:", err)
			}
		})
		t.Run("bad DELETE result", func(t *testing.T) {
			client, mock := setup()
			q := client.Queue("queue", DisableAutoVacuum())
			badDeleteResult := errors.New("delete result error")
			mock.ExpectBegin()
			mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewErrorResult(badDeleteResult))
			if stats := q.Vacuum(); !errors.Is(stats.Err, badDeleteResult) {
				t.Errorf("expected exec error missing: %v", stats.Err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error("mock expectation error:", err)
			}
		})
		t.Run("bad 1st UPDATE", func(t *testing.T) {
			client, mock := setup()
			q := client.Queue("queue", DisableAutoVacuum())
			badUpdate := errors.New("update error")
			mock.ExpectBegin()
			mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("UPDATE \"queue\"").WillReturnError(badUpdate)
			if stats := q.Vacuum(); !errors.Is(stats.Err, badUpdate) {
				t.Errorf("expected exec error missing: %v", stats.Err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error("mock expectation error:", err)
			}
		})
		t.Run("bad 1st UPDATE result", func(t *testing.T) {
			client, mock := setup()
			q := client.Queue("queue", DisableAutoVacuum())
			badUpdateResult := errors.New("update result error")
			mock.ExpectBegin()
			mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("UPDATE \"queue\"").WillReturnResult(sqlmock.NewErrorResult(badUpdateResult))
			if stats := q.Vacuum(); !errors.Is(stats.Err, badUpdateResult) {
				t.Errorf("expected exec error missing: %v", stats.Err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error("mock expectation error:", err)
			}
		})
		t.Run("bad 2nd UPDATE", func(t *testing.T) {
			client, mock := setup()
			q := client.Queue("queue", DisableAutoVacuum())
			badUpdate := errors.New("update error")
			mock.ExpectBegin()
			mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("UPDATE \"queue\"").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("UPDATE \"queue\"").WillReturnError(badUpdate)
			if stats := q.Vacuum(); !errors.Is(stats.Err, badUpdate) {
				t.Errorf("expected exec error missing: %v", stats.Err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error("mock expectation error:", err)
			}
		})
		t.Run("bad 2nd UPDATE result", func(t *testing.T) {
			client, mock := setup()
			q := client.Queue("queue", DisableAutoVacuum())
			badUpdateResult := errors.New("update result error")
			mock.ExpectBegin()
			mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("UPDATE \"queue\"").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("UPDATE \"queue\"").WillReturnResult(sqlmock.NewErrorResult(badUpdateResult))
			if stats := q.Vacuum(); !errors.Is(stats.Err, badUpdateResult) {
				t.Errorf("expected exec error missing: %v", stats.Err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error("mock expectation error:", err)
			}
		})
		t.Run("bad transaction", func(t *testing.T) {
			client, mock := setup()
			q := client.Queue("queue", DisableAutoVacuum())
			badCommit := errors.New("update result error")
			mock.ExpectBegin()
			mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("UPDATE \"queue\"").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("UPDATE \"queue\"").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit().WillReturnError(badCommit)
			if stats := q.Vacuum(); !errors.Is(stats.Err, badCommit) {
				t.Errorf("expected exec error missing: %v", stats.Err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error("mock expectation error:", err)
			}
		})
		t.Run("full transaction", func(t *testing.T) {
			client, mock := setup()
			q := client.Queue("queue", DisableAutoVacuum())
			mock.ExpectBegin()
			mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("UPDATE \"queue\"").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("UPDATE \"queue\"").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()
			if stats := q.Vacuum(); stats.Err != nil {
				t.Errorf("unexpected error: %v", stats.Err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error("mock expectation error:", err)
			}
		})
	})
}
