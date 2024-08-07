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

package pgqueue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
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
			if gotErr := validDuration(tt.args.d); !errors.Is(gotErr, tt.wantErr) {
				t.Errorf("validDuration() = %v, want %v", gotErr, tt.wantErr)
			}
		})
	}
}

func TestClient_CreateTable_errors(t *testing.T) {
	t.Parallel()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	errExpected := errors.New("mock error")
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnError(errExpected)
	ctx := context.Background()
	client := Open(mock)
	defer client.Close()
	if err := client.CreateTable(ctx); !errors.Is(err, errExpected) {
		t.Fatal("unexpected error:", err)
	}
}

func TestClient_ApproximateCount_errors(t *testing.T) {
	t.Parallel()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	errExpected := errors.New("mock error")
	mock.ExpectQuery("SELECT COUNT").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnError(errExpected)
	ctx := context.Background()
	client := Open(mock)
	defer client.Close()
	if _, err := client.ApproximateCount(ctx, "queue"); !errors.Is(err, errExpected) {
		t.Fatal("unexpected error:", err)
	}
}

func TestClient_PushN_errors(t *testing.T) {
	t.Parallel()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	errExpected := errors.New("mock error")
	mock.ExpectCopyFrom(pgx.Identifier{"queue"}, []string{"queue", "state", "content"}).WillReturnError(errExpected)
	ctx := context.Background()
	client := Open(mock)
	defer client.Close()
	if err := client.Push(ctx, "queue", []byte("content")); !errors.Is(err, errExpected) {
		t.Fatal("unexpected error:", err)
	}
}

func TestClient_ReserveN_errors(t *testing.T) {
	t.Parallel()
	t.Run("queryError", func(t *testing.T) {
		t.Parallel()
		mock, err := pgxmock.NewPool()
		if err != nil {
			t.Fatal(err)
		}
		errExpected := errors.New("mock error")
		mock.ExpectQuery("UPDATE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnError(errExpected)
		ctx := context.Background()
		client := Open(mock)
		defer client.Close()
		if _, err := client.Reserve(ctx, "queue", 1*time.Second, 1); !errors.Is(err, errExpected) {
			t.Fatal("unexpected error:", err)
		}
	})
	t.Run("scanError", func(t *testing.T) {
		t.Parallel()
		mock, err := pgxmock.NewPool()
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery("UPDATE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow("1"))
		ctx := context.Background()
		client := Open(mock)
		defer client.Close()
		if _, err := client.Reserve(ctx, "queue", 1*time.Second, 1); err == nil {
			t.Fatal("expected error missing")
		}
	})
}

func TestClient_ReleaseN_errors(t *testing.T) {
	t.Parallel()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	errExpected := errors.New("mock error")
	mock.ExpectExec("UPDATE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnError(errExpected)
	ctx := context.Background()
	client := Open(mock)
	defer client.Close()
	if err := client.Release(ctx, 1); !errors.Is(err, errExpected) {
		t.Fatal("unexpected error:", err)
	}
}

func TestClient_ExtendN_errors(t *testing.T) {
	t.Parallel()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	errExpected := errors.New("mock error")
	mock.ExpectExec("UPDATE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnError(errExpected)
	ctx := context.Background()
	client := Open(mock)
	defer client.Close()
	if err := client.Extend(ctx, 1*time.Minute, 1); !errors.Is(err, errExpected) {
		t.Fatal("unexpected error:", err)
	}
}

func TestClient_DeleteN_errors(t *testing.T) {
	t.Parallel()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	errExpected := errors.New("mock error")
	mock.ExpectExec("DELETE").WithArgs(pgxmock.AnyArg()).WillReturnError(errExpected)
	ctx := context.Background()
	client := Open(mock)
	defer client.Close()
	if err := client.Delete(ctx, 1); !errors.Is(err, errExpected) {
		t.Fatal("unexpected error:", err)
	}
}

func TestClient_PurgeN_errors(t *testing.T) {
	t.Parallel()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	errExpected := errors.New("mock error")
	mock.ExpectExec("DELETE").WithArgs(pgxmock.AnyArg()).WillReturnError(errExpected)
	ctx := context.Background()
	client := Open(mock)
	defer client.Close()
	if err := client.Purge(ctx, "queue"); !errors.Is(err, errExpected) {
		t.Fatal("unexpected error:", err)
	}
}

func TestClient_PopN_errors(t *testing.T) {
	t.Parallel()
	t.Run("queryError", func(t *testing.T) {
		t.Parallel()
		mock, err := pgxmock.NewPool()
		if err != nil {
			t.Fatal(err)
		}
		errExpected := errors.New("mock error")
		mock.ExpectQuery("UPDATE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnError(errExpected)
		ctx := context.Background()
		client := Open(mock)
		defer client.Close()
		if _, err := client.Pop(ctx, "queue", 1); !errors.Is(err, errExpected) {
			t.Fatal("unexpected error:", err)
		}
	})
	t.Run("scanError", func(t *testing.T) {
		t.Parallel()
		mock, err := pgxmock.NewPool()
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery("UPDATE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnRows(pgxmock.NewRows([]string{}).AddRow())
		ctx := context.Background()
		client := Open(mock)
		defer client.Close()
		if _, err := client.Pop(ctx, "queue", 1); err == nil {
			t.Fatal("expected error missing")
		}
	})
}

func TestClient_DumpDeadLetterQueue_errors(t *testing.T) {
	t.Parallel()
	t.Run("queryError", func(t *testing.T) {
		t.Parallel()
		mock, err := pgxmock.NewPool()
		if err != nil {
			t.Fatal(err)
		}
		errExpected := errors.New("mock error")
		mock.ExpectQuery("SELECT").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnError(errExpected)
		ctx := context.Background()
		client := Open(mock, EnableDeadLetterQueue())
		defer client.Close()
		if _, err := client.DumpDeadLetterQueue(ctx, "queue", 1); !errors.Is(err, errExpected) {
			t.Fatal("unexpected error:", err)
		}
	})
	t.Run("scanError", func(t *testing.T) {
		t.Parallel()
		mock, err := pgxmock.NewPool()
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery("SELECT").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow("1"))
		ctx := context.Background()
		client := Open(mock, EnableDeadLetterQueue())
		defer client.Close()
		if _, err := client.DumpDeadLetterQueue(ctx, "queue", 1); err == nil {
			t.Fatal("expected error missing")
		}
	})
	t.Run("rowsError", func(t *testing.T) {
		t.Parallel()
		mock, err := pgxmock.NewPool()
		if err != nil {
			t.Fatal(err)
		}
		errExpected := errors.New("mock error")
		mock.ExpectQuery("SELECT").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnRows(pgxmock.NewRows([]string{"id", "content"}).RowError(0, errExpected))
		ctx := context.Background()
		client := Open(mock, EnableDeadLetterQueue())
		defer client.Close()
		if _, err := client.DumpDeadLetterQueue(ctx, "queue", 1); !errors.Is(err, errExpected) {
			t.Fatal("unexpected error:", err)
		}
	})
	t.Run("deleteError", func(t *testing.T) {
		t.Parallel()
		mock, err := pgxmock.NewPool()
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery("SELECT").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnRows(pgxmock.NewRows([]string{"id", "content"}).AddRow(uint64(1), []byte("content")))
		errExpected := errors.New("mock error")
		mock.ExpectExec("DELETE").WithArgs(pgxmock.AnyArg()).WillReturnError(errExpected)
		ctx := context.Background()
		client := Open(mock, EnableDeadLetterQueue())
		defer client.Close()
		if _, err := client.DumpDeadLetterQueue(ctx, "queue", 1); !errors.Is(err, errExpected) {
			t.Fatal("unexpected error:", err)
		}
	})
}

func TestClient_vacuum_errors(t *testing.T) {
	t.Run("errDone", func(t *testing.T) {
		t.Parallel()
		mock, err := pgxmock.NewPool()
		if err != nil {
			t.Fatal(err)
		}
		errExpected := errors.New("mock error")
		mock.ExpectExec("DELETE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnError(errExpected)
		ctx := context.Background()
		client := Open(mock)
		defer client.Close()
		if stats := client.Vacuum(ctx); !errors.Is(stats.ErrDone, errExpected) {
			t.Fatal("unexpected error:", stats.ErrDone)
		}
	})
	t.Run("errRestoreStale", func(t *testing.T) {
		t.Parallel()
		mock, err := pgxmock.NewPool()
		if err != nil {
			t.Fatal(err)
		}
		errExpected := errors.New("mock error")
		mock.ExpectExec("DELETE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnResult(pgxmock.NewResult("DELETE", 1))
		mock.ExpectExec("UPDATE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnError(errExpected)
		ctx := context.Background()
		client := Open(mock)
		defer client.Close()
		if stats := client.Vacuum(ctx); !errors.Is(stats.ErrRestoreStale, errExpected) {
			t.Fatal("unexpected error:", stats.ErrRestoreStale)
		}
	})
	t.Run("errBadMessagesDelete", func(t *testing.T) {
		t.Parallel()
		mock, err := pgxmock.NewPool()
		if err != nil {
			t.Fatal(err)
		}
		errExpected := errors.New("mock error")
		mock.ExpectExec("DELETE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnResult(pgxmock.NewResult("DELETE", 1))
		mock.ExpectExec("UPDATE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnResult(pgxmock.NewResult("UPDATE", 1))
		mock.ExpectExec("DELETE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnError(errExpected)
		ctx := context.Background()
		client := Open(mock)
		defer client.Close()
		if stats := client.Vacuum(ctx); !errors.Is(stats.ErrBadMessagesDelete, errExpected) {
			t.Fatal("unexpected error:", stats.ErrBadMessagesDelete)
		}
	})
	t.Run("errDeadLetterQueue", func(t *testing.T) {
		t.Parallel()
		mock, err := pgxmock.NewPool()
		if err != nil {
			t.Fatal(err)
		}
		errExpected := errors.New("mock error")
		mock.ExpectExec("DELETE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnResult(pgxmock.NewResult("DELETE", 1))
		mock.ExpectExec("UPDATE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnResult(pgxmock.NewResult("UPDATE", 1))
		mock.ExpectExec("UPDATE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnError(errExpected)
		ctx := context.Background()
		client := Open(mock, EnableDeadLetterQueue())
		defer client.Close()
		if stats := client.Vacuum(ctx); !errors.Is(stats.ErrDeadLetterQueue, errExpected) {
			t.Fatal("unexpected error:", stats.ErrDeadLetterQueue)
		}
	})
	t.Run("errTableVacuum", func(t *testing.T) {
		t.Parallel()
		mock, err := pgxmock.NewPool()
		if err != nil {
			t.Fatal(err)
		}
		errExpected := errors.New("mock error")
		mock.ExpectExec("DELETE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnResult(pgxmock.NewResult("DELETE", 1))
		mock.ExpectExec("UPDATE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnResult(pgxmock.NewResult("UPDATE", 1))
		mock.ExpectExec("DELETE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnResult(pgxmock.NewResult("DELETE", 1))
		mock.ExpectExec("VACUUM").WillReturnError(errExpected)
		ctx := context.Background()
		client := Open(mock)
		defer client.Close()
		if stats := client.Vacuum(ctx); !errors.Is(stats.ErrTableVacuum, errExpected) {
			t.Fatal("unexpected error:", stats.ErrTableVacuum)
		}
	})
	t.Run("zeroMaxDeliveries", func(t *testing.T) {
		t.Parallel()
		mock, err := pgxmock.NewPool()
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectExec("DELETE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnResult(pgxmock.NewResult("DELETE", 1))
		mock.ExpectExec("UPDATE").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnError(errors.New("mock error"))
		ctx := context.Background()
		client := Open(mock, WithMaxDeliveries(0))
		defer client.Close()
		if stats := client.Vacuum(ctx); stats.Err() != nil {
			t.Fatal("unexpected error:", stats.Err())
		}
	})
}
