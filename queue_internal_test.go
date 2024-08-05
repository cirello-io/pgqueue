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
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	errExpected := errors.New("mock error")
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnError(errExpected)
	ctx := context.Background()
	client, err := Open(ctx, mock)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.CreateTable(ctx); !errors.Is(err, errExpected) {
		t.Fatal("unexpected error:", err)
	}
}
