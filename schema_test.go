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
	"os"
	"strings"
	"testing"
)

func TestValidateSchemaSQL(t *testing.T) {
	t.Parallel()
	c := &Client{tableName: defaultTableName}
	current, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("cannot read schema.sql: %v", err)
	}
	renderedCreateTable := strings.TrimSpace(c.renderCreateTable()) + "\n"
	if string(current) != renderedCreateTable {
		t.Logf("%s (%d)", current, len(current))
		t.Logf("%s (%d)", renderedCreateTable, len(renderedCreateTable))
		t.Fatalf("schema mismatch")
	}
}
