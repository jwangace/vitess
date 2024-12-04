/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sync2

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"
)

func TestConsolidator(t *testing.T) {
	con := NewConsolidator()
	sql := "select * from SomeTable"
	second_sql := "select * from AnotherTable"

	want := []ConsolidatorCacheItem{}
	if !reflect.DeepEqual(con.Items(), want) {
		t.Fatalf("expected consolidator to have no items")
	}

	orig, added := con.Create(sql)
	if !added {
		t.Fatalf("expected consolidator to register a new entry")
	}

	if !reflect.DeepEqual(con.Items(), want) {
		t.Fatalf("expected consolidator to still have no items")
	}

	dup, added := con.Create(sql)
	if added {
		t.Fatalf("did not expect consolidator to register a new entry")
	}

	result := &sqltypes.Result{}
	go func() {
		orig.SetResult(result)
		orig.Broadcast()
	}()
	dup.Wait()

	if orig.Result() != result {
		t.Errorf("failed to pass result")
	}
	if orig.Result() != dup.Result() {
		t.Fatalf("failed to share the result")
	}

	want = []ConsolidatorCacheItem{{Query: sql, Count: 1}}
	if !reflect.DeepEqual(con.Items(), want) {
		t.Fatalf("expected consolidator to have one items %v", con.Items())
	}

	// Running the query again should add a new entry since the original
	// query execution completed
	second, added := con.Create(sql)
	if !added {
		t.Fatalf("expected consolidator to register a new entry")
	}

	go func() {
		second.SetResult(result)
		second.Broadcast()
	}()
	dup.Wait()

	want = []ConsolidatorCacheItem{{Query: sql, Count: 2}}
	if !reflect.DeepEqual(con.Items(), want) {
		t.Fatalf("expected consolidator to have two items %v", con.Items())
	}

	if con.WaiterCountOfTotal() != 2 || con.WaiterCountOfQuery(sql) != 2 {
		t.Fatalf("expected consolidator to have WaiterCountOfTotal %v and WaiterCountOfQuery %v", con.WaiterCountOfTotal(), con.WaiterCountOfQuery(sql))
	}

	third, _ := con.Create(second_sql)

	if con.WaiterCountOfTotal() != 2 || con.WaiterCountOfQuery(second_sql) != 0 {
		t.Fatalf("expected consolidator to have WaiterCountOfTotal %v and WaiterCountOfQuery %v", con.WaiterCountOfTotal(), con.WaiterCountOfQuery(second_sql))
	}

	// Remove 2 wait count, prepare for concurrnt tests
	third.RemoveOneCount()
	third.RemoveOneCount()

	// Run same_query_times concurrently for sql and second_sql
	// Expect WaiterCountOfTotal() equals to (same_query_times)
	// Also test WaiterCountOfTotal() is concurrent safe
	same_query_times := 100
	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup
	var wg3 sync.WaitGroup

	go func() {
		for i := 0; i < same_query_times; i++ {
			wg1.Add(1)
			go func(i int) {
				defer wg1.Done()
				this, _ := con.Create(sql)
				go func() {
					this.Wait()
				}()
			}(i)
		}
	}()

	go func() {
		for i := 0; i < same_query_times; i++ {
			wg2.Add(1)
			go func(i int) {
				defer wg2.Done()
				this, _ := con.Create(second_sql)
				go func() {
					this.Wait()
				}()
			}(i)
		}
	}()

	// Test RemoveOneCount() is concurrent safe
	// Remove same_query_times concurrently for second_sql
	go func() {
		for i := 0; i < same_query_times; i++ {
			wg2.Add(1)
			go func(i int) {
				defer wg2.Done()
				this, _ := con.Create(second_sql)
				go func() {
					this.RemoveOneCount()
				}()
			}(i)
		}
	}()

	time.Sleep(500 * time.Millisecond)

	wg1.Wait()
	wg2.Wait()
	wg3.Wait()

	if con.WaiterCountOfTotal() != int64(same_query_times) {
		t.Fatalf("expected consolidator to have WaiterCountOfTotal %v", int64(same_query_times))
	}
}
