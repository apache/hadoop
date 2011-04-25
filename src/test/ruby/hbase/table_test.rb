#
# Copyright 2010 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'hbase'

include HBaseConstants

module Hbase
  # Constructor tests
  class TableConstructorTest < Test::Unit::TestCase
    include TestHelpers
    def setup
      setup_hbase
    end

    define_test "Hbase::Table constructor should fail for non-existent tables" do
      assert_raise(NativeException) do
        table('non-existent-table-name')
      end
    end

    define_test "Hbase::Table constructor should not fail for existent tables" do
      assert_nothing_raised do
        table('.META.')
      end
    end
  end

  # Helper methods tests
  class TableHelpersTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_shell_tests_table"
      create_test_table(@test_name)
      @test_table = table(@test_name)
    end

    define_test "is_meta_table? method should return true for the meta table" do
      assert(table('.META.').is_meta_table?)
    end

    define_test "is_meta_table? method should return true for the root table" do
      assert(table('-ROOT-').is_meta_table?)
    end

    define_test "is_meta_table? method should return false for a normal table" do
      assert(!@test_table.is_meta_table?)
    end

    #-------------------------------------------------------------------------------

    define_test "get_all_columns should return columns list" do
      cols = table('.META.').get_all_columns
      assert_kind_of(Array, cols)
      assert(cols.length > 0)
    end

    #-------------------------------------------------------------------------------

    define_test "parse_column_name should not return a qualifier for name-only column specifiers" do
      col, qual = table('.META.').parse_column_name('foo')
      assert_not_nil(col)
      assert_nil(qual)
    end

    define_test "parse_column_name should not return a qualifier for family-only column specifiers" do
      col, qual = table('.META.').parse_column_name('foo:')
      assert_not_nil(col)
      assert_nil(qual)
    end

    define_test "parse_column_name should return a qualifier for family:qualifier column specifiers" do
      col, qual = table('.META.').parse_column_name('foo:bar')
      assert_not_nil(col)
      assert_not_nil(qual)
    end
  end

  # Simple data management methods tests
  class TableSimpleMethodsTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_shell_tests_table"
      create_test_table(@test_name)
      @test_table = table(@test_name)
    end

    define_test "put should work without timestamp" do
      @test_table.put("123", "x:a", "1")
    end

    define_test "put should work with timestamp" do
      @test_table.put("123", "x:a", "2", Time.now.to_i)
    end

    define_test "put should work with integer keys" do
      @test_table.put(123, "x:a", "3")
    end

    define_test "put should work with integer values" do
      @test_table.put("123", "x:a", 4)
    end

    #-------------------------------------------------------------------------------

    define_test "delete should work without timestamp" do
      @test_table.delete("123", "x:a")
    end

    define_test "delete should work with timestamp" do
      @test_table.delete("123", "x:a", Time.now.to_i)
    end

    define_test "delete should work with integer keys" do
      @test_table.delete(123, "x:a")
    end

    #-------------------------------------------------------------------------------

    define_test "deleteall should work w/o columns and timestamps" do
      @test_table.deleteall("123")
    end

    define_test "deleteall should work with integer keys" do
      @test_table.deleteall(123)
    end

    #-------------------------------------------------------------------------------

    define_test "incr should work w/o value" do
      @test_table.incr("123", 'x:cnt1')
    end

    define_test "incr should work with value" do
      @test_table.incr("123", 'x:cnt2', 10)
    end

    define_test "incr should work with integer keys" do
      @test_table.incr(123, 'x:cnt3')
    end

    #-------------------------------------------------------------------------------

    define_test "get_counter should work with integer keys" do
      @test_table.incr(12345, 'x:cnt')
      assert_kind_of(Fixnum, @test_table.get_counter(12345, 'x:cnt'))
    end

    define_test "get_counter should return nil for non-existent counters" do
      assert_nil(@test_table.get_counter(12345, 'x:qqqq'))
    end
  end

  # Complex data management methods tests
  class TableComplexMethodsTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_shell_tests_table"
      create_test_table(@test_name)
      @test_table = table(@test_name)

      # Test data
      @test_ts = 12345678
      @test_table.put(1, "x:a", 1)
      @test_table.put(1, "x:b", 2, @test_ts)

      @test_table.put(2, "x:a", 11)
      @test_table.put(2, "x:b", 12, @test_ts)
    end

    define_test "count should work w/o a block passed" do
      assert(@test_table.count > 0)
    end

    define_test "count should work with a block passed (and yield)" do
      rows = []
      cnt = @test_table.count(1) do |cnt, row|
        rows << row
      end
      assert(cnt > 0)
      assert(!rows.empty?)
    end

    #-------------------------------------------------------------------------------

    define_test "get should work w/o columns specification" do
      res = @test_table.get('1')
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end

    define_test "get should work with integer keys" do
      res = @test_table.get(1)
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end

    define_test "get should work with hash columns spec and a single string COLUMN parameter" do
      res = @test_table.get('1', COLUMN => 'x:a')
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_nil(res['x:b'])
    end

    define_test "get should work with hash columns spec and a single string COLUMNS parameter" do
      res = @test_table.get('1', COLUMNS => 'x:a')
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_nil(res['x:b'])
    end

    define_test "get should work with hash columns spec and an array of strings COLUMN parameter" do
      res = @test_table.get('1', COLUMN => [ 'x:a', 'x:b' ])
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end

    define_test "get should work with hash columns spec and an array of strings COLUMNS parameter" do
      res = @test_table.get('1', COLUMNS => [ 'x:a', 'x:b' ])
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end

    define_test "get should work with hash columns spec and TIMESTAMP only" do
      res = @test_table.get('1', TIMESTAMP => @test_ts)
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end

    define_test "get should fail with hash columns spec and strange COLUMN value" do
      assert_raise(ArgumentError) do
        @test_table.get('1', COLUMN => {})
      end
    end

    define_test "get should fail with hash columns spec and strange COLUMNS value" do
      assert_raise(ArgumentError) do
        @test_table.get('1', COLUMN => {})
      end
    end

    define_test "get should fail with hash columns spec and no TIMESTAMP or COLUMN[S]" do
      assert_raise(ArgumentError) do
        @test_table.get('1', { :foo => :bar })
      end
    end

    define_test "get should work with a string column spec" do
      res = @test_table.get('1', 'x:b')
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end

    define_test "get should work with an array columns spec" do
      res = @test_table.get('1', 'x:a', 'x:b')
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end

    define_test "get should work with an array or arrays columns spec (yeah, crazy)" do
      res = @test_table.get('1', ['x:a'], ['x:b'])
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end

    define_test "get with a block should yield (column, value) pairs" do
      res = {}
      @test_table.get('1') { |col, val| res[col] = val }
      assert_equal(res.keys.sort, [ 'x:a', 'x:b' ])
    end

    #-------------------------------------------------------------------------------

    define_test "scan should work w/o any params" do
      res = @test_table.scan
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['1'])
      assert_not_nil(res['1']['x:a'])
      assert_not_nil(res['1']['x:b'])
      assert_not_nil(res['2'])
      assert_not_nil(res['2']['x:a'])
      assert_not_nil(res['2']['x:b'])
    end

    define_test "scan should support STARTROW parameter" do
      res = @test_table.scan STARTROW => '2'
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_nil(res['1'])
      assert_not_nil(res['2'])
      assert_not_nil(res['2']['x:a'])
      assert_not_nil(res['2']['x:b'])
    end

    define_test "scan should support STOPROW parameter" do
      res = @test_table.scan STOPROW => '2'
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['1'])
      assert_not_nil(res['1']['x:a'])
      assert_not_nil(res['1']['x:b'])
      assert_nil(res['2'])
    end

    define_test "scan should support LIMIT parameter" do
      res = @test_table.scan LIMIT => 1
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['1'])
      assert_not_nil(res['1']['x:a'])
      assert_not_nil(res['1']['x:b'])
      assert_nil(res['2'])
    end

    define_test "scan should support TIMESTAMP parameter" do
      res = @test_table.scan TIMESTAMP => @test_ts
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['1'])
      assert_nil(res['1']['x:a'])
      assert_not_nil(res['1']['x:b'])
      assert_not_nil(res['2'])
      assert_nil(res['2']['x:a'])
      assert_not_nil(res['2']['x:b'])
    end

    define_test "scan should support TIMERANGE parameter" do
      res = @test_table.scan TIMERANGE => [0, 1]
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_nil(res['1'])
      assert_nil(res['2'])
    end

    define_test "scan should support COLUMNS parameter with an array of columns" do
      res = @test_table.scan COLUMNS => [ 'x:a', 'x:b' ]
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['1'])
      assert_not_nil(res['1']['x:a'])
      assert_not_nil(res['1']['x:b'])
      assert_not_nil(res['2'])
      assert_not_nil(res['2']['x:a'])
      assert_not_nil(res['2']['x:b'])
    end

    define_test "scan should support COLUMNS parameter with a single column name" do
      res = @test_table.scan COLUMNS => 'x:a'
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['1'])
      assert_not_nil(res['1']['x:a'])
      assert_nil(res['1']['x:b'])
      assert_not_nil(res['2'])
      assert_not_nil(res['2']['x:a'])
      assert_nil(res['2']['x:b'])
    end

    define_test "scan should fail on invalid COLUMNS parameter types" do
      assert_raise(ArgumentError) do
        @test_table.scan COLUMNS => {}
      end
    end

    define_test "scan should fail on non-hash params" do
      assert_raise(ArgumentError) do
        @test_table.scan 123
      end
    end

    define_test "scan with a block should yield rows and return rows counter" do
      rows = {}
      res = @test_table.scan { |row, cells| rows[row] = cells }
      assert_equal(rows.keys.size, res)
    end
  end
end
