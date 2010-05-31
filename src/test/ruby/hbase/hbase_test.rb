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

module Hbase
  class HbaseTest < Test::Unit::TestCase
    def setup
      @formatter = Shell::Formatter::Console.new()
      @hbase = ::Hbase::Hbase.new($TEST_CLUSTER.getConfiguration)
    end

    define_test "Hbase::Hbase constructor should initialize hbase configuration object" do
      assert_kind_of(org.apache.hadoop.conf.Configuration, @hbase.configuration)
    end

    define_test "Hbase::Hbase#admin should create a new admin object when called the first time" do
      assert_kind_of(::Hbase::Admin, @hbase.admin(@formatter))
    end

    define_test "Hbase::Hbase#admin should create a new admin object every call" do
      assert_not_same(@hbase.admin(@formatter), @hbase.admin(@formatter))
    end

    define_test "Hbase::Hbase#table should create a new table object when called the first time" do
      assert_kind_of(::Hbase::Table, @hbase.table('.META.', @formatter))
    end

    define_test "Hbase::Hbase#table should create a new table object every call" do
      assert_not_same(@hbase.table('.META.', @formatter), @hbase.table('.META.', @formatter))
    end
  end
end
