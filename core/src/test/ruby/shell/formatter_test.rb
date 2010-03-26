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

require 'shell/formatter'

class ShellFormatterTest < Test::Unit::TestCase
  # Helper method to construct a null formatter
  def formatter
    Shell::Formatter::Base.new(:output_stream => STDOUT)
  end

  #
  # Constructor tests
  #
  define_test "Formatter constructor should not raise error valid IO streams" do
    assert_nothing_raised do
      Shell::Formatter::Base.new(:output_stream => STDOUT)
    end
  end

  define_test "Formatter constructor should not raise error when no IO stream passed" do
    assert_nothing_raised do
      Shell::Formatter::Base.new()
    end
  end

  define_test "Formatter constructor should raise error on non-IO streams" do
    assert_raise TypeError do
      Shell::Formatter::Base.new(:output_stream => 'foostring')
    end
  end

  #-------------------------------------------------------------------------------------------------------
  # Printing methods tests
  # FIXME: The tests are just checking that the code has no typos, try to figure out a better way to test
  #
  define_test "Formatter#header should work" do
    formatter.header(['a', 'b'])
    formatter.header(['a', 'b'], [10, 20])
  end

  define_test "Formatter#row should work" do
    formatter.row(['a', 'b'])
    formatter.row(['xxxxxxxxx xxxxxxxxxxx xxxxxxxxxxx xxxxxxxxxxxx xxxxxxxxx xxxxxxxxxxxx xxxxxxxxxxxxxxx xxxxxxxxx xxxxxxxxxxxxxx'])
    formatter.row(['yyyyyy yyyyyy yyyyy yyy', 'xxxxxxxxx xxxxxxxxxxx xxxxxxxxxxx xxxxxxxxxxxx xxxxxxxxx xxxxxxxxxxxx xxxxxxxxxxxxxxx xxxxxxxxx xxxxxxxxxxxxxx  xxx xx x xx xxx xx xx xx x xx x x xxx x x xxx x x xx x x x x x x xx '])
    formatter.row(["NAME => 'table1', FAMILIES => [{NAME => 'fam2', VERSIONS => 3, COMPRESSION => 'NONE', IN_MEMORY => false, BLOCKCACHE => false, LENGTH => 2147483647, TTL => FOREVER, BLOOMFILTER => NONE}, {NAME => 'fam1', VERSIONS => 3, COMPRESSION => 'NONE', IN_MEMORY => false, BLOCKCACHE => false, LENGTH => 2147483647, TTL => FOREVER, BLOOMFILTER => NONE}]"])
  end

  define_test "Froematter#footer should work" do
    formatter.footer(Time.now - 5)
  end
end
