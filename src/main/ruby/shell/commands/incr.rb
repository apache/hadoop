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

module Shell
  module Commands
    class Incr < Command
      def help
        return <<-EOF
Increments a cell 'value' at specified table/row/column coordinates.
To increment a cell value in table 't1' at row 'r1' under column
'c1' by 1 (can be omitted) or 10 do:

  hbase> incr 't1', 'r1', 'c1'
  hbase> incr 't1', 'r1', 'c1', 1
  hbase> incr 't1', 'r1', 'c1', 10
EOF
      end

      def command(table, row, column, value = nil)
        cnt = table(table).incr(row, column, value)
        puts "COUNTER VALUE = #{cnt}"
      end
    end
  end
end
