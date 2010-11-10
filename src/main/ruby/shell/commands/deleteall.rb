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
    class Deleteall < Command
      def help
        return <<-EOF
Delete all cells in a given row; pass a table name, row, and optionally
a column and timestamp. Examples:

  hbase> deleteall 't1', 'r1'
  hbase> deleteall 't1', 'r1', 'c1'
  hbase> deleteall 't1', 'r1', 'c1', ts1
EOF
      end

      def command(table, row, column = nil, timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP)
        format_simple_command do
          table(table).deleteall(row, column, timestamp)
        end
      end
    end
  end
end
