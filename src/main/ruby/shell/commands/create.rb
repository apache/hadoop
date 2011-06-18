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
    class Create < Command
      def help
        return <<-EOF
Create table; pass table name, a dictionary of specifications per
column family, and optionally a dictionary of table configuration.
Dictionaries are described below in the GENERAL NOTES section.
Examples:

  hbase> create 't1', {NAME => 'f1', VERSIONS => 5}
  hbase> create 't1', {NAME => 'f1'}, {NAME => 'f2'}, {NAME => 'f3'}
  hbase> # The above in shorthand would be the following:
  hbase> create 't1', 'f1', 'f2', 'f3'
  hbase> create 't1', {NAME => 'f1', VERSIONS => 1, TTL => 2592000, BLOCKCACHE => true}
  hbase> create 't1', 'f1', {SPLITS => ['10', '20', '30', '40']}
  hbase> create 't1', 'f1', {SPLITS_FILE => 'splits.txt'}
EOF
      end

      def command(table, *args)
        format_simple_command do
          admin.create(table, *args)
        end
      end
    end
  end
end
