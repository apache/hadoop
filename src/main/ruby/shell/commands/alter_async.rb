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
    class AlterAsync < Command
      def help
        return <<-EOF
Alter column family schema, does not wait for all regions to receive the
schema changes. Pass table name and a dictionary specifying new column
family schema. Dictionaries are described on the main help command output.
Dictionary must include name of column family to alter. For example,

To change or add the 'f1' column family in table 't1' from defaults
to instead keep a maximum of 5 cell VERSIONS, do:

  hbase> alter_async 't1', NAME => 'f1', VERSIONS => 5

To delete the 'f1' column family in table 't1', do:

  hbase> alter_async 't1', NAME => 'f1', METHOD => 'delete'

or a shorter version:

  hbase> alter_async 't1', 'delete' => 'f1'

You can also change table-scope attributes like MAX_FILESIZE
MEMSTORE_FLUSHSIZE, READONLY, and DEFERRED_LOG_FLUSH.

For example, to change the max size of a family to 128MB, do:

  hbase> alter 't1', METHOD => 'table_att', MAX_FILESIZE => '134217728'

There could be more than one alteration in one command:

  hbase> alter 't1', {NAME => 'f1'}, {NAME => 'f2', METHOD => 'delete'}

To check if all the regions have been updated, use alter_status <table_name>
EOF
      end

      def command(table, *args)
        format_simple_command do
          admin.alter(table, false, *args)
        end
      end
    end
  end
end
