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
    class Grant < Command
      def help
        return <<-EOF
Grant users specific rights to tables.
Syntax : grant <user> <permissions> <table> <column family> <column qualifier>

permissions is either zero or more letters from the set "RWXCA".
READ('R'), WRITE('W'), EXEC('X'), CREATE('C'), ADMIN('A')

For example:

    hbase> grant 'bobsmith', 'RW', 't1', 'f1', 'col1'
EOF
      end

      def command(user, rights, table_name, family=nil, qualifier=nil)
        format_simple_command do
          security_admin.grant(user, rights, table_name, family, qualifier)
        end
      end
    end
  end
end
