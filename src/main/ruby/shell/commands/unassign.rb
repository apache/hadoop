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
    class Unassign < Command
      def help
        return <<-EOF
Unassign a region. Unassign will close region in current location and then
reopen it again.  Pass 'true' to force the unassignment ('force' will clear
all in-memory state in master before the reassign. If results in
double assignment use hbck -fix to resolve. To be used by experts).
Use with caution.  For expert use only.  Examples:

  hbase> unassign 'REGIONNAME'
  hbase> unassign 'REGIONNAME', true
EOF
      end

      def command(region_name, force = 'false')
        format_simple_command do
          admin.unassign(region_name, force)
        end
      end
    end
  end
end
