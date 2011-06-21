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
    class CloseRegion < Command
      def help
        return <<-EOF
Close a single region. Optionally specify regionserver 'servername' where
A server name is its host, port plus startcode. For example:
host187.example.com,60020,1289493121758 (find servername in master ui or
when you do detailed status in shell).  Connects to the regionserver and
runs close on hosting regionserver.  The close is done without the master's
involvement (It will not know of the close).  Once closed, region will stay
closed.  Use assign to reopen/reassign.  Use unassign or move to assign the
region elsewhere on cluster. Use with caution.  For experts only.  Examples:

  hbase> close_region 'REGIONNAME'
  hbase> close_region 'REGIONNAME', 'SERVER_NAME'
EOF
      end

      def command(region_name, server = nil)
        format_simple_command do
          admin.close_region(region_name, server)
        end
      end
    end
  end
end
