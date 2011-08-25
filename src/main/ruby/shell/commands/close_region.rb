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
Close a single region.  Ask the master to close a region out on the cluster
or if 'SERVER_NAME' is supplied, ask the designated hosting regionserver to
close the region directly.  Closing a region, the master expects 'REGIONNAME'
to be a fully qualified region name.  When asking the hosting regionserver to
directly close a region, you pass the regions' encoded name only. A region
name looks like this:
 
 TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.

The trailing period is part of the regionserver name. A region's encoded name
is the hash at the end of a region name; e.g. 527db22f95c8a9e0116f0cc13c680396 
(without the period).  A 'SERVER_NAME' is its host, port plus startcode. For
example: host187.example.com,60020,1289493121758 (find servername in master ui
or when you do detailed status in shell).  This command will end up running
close on the region hosting regionserver.  The close is done without the
master's involvement (It will not know of the close).  Once closed, region will
stay closed.  Use assign to reopen/reassign.  Use unassign or move to assign
the region elsewhere on cluster. Use with caution.  For experts only.
Examples:

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
