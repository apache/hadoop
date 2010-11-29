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
    class Move < Command
      def help
        return <<-EOF
Move a region.  Optionally specify target regionserver else we choose one
at random.  NOTE: You pass the encoded region name, not the region name so
this command is a little different to the others.  The encoded region name
is the hash suffix on region names: e.g. if the region name were
TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396. then
the encoded region name portion is 527db22f95c8a9e0116f0cc13c680396
A server name is its host, port plus startcode. For example:
host187.example.com,60020,1289493121758
Examples:

  hbase> move 'ENCODED_REGIONNAME'
  hbase> move 'ENCODED_REGIONNAME', 'SERVER_NAME'
EOF
      end

      def command(encoded_region_name, server_name = nil)
        format_simple_command do
          admin.move(encoded_region_name, server_name)
        end
      end
    end
  end
end
