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
    class Split < Command
      def help
        return <<-EOF
Split entire table or pass a region to split individual region.  With the 
second parameter, you can specify an explicit split key for the region.  
Examples:
    split 'tableName'
    split 'regionName' # format: 'tableName,startKey,id'
    split 'tableName', 'splitKey'
    split 'regionName', 'splitKey'
EOF
      end

      def command(table_or_region_name, split_point = nil)
        format_simple_command do
          admin.split(table_or_region_name, split_point)
        end
      end
    end
  end
end
