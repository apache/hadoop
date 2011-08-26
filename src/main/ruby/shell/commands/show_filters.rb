#
# Copyright 2011 The Apache Software Foundation
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

java_import org.apache.hadoop.hbase.filter.ParseFilter

module Shell
  module Commands
    class ShowFilters < Command
      def help
        return <<-EOF
Show all the filters in hbase. Example:
  hbase> show_filters

  Documentation on filters mentioned below can be found at: https://our.intern.facebook.com/intern/wiki/index.php/HBase/Filter_Language
  ColumnPrefixFilter
  TimestampsFilter
  PageFilter
  .....
  KeyOnlyFilter
EOF
      end

      def command( )
        now = Time.now
        formatter.row(["Documentation on filters mentioned below can " +
                       "be found at: https://our.intern.facebook.com/intern/" +
                       "wiki/index.php/HBase/Filter_Language"])

        parseFilter = ParseFilter.new
        supportedFilters = parseFilter.getSupportedFilters

        supportedFilters.each do |filter|
          formatter.row([filter])
        end
      end
    end
  end
end
