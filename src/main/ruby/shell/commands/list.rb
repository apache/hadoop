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
    class List < Command
      def help
        return <<-EOF
          List all tables in hbase. Optional regular expression parameter could
          be used to filter the output. Examples:

            hbase> list
            hbase> list 'abc.*'
        EOF
      end

      def command(regex = ".*")
        now = Time.now
        formatter.header([ "TABLE" ])

        regex = /#{regex}/ unless regex.is_a?(Regexp)
        list = admin.list.grep(regex)
        list.each do |table|
          formatter.row([ table ])
        end

        formatter.footer(now, list.count)
      end
    end
  end
end
