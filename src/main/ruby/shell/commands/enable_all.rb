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
    class EnableAll < Command
      def help
        return <<-EOF
Enable all of the tables matching the given regex:

hbase> enable_all 't.*'
EOF
      end

      def command(regex)
        regex = /#{regex}/ unless regex.is_a?(Regexp)
        list = admin.list.grep(regex)
        count = list.size
        list.each do |table|
          formatter.row([ table ])
        end
        puts "\nEnable the above #{count} tables (y/n)?" unless count == 0
        answer = 'n'
        answer = gets.chomp unless count == 0
        puts "No tables matched the regex #{regex.to_s}" if count == 0
        return unless answer =~ /y.*/i
        failed = admin.enable_all(regex)
        puts "#{count - failed.size} tables successfully enabled"
        puts "#{failed.size} tables not enabled due to an exception: #{failed.join ','}" unless failed.size == 0
      end
    end
  end
end
