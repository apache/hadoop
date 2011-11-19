#
# Copyright 2009 The Apache Software Foundation
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
    class Command
      attr_accessor :shell

      def initialize(shell)
        self.shell = shell
      end

      def command_safe(debug, *args)
        translate_hbase_exceptions(*args) { command(*args) }
      rescue => e
        puts
        puts "ERROR: #{e}"
        puts "Backtrace: #{e.backtrace.join("\n           ")}" if debug
        puts
        puts "Here is some help for this command:"
        puts help
        puts
      ensure
        return nil
      end

      def admin
        shell.hbase_admin
      end

      def table(name)
        shell.hbase_table(name)
      end

      def replication_admin
        shell.hbase_replication_admin
      end

      def security_admin
        shell.hbase_security_admin
      end

      #----------------------------------------------------------------------

      def formatter
        shell.formatter
      end

      def format_simple_command
        now = Time.now
        yield
        formatter.header
        formatter.footer(now)
      end

      def translate_hbase_exceptions(*args)
        yield
      rescue org.apache.hadoop.hbase.TableNotFoundException
        raise "Unknown table #{args.first}!"
      rescue org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException
        valid_cols = table(args.first).get_all_columns.map { |c| c + '*' }
        raise "Unknown column family! Valid column names: #{valid_cols.join(", ")}"
      rescue org.apache.hadoop.hbase.TableExistsException
        raise "Table already exists: #{args.first}!"
      end
    end
  end
end
