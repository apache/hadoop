module Shell
  module Commands
    class Put < Command
      def help
        return <<-EOF
          Put a cell 'value' at specified table/row/column and optionally
          timestamp coordinates.  To put a cell value into table 't1' at
          row 'r1' under column 'c1' marked with the time 'ts1', do:

            hbase> put 't1', 'r1', 'c1', 'value', ts1
        EOF
      end

      def command(table, row, column, value, timestamp = nil)
        format_simple_command do
          table(table).put(row, column, value, timestamp)
        end
      end
    end
  end
end
