module Shell
  module Commands
    class Deleteall < Command
      def help
        return <<-EOF
          Delete all cells in a given row; pass a table name, row, and optionally
          a column and timestamp. Examples:

            hbase> deleteall 't1', 'r1'
            hbase> deleteall 't1', 'r1', 'c1'
            hbase> deleteall 't1', 'r1', 'c1', ts1
        EOF
      end

      def command(table, row, column = nil, timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP)
        format_simple_command do
          table(table).deleteall(row, column, timestamp)
        end
      end
    end
  end
end
