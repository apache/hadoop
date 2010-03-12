module Shell
  module Commands
    class Delete < Command
      def help
        return <<-EOF
          Put a delete cell value at specified table/row/column and optionally
          timestamp coordinates.  Deletes must match the deleted cell's
          coordinates exactly.  When scanning, a delete cell suppresses older
          versions. To delete a cell from  't1' at row 'r1' under column 'c1'
          marked with the time 'ts1', do:

            hbase> delete 't1', 'r1', 'c1', ts1
        EOF
      end

      def command(table, row, column, timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP)
        format_simple_command do
          table(table).delete(row, column, timestamp)
        end
      end
    end
  end
end
