module Shell
  module Commands
    class GetCounter < Command
      def help
        return <<-EOF
          Return a counter cell value at specified table/row/column coordinates.
          A cell cell should be managed with atomic increment function oh HBase
          and the data should be binary encoded. Example:

            hbase> get_counter 't1', 'r1', 'c1'
        EOF
      end

      def command(table, row, column, value = nil)
        if cnt = table(table).get_counter(row, column)
          puts "COUNTER VALUE = #{cnt}"
        else
          puts "No counter found at specified coordinates"
        end
      end
    end
  end
end
