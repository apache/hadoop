module Shell
  module Commands
    class Incr < Command
      def help
        return <<-EOF
          Increments a cell 'value' at specified table/row/column coordinates.
          To increment a cell value in table 't1' at row 'r1' under column
          'c1' by 1 (can be omitted) or 10 do:

            hbase> incr 't1', 'r1', 'c1'
            hbase> incr 't1', 'r1', 'c1', 1
            hbase> incr 't1', 'r1', 'c1', 10
        EOF
      end

      def command(table, row, column, value = nil)
        cnt = table(table).incr(row, column, value)
        puts "COUNTER VALUE = #{cnt}"
      end
    end
  end
end
