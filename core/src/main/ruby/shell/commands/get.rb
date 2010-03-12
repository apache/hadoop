module Shell
  module Commands
    class Get < Command
      def help
        return <<-EOF
          Get row or cell contents; pass table name, row, and optionally
          a dictionary of column(s), timestamp and versions. Examples:

            hbase> get 't1', 'r1'
            hbase> get 't1', 'r1', {COLUMN => 'c1'}
            hbase> get 't1', 'r1', {COLUMN => ['c1', 'c2', 'c3']}
            hbase> get 't1', 'r1', {COLUMN => 'c1', TIMESTAMP => ts1}
            hbase> get 't1', 'r1', {COLUMN => 'c1', TIMESTAMP => ts1, VERSIONS => 4}
            hbase> get 't1', 'r1', 'c1'
            hbase> get 't1', 'r1', 'c1', 'c2'
            hbase> get 't1', 'r1', ['c1', 'c2']
        EOF
      end

      def command(table, row, *args)
        now = Time.now
        formatter.header(["COLUMN", "CELL"])

        table(table).get(row, *args) do |column, value|
          formatter.row([ column, value ])
        end

        formatter.footer(now)
      end
    end
  end
end
