module Shell
  module Commands
    class Scan < Command
      def help
        return <<-EOF
          Scan a table; pass table name and optionally a dictionary of scanner
          specifications.  Scanner specifications may include one or more of
          the following: LIMIT, STARTROW, STOPROW, TIMESTAMP, or COLUMNS.  If
          no columns are specified, all columns will be scanned.  To scan all
          members of a column family, leave the qualifier empty as in
          'col_family:'.  Examples:

            hbase> scan '.META.'
            hbase> scan '.META.', {COLUMNS => 'info:regioninfo'}
            hbase> scan 't1', {COLUMNS => ['c1', 'c2'], LIMIT => 10, STARTROW => 'xyz'}

          For experts, there is an additional option -- CACHE_BLOCKS -- which
          switches block caching for the scanner on (true) or off (false).  By
          default it is enabled.  Examples:

            hbase> scan 't1', {COLUMNS => ['c1', 'c2'], CACHE_BLOCKS => false}
        EOF
      end

      def command(table, args = {})
        now = Time.now
        formatter.header(["ROW", "COLUMN+CELL"])

        count = table(table).scan(args) do |row, cells|
          formatter.row([ row, cells ])
        end

        formatter.footer(now, count)
      end
    end
  end
end
