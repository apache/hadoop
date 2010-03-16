module Shell
  module Commands
    class Count < Command
      def help
        return <<-EOF
          Count the number of rows in a table. This operation may take a LONG
          time (Run '$HADOOP_HOME/bin/hadoop jar hbase.jar rowcount' to run a
          counting mapreduce job). Current count is shown every 1000 rows by
          default. Count interval may be optionally specified. Scan caching
          is enabled on count scans by default. Default cache size is 10 rows.
          If your rows are small in size, you may want to increase this
          parameter. Examples:

          hbase> count 't1'
          hbase> count 't1', INTERVAL => 100000
          hbase> count 't1', CACHE => 1000
          hbase> count 't1', INTERVAL => 10, CACHE => 1000
        EOF
      end

      def command(table, params = {})
        # If the second parameter is an integer, then it is the old command syntax
        params = { 'INTERVAL' => params } if params.kind_of?(Fixnum)

        # Merge params with defaults
        params = {
          'INTERVAL' => 1000,
          'CACHE' => 10
        }.merge(params)

        # Call the counter method
        now = Time.now
        formatter.header
        count = table(table).count(params['INTERVAL'].to_i, params['CACHE'].to_i) do |cnt, row|
          formatter.row([ "Current count: #{cnt}, row: #{row}" ])
        end
        formatter.footer(now, count)
      end
    end
  end
end
