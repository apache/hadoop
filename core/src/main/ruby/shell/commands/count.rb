module Shell
  module Commands
    class Count < Command
      def help
        return <<-EOF
          Count the number of rows in a table. This operation may take a LONG
          time (Run '$HADOOP_HOME/bin/hadoop jar hbase.jar rowcount' to run a
          counting mapreduce job). Current count is shown every 1000 rows by
          default. Count interval may be optionally specified. Examples:

          hbase> count 't1'
          hbase> count 't1', 100000
        EOF
      end

      def command(table, interval = 1000)
        now = Time.now
        formatter.header
        count = table(table).count(interval) do |cnt, row|
          formatter.row([ "Current count: #{cnt}, row: #{row}" ])
        end
        formatter.footer(now, count)
      end
    end
  end
end
