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
