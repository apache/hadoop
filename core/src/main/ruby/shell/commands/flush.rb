module Shell
  module Commands
    class Flush < Command
      def help
        return <<-EOF
          Flush all regions in passed table or pass a region row to
          flush an individual region.  For example:

            hbase> flush 'TABLENAME'
            hbase> flush 'REGIONNAME'
        EOF
      end

      def command(table_or_region_name)
        format_simple_command do
          admin.flush(table_or_region_name)
        end
      end
    end
  end
end
