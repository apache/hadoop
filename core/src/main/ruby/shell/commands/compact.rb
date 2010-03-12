module Shell
  module Commands
    class Compact < Command
      def help
        return <<-EOF
          Compact all regions in passed table or pass a region row
          to compact an individual region
        EOF
      end

      def command(table_or_region_name)
        format_simple_command do
          admin.compact(table_or_region_name)
        end
      end
    end
  end
end
