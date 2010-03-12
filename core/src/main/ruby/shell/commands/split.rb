module Shell
  module Commands
    class Split < Command
      def help
        return <<-EOF
          Split table or pass a region row to split individual region
        EOF
      end

      def command(table_or_region_name)
        format_simple_command do
          admin.split(table_or_region_name)
        end
      end
    end
  end
end
