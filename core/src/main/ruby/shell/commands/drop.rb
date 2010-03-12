module Shell
  module Commands
    class Drop < Command
      def help
        return <<-EOF
          Drop the named table. Table must first be disabled. If table has
          more than one region, run a major compaction on .META.:

          hbase> major_compact ".META."
        EOF
      end

      def command(table)
        format_simple_command do
          admin.drop(table)
        end
      end
    end
  end
end
