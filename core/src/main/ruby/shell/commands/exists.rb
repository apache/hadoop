module Shell
  module Commands
    class Exists < Command
      def help
        return <<-EOF
          Does the named table exist? e.g. "hbase> exists 't1'"
        EOF
      end

      def command(table)
        format_simple_command do
          formatter.row([
            "Table #{table} " + (admin.exists?(table.to_s) ? "does exist" : "does not exist")
          ])
        end
      end
    end
  end
end
