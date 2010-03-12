module Shell
  module Commands
    class Enable < Command
      def help
        return <<-EOF
          Enable the named table: e.g. "hbase> enable 't1'"
        EOF
      end

      def command(table)
        format_simple_command do
          admin.enable(table)
        end
      end
    end
  end
end
