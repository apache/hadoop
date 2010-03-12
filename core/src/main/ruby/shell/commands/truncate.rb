module Shell
  module Commands
    class Truncate < Command
      def help
        return <<-EOF
          Disables, drops and recreates the specified table.
        EOF
      end

      def command(table)
        format_simple_command do
          puts "Truncating '#{table}' table (it may take a while):"
          admin.truncate(table) { |log| puts " - #{log}" }
        end
      end

    end
  end
end
