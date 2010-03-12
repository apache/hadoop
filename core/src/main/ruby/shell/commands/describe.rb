module Shell
  module Commands
    class Describe < Command
      def help
        return <<-EOF
          Describe the named table. For example:
            hbase> describe 't1'
        EOF
      end

      def command(table)
        now = Time.now

        desc = admin.describe(table)

        formatter.header([ "DESCRIPTION", "ENABLED" ], [ 64 ])
        formatter.row([ desc, admin.enabled?(table).to_s ], true, [ 64 ])
        formatter.footer(now)
      end
    end
  end
end
