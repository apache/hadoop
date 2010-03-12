module Shell
  module Commands
    class Status < Command
      def help
        return <<-EOF
          Show cluster status. Can be 'summary', 'simple', or 'detailed'. The
          default is 'summary'. Examples:

            hbase> status
            hbase> status 'simple'
            hbase> status 'summary'
            hbase> status 'detailed'
        EOF
      end

      def command(format = 'summary')
        admin.status(format)
      end
    end
  end
end
