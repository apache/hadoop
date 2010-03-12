module Shell
  module Commands
    class Shutdown < Command
      def help
        return <<-EOF
          Shut down the cluster.
        EOF
      end

      def command
        admin.shutdown
      end
    end
  end
end
