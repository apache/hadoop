module Shell
  module Commands
    class Zk < Command
      def help
        return <<-EOF
          Low level ZooKeeper surgery tools. Type "zk 'help'" for more
          information (Yes, you must quote 'help').
        EOF
      end

      def command(*args)
        admin.zk(args)
      end
    end
  end
end
