module Shell
  module Commands
    class ZkDump < Command
      def help
        return <<-EOF
          Dump status of HBase cluster as seen by ZooKeeper.
        EOF
      end

      def command
        puts admin.zk_dump
      end
    end
  end
end
