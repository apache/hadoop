module Shell
  module Commands
    class Version < Command
      def help
        return <<-EOF
          Output this HBase version
        EOF
      end

      def command
        # Output version.
        puts "Version: #{org.apache.hadoop.hbase.util.VersionInfo.getVersion()}, " +
             "r#{org.apache.hadoop.hbase.util.VersionInfo.getRevision()}, " +
             "#{org.apache.hadoop.hbase.util.VersionInfo.getDate()}"
      end
    end
  end
end
