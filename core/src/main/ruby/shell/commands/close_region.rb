module Shell
  module Commands
    class CloseRegion < Command
      def help
        return <<-EOF
          Close a single region. Optionally specify regionserver.
          Examples:
            hbase> close_region 'REGIONNAME'
            hbase> close_region 'REGIONNAME', 'REGIONSERVER_IP:PORT'
        EOF
      end

      def command(region_name, server = nil)
        format_simple_command do
          admin.close_region(region_name, server)
        end
      end
    end
  end
end
