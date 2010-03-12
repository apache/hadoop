module Shell
  module Commands
    class EnableRegion < Command
      def help
        return <<-EOF
          Enable a single region. For example:

            hbase> enable_region 'REGIONNAME'
        EOF
      end

      def command(region_name)
        format_simple_command do
          admin.enable_region(region_name)
        end
      end
    end
  end
end
