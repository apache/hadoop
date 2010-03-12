module Shell
  module Commands
    class DisableRegion < Command
      def help
        return <<-EOF
          Disable a single region. For example:

            hbase> disable_region 'REGIONNAME'
        EOF
      end

      def command(region_name)
        format_simple_command do
          admin.disable_region(region_name)
        end
      end
    end
  end
end
