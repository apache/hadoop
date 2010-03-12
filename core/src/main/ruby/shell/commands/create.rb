module Shell
  module Commands
    class Create < Command
      def help
        return <<-EOF
          Create table; pass table name, a dictionary of specifications per
          column family, and optionally a dictionary of table configuration.
          Dictionaries are described below in the GENERAL NOTES section.
          Examples:

          hbase> create 't1', {NAME => 'f1', VERSIONS => 5}
          hbase> create 't1', {NAME => 'f1'}, {NAME => 'f2'}, {NAME => 'f3'}
          hbase> # The above in shorthand would be the following:
          hbase> create 't1', 'f1', 'f2', 'f3'
          hbase> create 't1', {NAME => 'f1', VERSIONS => 1, TTL => 2592000,
                 BLOCKCACHE => true}
        EOF
      end

      def command(table, *args)
        format_simple_command do
          admin.create(table, *args)
        end
      end
    end
  end
end
