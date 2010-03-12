module Shell
  module Commands
    class Alter < Command
      def help
        return <<-EOF
          Alter column family schema;  pass table name and a dictionary
          specifying new column family schema. Dictionaries are described
          on the main help command output. Dictionary must include name
          of column family to alter. For example,

          To change or add the 'f1' column family in table 't1' from defaults
          to instead keep a maximum of 5 cell VERSIONS, do:
          hbase> alter 't1', NAME => 'f1', VERSIONS => 5

          To delete the 'f1' column family in table 't1', do:
          hbase> alter 't1', NAME => 'f1', METHOD => 'delete'
          or a shorter version:
          hbase> alter 't1', 'delete' => 'f1'

          You can also change table-scope attributes like MAX_FILESIZE
          MEMSTORE_FLUSHSIZE, READONLY, and DEFERRED_LOG_FLUSH.

          For example, to change the max size of a family to 128MB, do:
          hbase> alter 't1', METHOD => 'table_att', MAX_FILESIZE => '134217728'

          There could be more than one alteration in one command:
          hbase> alter 't1', {NAME => 'f1'}, {NAME => 'f2', METHOD => 'delete'}
        EOF
      end

      def command(table, *args)
        format_simple_command do
          admin.alter(table, *args)
        end
      end
    end
  end
end
