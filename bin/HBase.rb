# HBase ruby classes

module HBase
  # Constants needed as keys creating tables, etc.
  NAME = "NAME"
  MAX_VERSIONS = "MAX_VERSIONS"
  MAX_LENGTH = "MAX_LENGTH"
  TTL = "TTL"
  BLOOMFILTER = "BLOOMFILTER"
  COMPRESSION_TYPE = "COMPRESSION_TYPE"
  # TODO: Add table options here.

  class Admin
    def initialize(configuration, formatter)
      @admin = HBaseAdmin.new(configuration)
      @formatter = formatter
    end
   
    def list
      now = Time.now 
      @formatter.header()
      for t in @admin.listTables()
        @formatter.row([t.getNameAsString()])
      end
      @formatter.footer(now)
    end

    def exists(tableName)
      now = Time.now 
      @formatter.header()
      @formatter.row([@admin.tableExists(tableName)])
      @formatter.footer(now)
    end

    def enable(tableName)
      # TODO: Need an isEnabled method
      now = Time.now 
      @admin.enableTable(tableName)
      @formatter.header()
      @formatter.footer(now)
    end

    def disable(tableName)
      # TODO: Need an isDisabled method
      now = Time.now 
      @admin.disableTable(tableName)
      @formatter.header()
      @formatter.footer(now)
    end

    def drop(tableName)
      now = Time.now 
      @admin.deleteTable(tableName)
      @formatter.header()
      @formatter.row(["Deleted %s" % tableName])
      @formatter.footer(now)
    end

    def create(tableName, args)
      now = Time.now 
      # Pass table name and an array of Hashes.  Later, test the last
      # array to see if its table options rather than column family spec.
      raise TypeError.new("Table name must be of type String") \
        unless tableName.instance_of? String
      # For now presume all the rest of the args are column family
      # hash specifications. TODO: Add table options handling.
      htd = HTableDescriptor.new(tableName)
      for arg in args
        raise TypeError.new(arg.class.to_s + " of " + arg.to_s + " is not of Hash type") \
          unless arg.instance_of? Hash
        name = arg[NAME]
        raise ArgumentError.new("Column family " + arg + " must have a name at least") \
          unless name
        # TODO: Process all other parameters for column family
        # Check the family name for colon.  Add it if missing.
        index = name.index(':')
        if not index
          # Add a colon.  If already a colon, its in the right place,
          # or an exception will come up out of the addFamily
          name << ':'
        end
        htd.addFamily(HColumnDescriptor.new(name))
      end
      @admin.createTable(htd)
      @formatter.header()
      @formatter.row(["Created %s" % tableName])
      @formatter.footer(now)
    end
  end

  class Table
  end
end
