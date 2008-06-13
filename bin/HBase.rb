# HBase ruby classes
module HBase
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

    def describe(tableName)
      now = Time.now 
      @formatter.header()
      found = false
      for t in @admin.listTables()
        if t.getNameAsString() == tableName
          @formatter.row([t.to_s])
          found = true
        end
      end
      if not found
        raise new ArgumentError.new("Failed to find table named " + tableName)
      end
      @formatter.footer(now)
    end

    def exists(tableName)
      now = Time.now 
      @formatter.header()
      @formatter.row([@admin.tableExists(tableName).to_s])
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
        htd.addFamily(hcd(arg))
      end
      @admin.createTable(htd)
      @formatter.header()
      @formatter.footer(now)
    end

    def alter(tableName, args)
      now = Time.now 
      raise TypeError.new("Table name must be of type String") \
        unless tableName.instance_of? String
      descriptor = hcd(args)
      @admin.modifyColumn(tableName, descriptor.getNameAsString(), descriptor);
      @formatter.header()
      @formatter.footer(now)
    end

    def hcd(arg)
      # Return a new HColumnDescriptor made of passed args
      # TODO: This is brittle code.
      # Here is current HCD constructor:
      # public HColumnDescriptor(final byte [] columnName, final int maxVersions,
      # final CompressionType compression, final boolean inMemory,
      # final boolean blockCacheEnabled,
      # final int maxValueLength, final int timeToLive,
      # BloomFilterDescriptor bloomFilter)
      name = arg[NAME]
      raise ArgumentError.new("Column family " + arg + " must have a name") \
        unless name
      # Check the family name for colon.  Add it if missing.
      index = name.index(':')
      if not index
        # Add a colon.  If already a colon, its in the right place,
        # or an exception will come up out of the addFamily
        name << ':'
      end
      # TODO: What encoding are Strings in jruby?
      return HColumnDescriptor.new(name.to_java_bytes,
        # JRuby uses longs for ints. Need to convert.  Also constants are String 
        arg[MAX_VERSIONS]? arg[MAX_VERSIONS]: HColumnDescriptor::DEFAULT_MAX_VERSIONS,
        arg[COMPRESSION]? HColumnDescriptor::CompressionType::valueOf(arg[COMPRESSION]):
          HColumnDescriptor::DEFAULT_COMPRESSION,
        arg[IN_MEMORY]? arg[IN_MEMORY]: HColumnDescriptor::DEFAULT_IN_MEMORY,
        arg[BLOCKCACHE]? arg[BLOCKCACHE]: HColumnDescriptor::DEFAULT_BLOCKCACHE,
        arg[MAX_LENGTH]? arg[MAX_LENGTH]: HColumnDescriptor::DEFAULT_MAX_LENGTH,
        arg[TTL]? arg[TTL]: HColumnDescriptor::DEFAULT_TTL,
        arg[BLOOMFILTER]? arg[BLOOMFILTER]: HColumnDescriptor::DEFAULT_BLOOMFILTER)
    end
  end

  class Table
  end
end
