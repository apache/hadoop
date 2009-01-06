# HBase ruby classes.
# Has wrapper classes for org.apache.hadoop.hbase.client.HBaseAdmin
# and for org.apache.hadoop.hbase.client.HTable.  Classes take
# Formatters on construction and outputs any results using
# Formatter methods.  These classes are only really for use by
# the hirb.rb HBase Shell script; they don't make much sense elsewhere.
# For example, the exists method on Admin class prints to the formatter
# whether the table exists and returns nil regardless.
include Java
include_class('java.lang.Integer') {|package,name| "J#{name}" }
include_class('java.lang.Boolean') {|package,name| "J#{name}" }

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.io.BatchUpdate
import org.apache.hadoop.hbase.io.RowResult
import org.apache.hadoop.hbase.io.Cell
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.hbase.HRegionInfo

module HBase
  COLUMN = "COLUMN"
  COLUMNS = "COLUMNS"
  TIMESTAMP = "TIMESTAMP"
  NAME = HConstants::NAME
  VERSIONS = HConstants::VERSIONS
  IN_MEMORY = HConstants::IN_MEMORY
  STOPROW = "STOPROW"
  STARTROW = "STARTROW"
  ENDROW = STOPROW
  LIMIT = "LIMIT"
  METHOD = "METHOD"

  # Wrapper for org.apache.hadoop.hbase.client.HBaseAdmin
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
      tables = @admin.listTables().to_a
      tables.push(HTableDescriptor::META_TABLEDESC, HTableDescriptor::ROOT_TABLEDESC)
      for t in tables
        if t.getNameAsString() == tableName
          @formatter.row([t.to_s])
          found = true
        end
      end
      if not found
        raise ArgumentError.new("Failed to find table named " + tableName)
      end
      @formatter.footer(now)
    end

    def exists(tableName)
      now = Time.now 
      @formatter.header()
      e = @admin.tableExists(tableName)
      @formatter.row([e.to_s])
      @formatter.footer(now)
    end

    def flush(tableNameOrRegionName)
      now = Time.now 
      @formatter.header()
      @admin.flush(tableNameOrRegionName)
      @formatter.footer(now)
    end

    def compact(tableNameOrRegionName)
      now = Time.now 
      @formatter.header()
      @admin.compact(tableNameOrRegionName)
      @formatter.footer(now)
    end

    def major_compact(tableNameOrRegionName)
      now = Time.now 
      @formatter.header()
      @admin.majorCompact(tableNameOrRegionName)
      @formatter.footer(now)
    end

    def split(tableNameOrRegionName)
      now = Time.now 
      @formatter.header()
      @admin.split(tableNameOrRegionName)
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

    def enable_region(regionName)
      online(regionName, false)
    end

    def disable_region(regionName)
      online(regionName, true)
    end
   
    def online(regionName, onOrOff)
      now = Time.now 
      meta = HTable.new(HConstants::META_TABLE_NAME)
      bytes = Bytes.toBytes(regionName)
      hriBytes = meta.get(bytes, HConstants::COL_REGIONINFO).getValue()
      hri = Writables.getWritable(hriBytes, HRegionInfo.new());
      hri.setOffline(onOrOff)
      p hri
      bu = BatchUpdate.new(bytes)
      bu.put(HConstants::COL_REGIONINFO, Writables.getBytes(hri))
      meta.commit(bu);
      @formatter.header()
      @formatter.footer(now)
    end

    def drop(tableName)
      now = Time.now 
      @formatter.header()
      if @admin.isTableEnabled(tableName)
        raise IOError.new("Table " + tableName + " is enabled. Disable it first")
      else
        @admin.deleteTable(tableName)
      end
      @formatter.footer(now)
    end

    def truncate(tableName)
      now = Time.now
      @formatter.header()
      hTable = HTable.new(tableName)
      tableDescription = hTable.getTableDescriptor()
      puts 'Truncating ' + tableName + '; it may take a while'
      puts 'Disabling table...'
      disable(tableName)
      puts 'Dropping table...'
      drop(tableName)
      puts 'Creating table...'
      @admin.createTable(tableDescription)
      @formatter.footer(now)
    end

    # Pass tablename and an array of Hashes
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
        if arg.instance_of? String
          htd.addFamily(HColumnDescriptor.new(makeColumnName(arg)))
        else
          raise TypeError.new(arg.class.to_s + " of " + arg.to_s + " is not of Hash type") \
            unless arg.instance_of? Hash
          htd.addFamily(hcd(arg))
        end
      end
      @admin.createTable(htd)
      @formatter.header()
      @formatter.footer(now)
    end

    def alter(tableName, args)
      now = Time.now
      raise TypeError.new("Table name must be of type String") \
        unless tableName.instance_of? String
      htd = @admin.getTableDescriptor(tableName.to_java_bytes)
      method = args.delete(METHOD)
      if method == "delete"
        @admin.deleteColumn(tableName, makeColumnName(args[NAME]))
      else
        descriptor = hcd(args) 
        if (htd.hasFamily(descriptor.getNameAsString().to_java_bytes))
          @admin.modifyColumn(tableName, descriptor.getNameAsString(), 
                              descriptor);
        else
          @admin.addColumn(tableName, descriptor);
        end
      end
      @formatter.header()
      @formatter.footer(now)
    end

    def close_region(regionName, server)
      now = Time.now
      s = nil
      s = [server].to_java if server
      @admin.closeRegion(regionName, s)
      @formatter.header()
      @formatter.footer(now)
    end

    # Make a legal column  name of the passed String
    # Check string ends in colon. If not, add it.
    def makeColumnName(arg)
      index = arg.index(':')
      if not index
        # Add a colon.  If already a colon, its in the right place,
        # or an exception will come up out of the addFamily
        arg << ':'
      end
      arg
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
      name = makeColumnName(name)
      # TODO: What encoding are Strings in jruby?
      return HColumnDescriptor.new(name.to_java_bytes,
        # JRuby uses longs for ints. Need to convert.  Also constants are String 
        arg[VERSIONS]? JInteger.new(arg[VERSIONS]): HColumnDescriptor::DEFAULT_VERSIONS,
        arg[HColumnDescriptor::COMPRESSION]? HColumnDescriptor::CompressionType::valueOf(arg[HColumnDescriptor::COMPRESSION]):
          HColumnDescriptor::DEFAULT_COMPRESSION,
        arg[IN_MEMORY]? JBoolean.valueOf(arg[IN_MEMORY]): HColumnDescriptor::DEFAULT_IN_MEMORY,
        arg[HColumnDescriptor::BLOCKCACHE]? JBoolean.valueOf(arg[HColumnDescriptor::BLOCKCACHE]): HColumnDescriptor::DEFAULT_BLOCKCACHE,
        arg[HColumnDescriptor::LENGTH]? JInteger.new(arg[HColumnDescriptor::LENGTH]): HColumnDescriptor::DEFAULT_LENGTH,
        arg[HColumnDescriptor::TTL]? JInteger.new(arg[HColumnDescriptor::TTL]): HColumnDescriptor::DEFAULT_TTL,
        arg[HColumnDescriptor::BLOOMFILTER]? JBoolean.valueOf(arg[HColumnDescriptor::BLOOMFILTER]): HColumnDescriptor::DEFAULT_BLOOMFILTER)
    end
  end

  # Wrapper for org.apache.hadoop.hbase.client.HTable
  class Table
    def initialize(configuration, tableName, formatter)
      @table = HTable.new(configuration, tableName)
      @formatter = formatter
    end

    # Delete a cell
    def delete(row, column, timestamp = HConstants::LATEST_TIMESTAMP)
      now = Time.now 
      bu = BatchUpdate.new(row, timestamp)
      bu.delete(column)
      @table.commit(bu)
      @formatter.header()
      @formatter.footer(now)
    end

    def deleteall(row, column = nil, timestamp = HConstants::LATEST_TIMESTAMP)
      now = Time.now 
      @table.deleteAll(row, column, timestamp)
      @formatter.header()
      @formatter.footer(now)
    end

    def getAllColumns
       htd = @table.getTableDescriptor()
       result = []
       for f in htd.getFamilies()
         n = f.getNameAsString()
         n << ':'
         result << n
       end
       result
    end

    def scan(args = {})
      now = Time.now 
      limit = -1
      if args != nil and args.length > 0
        limit = args["LIMIT"] || -1 
        filter = args["FILTER"] || nil
        startrow = args["STARTROW"] || ""
        stoprow = args["STOPROW"] || nil
        timestamp = args["TIMESTAMP"] || HConstants::LATEST_TIMESTAMP
        columns = args["COLUMNS"] || getAllColumns()
        
        if columns.class == String
          columns = [columns]
        elsif columns.class != Array
          raise ArgumentError.new("COLUMNS must be specified as a String or an Array")
        end
        cs = columns.to_java(java.lang.String)
        
        if stoprow
          s = @table.getScanner(cs, startrow, stoprow, timestamp)
        else
          s = @table.getScanner(cs, startrow, timestamp, filter) 
        end
      else
        columns = getAllColumns()
        s = @table.getScanner(columns.to_java(java.lang.String))
      end
      count = 0
      @formatter.header(["ROW", "COLUMN+CELL"])
      i = s.iterator()
      while i.hasNext()
        r = i.next()
        row = String.from_java_bytes r.getRow()
        for k, v in r
          column = String.from_java_bytes k
          cell = toString(column, v)
          @formatter.row([row, "column=%s, %s" % [column, cell]])
        end
        count += 1
        if limit != -1 and count >= limit
          break
        end
      end
      @formatter.footer(now)
    end

    def put(row, column, value, timestamp = nil)
      now = Time.now 
      bu = nil
      if timestamp
        bu = BatchUpdate.new(row, timestamp)
      else
        bu = BatchUpdate.new(row)
      end
      bu.put(column, value.to_java_bytes)
      @table.commit(bu)
      @formatter.header()
      @formatter.footer(now)
    end

    def isMetaTable()
      tn = @table.getTableName()
      return Bytes.equals(tn, HConstants::META_TABLE_NAME) ||
        Bytes.equals(tn, HConstants::ROOT_TABLE_NAME)
    end

    # Make a String of the passed cell.
    # Intercept cells whose format we know such as the info:regioninfo in .META.
    def toString(column, cell)
      if isMetaTable()
        if column == 'info:regioninfo'
          hri = Writables.getHRegionInfoOrNull(cell.getValue())
          return "timestamp=%d, value=%s" % [cell.getTimestamp(), hri.toString()]
        elsif column == 'info:serverstartcode'
          return "timestamp=%d, value=%s" % [cell.getTimestamp(), \
            Bytes.toLong(cell.getValue())]
        end
      end
      cell.toString()
    end
  
    # Get from table
    def get(row, args = {})
      now = Time.now 
      result = nil
      if args == nil or args.length == 0
        result = @table.getRow(row.to_java_bytes)
      else
        # Its a hash.
        columns = args[COLUMN] 
        if columns == nil
          # Maybe they used the COLUMNS key
          columns = args[COLUMNS]
        end
        if columns == nil
          # May have passed TIMESTAMP and row only; wants all columns from ts.
          ts = args[TIMESTAMP] 
          if not ts
            raise ArgumentError.new("Failed parse of " + args + ", " + args.class)
          end
          result = @table.getRow(row.to_java_bytes, ts)
        else
          # Columns are non-nil
          if columns.class == String
            # Single column
            result = @table.get(row, columns,
              args[TIMESTAMP]? args[TIMESTAMP]: HConstants::LATEST_TIMESTAMP,
              args[VERSIONS]? args[VERSIONS]: 1)
          elsif columns.class == Array
            result = @table.getRow(row, columns.to_java(:string),
              args[TIMESTAMP]? args[TIMESTAMP]: HConstants::LATEST_TIMESTAMP)
          else
            raise ArgumentError.new("Failed parse column argument type " +
              args + ", " + args.class)
          end
        end
      end
      # Print out results.  Result can be Cell or RowResult.
      h = nil
      if result.instance_of? RowResult
        h = String.from_java_bytes result.getRow()
        @formatter.header(["COLUMN", "CELL"])
        if result
          for k, v in result
            column = String.from_java_bytes k
            @formatter.row([column, toString(column, v)])
          end
        end
      else
        # Presume Cells
        @formatter.header()
        if result 
          for c in result
            @formatter.row([c.toString()])
          end
        end
      end
      @formatter.footer(now)
    end
    
    def count(interval = 1000)
      now = Time.now
      columns = getAllColumns()
      cs = columns.to_java(java.lang.String)
      s = @table.getScanner(cs)
      count = 0
      i = s.iterator()
      @formatter.header()
      while i.hasNext()
        r = i.next()
        count += 1
        if count % interval == 0
          @formatter.row(["Current count: " + count.to_s + ", row: " + \
            (String.from_java_bytes r.getRow())])
        end
      end
      @formatter.footer(now, count)
    end
    
  end

  # Testing. To run this test, there needs to be an hbase cluster up and
  # running.  Then do: ${HBASE_HOME}/bin/hbase org.jruby.Main bin/HBase.rb
  if $0 == __FILE__
    # Add this directory to LOAD_PATH; presumption is that Formatter module
    # sits beside this one.  Then load it up.
    $LOAD_PATH.unshift File.dirname($PROGRAM_NAME)
    require 'Formatter'
    # Make a console formatter
    formatter = Formatter::Console.new(STDOUT)
    # Now add in java and hbase classes
    configuration = HBaseConfiguration.new()
    admin = Admin.new(configuration, formatter)
    # Drop old table.  If it does not exist, get an exception.  Catch and
    # continue
    TESTTABLE = "HBase_rb_testtable"
    begin
      admin.disable(TESTTABLE)
      admin.drop(TESTTABLE)
    rescue org.apache.hadoop.hbase.TableNotFoundException
      # Just suppress not found exception
    end
    admin.create(TESTTABLE, [{NAME => 'x', VERSIONS => 5}])
    # Presume it exists.  If it doesn't, next items will fail.
    table = Table.new(configuration, TESTTABLE, formatter) 
    for i in 1..10
      table.put('x%d' % i, 'x:%d' % i, 'x%d' % i)
    end
    table.get('x1', {COLUMN => 'x:1'})
    if formatter.rowCount() != 1
      raise IOError.new("Failed first put")
    end
    table.scan(['x:'])
    if formatter.rowCount() != 10
      raise IOError.new("Failed scan of expected 10 rows")
    end
    # Verify that limit works.
    table.scan(['x:'], {LIMIT => 3})
    if formatter.rowCount() != 3
      raise IOError.new("Failed scan of expected 3 rows")
    end
    # Should only be two rows if we start at 8 (Row x10 sorts beside x1).
    table.scan(['x:'], {STARTROW => 'x8', LIMIT => 3})
    if formatter.rowCount() != 2
      raise IOError.new("Failed scan of expected 2 rows")
    end
    # Scan between two rows
    table.scan(['x:'], {STARTROW => 'x5', ENDROW => 'x8'})
    if formatter.rowCount() != 3
      raise IOError.new("Failed endrow test")
    end
    # Verify that delete works
    table.delete('x1', 'x:1');
    table.scan(['x:1'])
    scan1 = formatter.rowCount()
    table.scan(['x:'])
    scan2 = formatter.rowCount()
    if scan1 != 0 or scan2 != 9
      raise IOError.new("Failed delete test")
    end
    # Verify that deletall works
    table.put('x2', 'x:1', 'x:1')
    table.deleteall('x2')
    table.scan(['x:2'])
    scan1 = formatter.rowCount()
    table.scan(['x:'])
    scan2 = formatter.rowCount()
    if scan1 != 0 or scan2 != 8
      raise IOError.new("Failed deleteall test")
    end
    admin.disable(TESTTABLE)
    admin.drop(TESTTABLE)
  end
end
