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
include_class('java.lang.Long') {|package,name| "J#{name}" }
include_class('java.lang.Boolean') {|package,name| "J#{name}" }

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.io.hfile.Compression
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.ZooKeeperMain

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
  MAXLENGTH = "MAXLENGTH"
  CACHE_BLOCKS = "CACHE_BLOCKS"
  REPLICATION_SCOPE = "REPLICATION_SCOPE"

  # Wrapper for org.apache.hadoop.hbase.client.HBaseAdmin
  class Admin
    def initialize(configuration, formatter)
      @admin = HBaseAdmin.new(configuration)
      connection = @admin.getConnection()
      @zkWrapper = connection.getZooKeeperWrapper()
      zk = @zkWrapper.getZooKeeper()
      @zkMain = ZooKeeperMain.new(zk)
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
      @formatter.header(["DESCRIPTION", "ENABLED"], [64])
      found = false
      tables = @admin.listTables().to_a
      tables.push(HTableDescriptor::META_TABLEDESC, HTableDescriptor::ROOT_TABLEDESC)
      for t in tables
        if t.getNameAsString() == tableName
          @formatter.row([t.to_s, "%s" % [@admin.isTableEnabled(tableName)]], true, [64])
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
      g = Get.new(bytes)
      g.addColumn(HConstants::CATALOG_FAMILY,
        HConstants::REGIONINFO_QUALIFIER)
      hriBytes = meta.get(g).value()
      hri = Writables.getWritable(hriBytes, HRegionInfo.new());
      hri.setOffline(onOrOff)
      put = Put.new(bytes)
      put.add(HConstants::CATALOG_FAMILY,
        HConstants::REGIONINFO_QUALIFIER, Writables.getBytes(hri))
      meta.put(put);
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
        flush(HConstants::META_TABLE_NAME);
        major_compact(HConstants::META_TABLE_NAME);
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
          htd.addFamily(HColumnDescriptor.new(arg))
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
        @admin.deleteColumn(tableName, args[NAME])
      elsif method == "table_att"
        if args[MAX_FILESIZE]
          htd.setMaxFileSize(JLong.valueOf(args[MAX_FILESIZE])) 
        end
        if args[READONLY] 
          htd.setReadOnly(JBoolean.valueOf(args[READONLY])) 
        end  
        if args[MEMSTORE_FLUSHSIZE]
          htd.setMemStoreFlushSize(JLong.valueOf(args[MEMSTORE_FLUSHSIZE]))
        end
        if args[DEFERRED_LOG_FLUSH]
          htd.setDeferredLogFlush(JBoolean.valueOf(args[DEFERRED_LOG_FLUSH]))
        end
        @admin.modifyTable(tableName.to_java_bytes, htd)
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

    def shutdown()
      @admin.shutdown()
    end

    def status(format)
      status = @admin.getClusterStatus()
      if format != nil and format == "detailed"
        puts("version %s" % [ status.getHBaseVersion() ])
        # Put regions in transition first because usually empty
        puts("%d regionsInTransition" % status.getRegionsInTransition().size())
        for k, v in status.getRegionsInTransition()
          puts("    %s" % [v])
        end
        puts("%d live servers" % [ status.getServers() ])
        for server in status.getServerInfo()
          puts("    %s:%d %d" % \
            [ server.getServerAddress().getHostname(),  \
              server.getServerAddress().getPort(), server.getStartCode() ])
          puts("        %s" % [ server.getLoad().toString() ])
          for region in server.getLoad().getRegionsLoad()
            puts("        %s" % [ region.getNameAsString() ])
            puts("            %s" % [ region.toString() ])
          end
        end
        puts("%d dead servers" % [ status.getDeadServers() ])
        for server in status.getDeadServerNames()
          puts("    %s" % [ server ])
        end
      elsif format != nil and format == "simple"
        load = 0
        regions = 0
        puts("%d live servers" % [ status.getServers() ])
        for server in status.getServerInfo()
          puts("    %s:%d %d" % \
            [ server.getServerAddress().getHostname(),  \
              server.getServerAddress().getPort(), server.getStartCode() ])
          puts("        %s" % [ server.getLoad().toString() ])
          load += server.getLoad().getNumberOfRequests()
          regions += server.getLoad().getNumberOfRegions()
        end
        puts("%d dead servers" % [ status.getDeadServers() ])
        for server in status.getDeadServerNames()
          puts("    %s" % [ server ])
        end
        puts("Aggregate load: %d, regions: %d" % [ load , regions ] )
      else
        puts("%d servers, %d dead, %.4f average load" % \
          [ status.getServers(), status.getDeadServers(), \
            status.getAverageLoad()])
      end
    end
    def hcd(arg)
      # Return a new HColumnDescriptor made of passed args
      # TODO: This is brittle code.
      # Here is current HCD constructor:
      # public HColumnDescriptor(final byte [] familyName, final int maxVersions,
      # final String compression, final boolean inMemory,
      # final boolean blockCacheEnabled, final int blocksize,
      # final int timeToLive, final boolean bloomFilter, final int scope) {
      name = arg[NAME]
      raise ArgumentError.new("Column family " + arg + " must have a name") \
        unless name
      # TODO: What encoding are Strings in jruby?
      return HColumnDescriptor.new(name.to_java_bytes,
        # JRuby uses longs for ints. Need to convert.  Also constants are String 
        arg[VERSIONS]? JInteger.new(arg[VERSIONS]): HColumnDescriptor::DEFAULT_VERSIONS,
        arg[HColumnDescriptor::COMPRESSION]? arg[HColumnDescriptor::COMPRESSION]: HColumnDescriptor::DEFAULT_COMPRESSION,
        arg[IN_MEMORY]? JBoolean.valueOf(arg[IN_MEMORY]): HColumnDescriptor::DEFAULT_IN_MEMORY,
        arg[HColumnDescriptor::BLOCKCACHE]? JBoolean.valueOf(arg[HColumnDescriptor::BLOCKCACHE]): HColumnDescriptor::DEFAULT_BLOCKCACHE,
        arg[HColumnDescriptor::BLOCKSIZE]? JInteger.valueOf(arg[HColumnDescriptor::BLOCKSIZE]): HColumnDescriptor::DEFAULT_BLOCKSIZE,
        arg[HColumnDescriptor::TTL]? JInteger.new(arg[HColumnDescriptor::TTL]): HColumnDescriptor::DEFAULT_TTL,
        arg[HColumnDescriptor::BLOOMFILTER]? JBoolean.valueOf(arg[HColumnDescriptor::BLOOMFILTER]): HColumnDescriptor::DEFAULT_BLOOMFILTER,
        arg[HColumnDescriptor::REPLICATION_SCOPE]? JInteger.new(arg[REPLICATION_SCOPE]): HColumnDescriptor::DEFAULT_REPLICATION_SCOPE)
    end

    def zk(args)
      line = args.join(' ')
      line = 'help' if line.empty?
      @zkMain.executeLine(line)
    end

    def zk_dump
      puts @zkWrapper.dump
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
      d = Delete.new(row.to_java_bytes, timestamp, nil)
      split = KeyValue.parseColumn(column.to_java_bytes)
      d.deleteColumn(split[0], split.length > 1 ? split[1] : nil, timestamp)
      @table.delete(d)
      @formatter.header()
      @formatter.footer(now)
    end

    def deleteall(row, column = nil, timestamp = HConstants::LATEST_TIMESTAMP)
      now = Time.now 
      d = Delete.new(row.to_java_bytes, timestamp, nil)
      if column != nil
        split = KeyValue.parseColumn(column.to_java_bytes)
        d.deleteColumns(split[0], split.length > 1 ? split[1] : nil, timestamp)
      end
      @table.delete(d)
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
      maxlength = -1
      if args != nil and args.length > 0
        limit = args["LIMIT"] || -1 
        maxlength = args["MAXLENGTH"] || -1 
        filter = args["FILTER"] || nil
        startrow = args["STARTROW"] || ""
        stoprow = args["STOPROW"] || nil
        timestamp = args["TIMESTAMP"] || nil
        columns = args["COLUMNS"] || getAllColumns()
        cache = args["CACHE_BLOCKS"] || true
        versions = args["VERSIONS"] || 1
        
        if columns.class == String
          columns = [columns]
        elsif columns.class != Array
          raise ArgumentError.new("COLUMNS must be specified as a String or an Array")
        end
        if stoprow
          scan = Scan.new(startrow.to_java_bytes, stoprow.to_java_bytes)
        else
          scan = Scan.new(startrow.to_java_bytes)
        end
        for c in columns
          scan.addColumns(c)
        end
        if filter != nil
          scan.setFilter(filter)
        end
        if timestamp != nil
          scan.setTimeStamp(timestamp)
        end
        scan.setCacheBlocks(cache)
        scan.setMaxVersions(versions) if versions > 1
      else
        scan = Scan.new()
      end
      s = @table.getScanner(scan)
      count = 0
      @formatter.header(["ROW", "COLUMN+CELL"])
      i = s.iterator()
      while i.hasNext()
        r = i.next()
        row = Bytes::toStringBinary(r.getRow())
        if limit != -1 and count >= limit
          break
        end
        for kv in r.list
          family = String.from_java_bytes kv.getFamily()
          qualifier = Bytes::toStringBinary(kv.getQualifier())
          column = family + ':' + qualifier
          cell = toString(column, kv, maxlength)
          @formatter.row([row, "column=%s, %s" % [column, cell]])
        end
        count += 1
      end
      @formatter.footer(now, count)
    end

    def put(row, column, value, timestamp = nil)
      now = Time.now 
      p = Put.new(row.to_java_bytes)
      split = KeyValue.parseColumn(column.to_java_bytes)
      if split.length > 1
        if timestamp
          p.add(split[0], split[1], timestamp, value.to_java_bytes)
        else
          p.add(split[0], split[1], value.to_java_bytes)
        end
      else
        if timestamp
          p.add(split[0], nil, timestamp, value.to_java_bytes)
        else
          p.add(split[0], nil, value.to_java_bytes)
        end
      end
      @table.put(p)
      @formatter.header()
      @formatter.footer(now)
    end

    def incr(row, column, value = nil)
      now = Time.now 
      split = KeyValue.parseColumn(column.to_java_bytes)
      family = split[0]
      qualifier = nil
      if split.length > 1
        qualifier = split[1]
      end
      if value == nil
        value = 1
      end
      @table.incrementColumnValue(row.to_java_bytes, family, qualifier, value)
      @formatter.header()
      @formatter.footer(now)
    end

    def isMetaTable()
      tn = @table.getTableName()
      return Bytes.equals(tn, HConstants::META_TABLE_NAME) ||
        Bytes.equals(tn, HConstants::ROOT_TABLE_NAME)
    end

    # Make a String of the passed kv 
    # Intercept cells whose format we know such as the info:regioninfo in .META.
    def toString(column, kv, maxlength)
      if isMetaTable()
        if column == 'info:regioninfo'
          hri = Writables.getHRegionInfoOrNull(kv.getValue())
          return "timestamp=%d, value=%s" % [kv.getTimestamp(), hri.toString()]
        elsif column == 'info:serverstartcode'
          return "timestamp=%d, value=%s" % [kv.getTimestamp(), \
            Bytes.toLong(kv.getValue())]
        end
      end
      val = "timestamp=" + kv.getTimestamp().to_s + ", value=" + Bytes::toStringBinary(kv.getValue())
      maxlength != -1 ? val[0, maxlength] : val    
    end
  
    # Get from table
    def get(row, args = {})
      now = Time.now 
      result = nil
      if args == nil or args.length == 0 or (args.length == 1 and args[MAXLENGTH] != nil)
        get = Get.new(row.to_java_bytes)
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
            raise ArgumentError, "Failed parse of #{args}, #{args.class}"
          end
          get = Get.new(row.to_java_bytes, ts)
        else
          get = Get.new(row.to_java_bytes)
          # Columns are non-nil
          if columns.class == String
            # Single column
            split = KeyValue.parseColumn(columns.to_java_bytes)
            if (split.length > 1) 
              get.addColumn(split[0], split[1])
            else
              get.addFamily(split[0])
            end
          elsif columns.class == Array
            for column in columns
              split = KeyValue.parseColumn(columns.to_java_bytes)
              if (split.length > 1)
                get.addColumn(split[0], split[1])
              else
                get.addFamily(split[0])
              end
            end
          else
            raise ArgumentError.new("Failed parse column argument type " +
              args + ", " + args.class)
          end
          get.setMaxVersions(args[VERSIONS] ? args[VERSIONS] : 1)
          if args[TIMESTAMP] 
            get.setTimeStamp(args[TIMESTAMP])
          end
        end
      end
      result = @table.get(get)
      # Print out results.  Result can be Cell or RowResult.
      maxlength = args[MAXLENGTH] || -1
      @formatter.header(["COLUMN", "CELL"])
      if !result.isEmpty()
        for kv in result.list()
          family = String.from_java_bytes kv.getFamily()
          qualifier = Bytes::toStringBinary(kv.getQualifier())
          column = family + ':' + qualifier
          @formatter.row([column, toString(column, kv, maxlength)])
        end
      end
      @formatter.footer(now)
    end
    
    def count(interval = 1000)
      now = Time.now
      scan = Scan.new()
      scan.setCacheBlocks(false)
      # We can safely set scanner caching with the first key only filter
      scan.setCaching(10)
      scan.setFilter(FirstKeyOnlyFilter.new())
      s = @table.getScanner(scan)
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
    table.get('x1', {COLUMNS => 'x:1'})
    if formatter.rowCount() != 1
      raise IOError.new("Failed first put")
    end
    table.scan({COLUMNS => ['x:']})
    if formatter.rowCount() != 10
      raise IOError.new("Failed scan of expected 10 rows")
    end
    # Verify that limit works.
    table.scan({COLUMNS => ['x:'], LIMIT => 4})
    if formatter.rowCount() != 3
      raise IOError.new("Failed scan of expected 3 rows")
    end
    # Should only be two rows if we start at 8 (Row x10 sorts beside x1).
    table.scan({COLUMNS => ['x:'], STARTROW => 'x8', LIMIT => 3})
    if formatter.rowCount() != 2
      raise IOError.new("Failed scan of expected 2 rows")
    end
    # Scan between two rows
    table.scan({COLUMNS => ['x:'], STARTROW => 'x5', ENDROW => 'x8'})
    if formatter.rowCount() != 3
      raise IOError.new("Failed endrow test")
    end
    # Verify that incr works
    table.incr('incr1', 'c:1');
    table.scan({COLUMNS => ['c:1']})
    if formatter.rowCount() != 1
      raise IOError.new("Failed incr test")
    end
    # Verify that delete works
    table.delete('x1', 'x:1');
    table.scan({COLUMNS => ['x:1']})
    scan1 = formatter.rowCount()
    table.scan({COLUMNS => ['x:']})
    scan2 = formatter.rowCount()
    if scan1 != 0 or scan2 != 9
      raise IOError.new("Failed delete test")
    end
    # Verify that deletall works
    table.put('x2', 'x:1', 'x:1')
    table.deleteall('x2')
    table.scan({COLUMNS => ['x:2']})
    scan1 = formatter.rowCount()
    table.scan({COLUMNS => ['x:']})
    scan2 = formatter.rowCount()
    if scan1 != 0 or scan2 != 8
      raise IOError.new("Failed deleteall test")
    end
    admin.disable(TESTTABLE)
    admin.drop(TESTTABLE)
  end
end
