# Script that takes over from org.apache.hadoop.hbase.mapreduce.HFileOutputFormat.
# Pass it output directory of HFileOutputFormat. It will read the passed files,
# move them into place and update the catalog table appropriately.  Warning:
# it will overwrite anything that exists already for passed table.
# It expects hbase to be up and running so it can insert table info.
#
# To see usage for this script, run: 
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main loadtable.rb
#
include Java
import java.util.TreeMap
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.io.hfile.HFile
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.OutputLogFilter
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

# Name of this script
NAME = "loadtable"

# Print usage for this script
def usage
  puts 'Usage: %s.rb TABLENAME HFILEOUTPUTFORMAT_OUTPUT_DIR' % NAME
  exit!
end

# Passed 'dir' exists and is a directory else exception
def isDirExists(fs, dir)
  raise IOError.new("Does not exit: " + dir.toString()) unless fs.exists(dir)
  raise IOError.new("Not a directory: " + dir.toString()) unless fs.isDirectory(dir)
end

# Check arguments
if ARGV.size != 2
  usage
end

# Check good table names were passed.
tableName = HTableDescriptor.isLegalTableName(ARGV[0].to_java_bytes)
outputdir = Path.new(ARGV[1])

# Get configuration to use.
c = HBaseConfiguration.new()
# Get a logger and a metautils instance.
LOG = LogFactory.getLog(NAME)

# Set hadoop filesystem configuration using the hbase.rootdir.
# Otherwise, we'll always use localhost though the hbase.rootdir
# might be pointing at hdfs location.
c.set("fs.default.name", c.get(HConstants::HBASE_DIR))
fs = FileSystem.get(c)

# If hfiles directory does not exist, exit.
isDirExists(fs, outputdir)
# Create table dir if it doesn't exist.
rootdir = FSUtils.getRootDir(c)
tableDir = Path.new(rootdir, Path.new(Bytes.toString(tableName)))
fs.mkdirs(tableDir) unless fs.exists(tableDir)

# Start. Per hfile, move it, and insert an entry in catalog table.
families = fs.listStatus(outputdir, OutputLogFilter.new())
throw IOError.new("Can do one family only") if families.length > 1
# Read meta on all files. Put in map keyed by end key.
map = TreeMap.new(Bytes::ByteArrayComparator.new())
family = families[0]
# Make sure this subdir exists under table
hfiles = fs.listStatus(family.getPath())
LOG.info("Found " + hfiles.length.to_s + " hfiles");
count = 0
for hfile in hfiles
  reader = HFile::Reader.new(fs, hfile.getPath(), nil, false)
  begin
    fileinfo = reader.loadFileInfo() 
    lastkey = reader.getLastKey()
    # Last key is row/column/ts.  We just want the row part.
    rowlen = Bytes.toShort(lastkey)
    LOG.info(count.to_s + " read lastrow of " +
      Bytes.toString(lastkey[2, rowlen]) + " from " + hfile.getPath().toString())
    map.put(lastkey[2, rowlen], [hfile, fileinfo])
    count = count + 1
  ensure
    reader.close()
  end
end
# Now I have sorted list of fileinfo+paths.  Start insert.
# Get a client on catalog table.
meta = HTable.new(c, HConstants::META_TABLE_NAME)
# I can't find out from hfile how its compressed.
# Using all defaults. Change manually after loading if
# something else wanted in column or table attributes.
familyName = family.getPath().getName()
hcd = HColumnDescriptor.new(familyName)
htd = HTableDescriptor.new(tableName)
htd.addFamily(hcd)
previouslastkey = HConstants::EMPTY_START_ROW
count = 0
for i in map.keySet()
  tuple = map.get(i)
  startkey = previouslastkey
  count = 1 + count
  lastkey = i
  if count == map.size()
    # Then we are at last key. Set it to special indicator
    lastkey = HConstants::EMPTY_START_ROW
  end
  previouslastkey = lastkey
  hri = HRegionInfo.new(htd, startkey, lastkey)  
  LOG.info(hri.toString())
  hfile = tuple[0].getPath()
  rdir = Path.new(Path.new(tableDir, hri.getEncodedName().to_s), familyName)
  fs.mkdirs(rdir)
  tgt = Path.new(rdir, hfile.getName())
  fs.rename(hfile, tgt)
  LOG.info("Moved " + hfile.toString() + " to " + tgt.toString())
  p = Put.new(hri.getRegionName())
  p.add(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER, Writables.getBytes(hri))
  meta.put(p)
  LOG.info("Inserted " + hri.toString())
end
