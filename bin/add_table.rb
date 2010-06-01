#
# Copyright 2009 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Script adds a table back to a running hbase.
# Currently only works on if table data is in place.
#
# To see usage for this script, run:
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main addtable.rb
#
include Java
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.regionserver.HRegion
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.commons.logging.LogFactory

# Name of this script
NAME = "add_table"

# Print usage for this script
def usage
  puts 'Usage: %s.rb TABLE_DIR [alternate_tablename]' % NAME
  exit!
end

# Get configuration to use.
c = HBaseConfiguration.new()

# Set hadoop filesystem configuration using the hbase.rootdir.
# Otherwise, we'll always use localhost though the hbase.rootdir
# might be pointing at hdfs location.
c.set("fs.default.name", c.get(HConstants::HBASE_DIR))
fs = FileSystem.get(c)

# Get a logger and a metautils instance.
LOG = LogFactory.getLog(NAME)

# Check arguments
if ARGV.size < 1 || ARGV.size > 2
  usage
end

# Get cmdline args.
srcdir = fs.makeQualified(Path.new(java.lang.String.new(ARGV[0])))

if not fs.exists(srcdir)
  raise IOError.new("src dir " + srcdir.toString() + " doesn't exist!")
end

# Get table name
tableName = nil
if ARGV.size > 1
  tableName = ARGV[1]
  raise IOError.new("Not supported yet")
elsif
  # If none provided use dirname
  tableName = srcdir.getName()
end
HTableDescriptor.isLegalTableName(tableName.to_java_bytes)

# Figure locations under hbase.rootdir
# Move directories into place; be careful not to overwrite.
rootdir = FSUtils.getRootDir(c)
tableDir = fs.makeQualified(Path.new(rootdir, tableName))

# If a directory currently in place, move it aside.
if srcdir.equals(tableDir)
  LOG.info("Source directory is in place under hbase.rootdir: " + srcdir.toString());
elsif fs.exists(tableDir)
  movedTableName = tableName + "." + java.lang.System.currentTimeMillis().to_s
  movedTableDir = Path.new(rootdir, java.lang.String.new(movedTableName))
  LOG.warn("Moving " + tableDir.toString() + " aside as " + movedTableDir.toString());
  raise IOError.new("Failed move of " + tableDir.toString()) unless fs.rename(tableDir, movedTableDir)
  LOG.info("Moving " + srcdir.toString() + " to " + tableDir.toString());
  raise IOError.new("Failed move of " + srcdir.toString()) unless fs.rename(srcdir, tableDir)
end

# Clean mentions of table from .META.
# Scan the .META. and remove all lines that begin with tablename
LOG.info("Deleting mention of " + tableName + " from .META.")
metaTable = HTable.new(c, HConstants::META_TABLE_NAME)
tableNameMetaPrefix = tableName + HConstants::META_ROW_DELIMITER.chr
scan = Scan.new((tableNameMetaPrefix + HConstants::META_ROW_DELIMITER.chr).to_java_bytes)
scanner = metaTable.getScanner(scan)
# Use java.lang.String doing compares.  Ruby String is a bit odd.
tableNameStr = java.lang.String.new(tableName)
while (result = scanner.next())
  rowid = Bytes.toString(result.getRow())
  rowidStr = java.lang.String.new(rowid)
  if not rowidStr.startsWith(tableNameMetaPrefix)
    # Gone too far, break
    break
  end
  LOG.info("Deleting row from catalog: " + rowid);
  d = Delete.new(result.getRow())
  metaTable.delete(d)
end
scanner.close()

# Now, walk the table and per region, add an entry
LOG.info("Walking " + srcdir.toString() + " adding regions to catalog table")
statuses = fs.listStatus(srcdir)
for status in statuses
  next unless status.isDir()
  next if status.getPath().getName() == "compaction.dir"
  regioninfofile =  Path.new(status.getPath(), HRegion::REGIONINFO_FILE)
  unless fs.exists(regioninfofile)
    LOG.warn("Missing .regioninfo: " + regioninfofile.toString())
    next
  end
  is = fs.open(regioninfofile)
  hri = HRegionInfo.new()
  hri.readFields(is)
  is.close()
  # TODO: Need to redo table descriptor with passed table name and then recalculate the region encoded names.
  p = Put.new(hri.getRegionName())
  p.add(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER, Writables.getBytes(hri))
  metaTable.put(p)
  LOG.info("Added to catalog: " + hri.toString())
end
