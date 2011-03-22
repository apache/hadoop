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
# Script that renames table in hbase.  As written, will not work for rare
# case where there is more than one region in .META. table (You'd have to
# have a really massive hbase install).  Does the update of the hbase
# .META. and moves the directories in the filesystem.  Use at your own
# risk.  Before running you must DISABLE YOUR TABLE:
# 
# hbase> disable "YOUR_TABLE_NAME"
#
# Enable the new table after the script completes.
#
# hbase> enable "NEW_TABLE_NAME"
#
# To see usage for this script, run: 
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main rename_table.rb
#
include Java
import org.apache.hadoop.hbase.util.MetaUtils
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.regionserver.HRegion
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import java.util.TreeMap

# Name of this script
NAME = "rename_table"

# Print usage for this script
def usage
  puts 'Usage: %s.rb <OLD_NAME> <NEW_NAME>' % NAME
  exit!
end

# Passed 'dir' exists and is a directory else exception
def isDirExists(fs, dir)
  raise IOError.new("Does not exit: " + dir.toString()) unless fs.exists(dir)
  raise IOError.new("Not a directory: " + dir.toString()) unless fs.isDirectory(dir)
end

# Returns true if the region belongs to passed table
def isTableRegion(tableName, hri)
  return Bytes.equals(hri.getTableDesc().getName(), tableName)
end

# Create new HRI based off passed 'oldHRI'
def createHRI(tableName, oldHRI)
  htd = oldHRI.getTableDesc()
  newHtd = HTableDescriptor.new(tableName)
  for family in htd.getFamilies()
    newHtd.addFamily(family)
  end
  return HRegionInfo.new(newHtd, oldHRI.getStartKey(), oldHRI.getEndKey(),
    oldHRI.isSplit())
end

# Check arguments
if ARGV.size != 2
  usage
end

# Check good table names were passed.
oldTableName = ARGV[0]
newTableName = ARGV[1]

# Get configuration to use.
c = HBaseConfiguration.create()

# Set hadoop filesystem configuration using the hbase.rootdir.
# Otherwise, we'll always use localhost though the hbase.rootdir
# might be pointing at hdfs location.
c.set("fs.default.name", c.get(HConstants::HBASE_DIR))
fs = FileSystem.get(c)

# If new table directory does not exit, create it.  Keep going if already
# exists because maybe we are rerunning script because it failed first
# time. Otherwise we are overwriting a pre-existing table.
rootdir = FSUtils.getRootDir(c)
oldTableDir = fs.makeQualified(Path.new(rootdir, Path.new(oldTableName)))
isDirExists(fs, oldTableDir)
newTableDir = fs.makeQualified(Path.new(rootdir, newTableName))
if !fs.exists(newTableDir)
  fs.mkdirs(newTableDir)
end

# Get a logger and a metautils instance.
LOG = LogFactory.getLog(NAME)

# Run through the meta table moving region mentions from old to new table name.
metaTable = HTable.new(c, HConstants::META_TABLE_NAME)
# TODO: Start the scan at the old table offset in .META.
scan = Scan.new()
scanner = metaTable.getScanner(scan)
while (result = scanner.next())
  rowid = Bytes.toString(result.getRow())
  oldHRI = Writables.getHRegionInfo(result.getValue(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER))
  if !oldHRI
    raise IOError.new("HRegionInfo is null for " + rowid)
  end
  next unless isTableRegion(oldTableName.to_java_bytes, oldHRI)
  puts oldHRI.toString()
  oldRDir = Path.new(oldTableDir, Path.new(oldHRI.getEncodedName().to_s))
  if !fs.exists(oldRDir)
    LOG.warn(oldRDir.toString() + " does not exist -- region " +
      oldHRI.getRegionNameAsString())
  else
    # Now make a new HRegionInfo to add to .META. for the new region.
    newHRI = createHRI(newTableName, oldHRI)
    puts newHRI.toString()
    newRDir = Path.new(newTableDir, Path.new(newHRI.getEncodedName().to_s))
    # Move the region in filesystem
    LOG.info("Renaming " + oldRDir.toString() + " as " + newRDir.toString())
    fs.rename(oldRDir, newRDir)
    # Removing old region from meta
    LOG.info("Removing " + rowid + " from .META.")
    d = Delete.new(result.getRow())
    metaTable.delete(d)
    # Create 'new' region
    newR = HRegion.new(newTableDir, nil, fs, c, newHRI, nil)
    # Add new row. NOTE: Presumption is that only one .META. region. If not,
    # need to do the work to figure proper region to add this new region to.
    LOG.info("Adding to meta: " + newR.toString())
    bytes = Writables.getBytes(newR.getRegionInfo())
    p = Put.new(newR.getRegionName())
    p.add(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER, bytes)
    metaTable.put(p)
    # Finally update the .regioninfo under new region location so it has new name.
    regioninfofile =  Path.new(newR.getRegionDir(), HRegion::REGIONINFO_FILE)
    fs.delete(regioninfofile, true)
    out = fs.create(regioninfofile)
    newR.getRegionInfo().write(out)
    out.close()
  end
end
scanner.close()
fs.delete(oldTableDir)
LOG.info("DONE");
