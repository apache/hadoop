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
# Script that copies table in hbase.  As written, will not work for rare
# case where there is more than one region in .META. table.  Does the
# update of the hbase .META. and copies the directories in filesystem.  
# HBase MUST be shutdown when you run this script.
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
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HStoreKey
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.regionserver.HLogEdit
import org.apache.hadoop.hbase.regionserver.HRegion
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import java.util.TreeMap

# Name of this script
NAME = "copy_table"

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
oldTableName = HTableDescriptor.isLegalTableName(ARGV[0].to_java_bytes)
newTableName = HTableDescriptor.isLegalTableName(ARGV[1].to_java_bytes)

# Get configuration to use.
c = HBaseConfiguration.new()

# Set hadoop filesystem configuration using the hbase.rootdir.
# Otherwise, we'll always use localhost though the hbase.rootdir
# might be pointing at hdfs location.
c.set("fs.default.name", c.get(HConstants::HBASE_DIR))
fs = FileSystem.get(c)

# If new table directory does not exit, create it.  Keep going if already
# exists because maybe we are rerunning script because it failed first
# time.
rootdir = FSUtils.getRootDir(c)
oldTableDir = Path.new(rootdir, Path.new(Bytes.toString(oldTableName)))
isDirExists(fs, oldTableDir)
newTableDir = Path.new(rootdir, Bytes.toString(newTableName))
if !fs.exists(newTableDir)
  fs.mkdirs(newTableDir)
end

# Get a logger and a metautils instance.
LOG = LogFactory.getLog(NAME)
utils = MetaUtils.new(c)

# Start.  Get all meta rows.
begin
  # Get list of all .META. regions that contain old table name
  metas = utils.getMETARows(oldTableName)
  index = 0
  for meta in metas
    # For each row we find, move its region from old to new table.
    # Need to update the encoded name in the hri as we move.
    # After move, delete old entry and create a new.
    LOG.info("Scanning " + meta.getRegionNameAsString())
    metaRegion = utils.getMetaRegion(meta)
    scanner = metaRegion.getScanner(HConstants::COL_REGIONINFO_ARRAY, oldTableName,
      HConstants::LATEST_TIMESTAMP, nil) 
    begin
      key = HStoreKey.new()
      value = TreeMap.new(Bytes.BYTES_COMPARATOR)
      while scanner.next(key, value)
        index = index + 1
        keyStr = key.toString()
        oldHRI = Writables.getHRegionInfo(value.get(HConstants::COL_REGIONINFO))
        if !oldHRI
          raise IOError.new(index.to_s + " HRegionInfo is null for " + keyStr)
        end
        unless isTableRegion(oldTableName, oldHRI)
          # If here, we passed out the table.  Break.
          break
        end
        oldRDir = Path.new(oldTableDir, Path.new(oldHRI.getEncodedName().to_s))
        if !fs.exists(oldRDir)
          LOG.warn(oldRDir.toString() + " does not exist -- region " +
            oldHRI.getRegionNameAsString())
        else
           # Now make a new HRegionInfo to add to .META. for the new region.
          newHRI = createHRI(newTableName, oldHRI)
          newRDir = Path.new(newTableDir, Path.new(newHRI.getEncodedName().to_s))
          # Move the region in filesystem
          LOG.info("Copying " + oldRDir.toString() + " as " + newRDir.toString())
          FileUtil.copy(fs, oldRDir, fs, newRDir, false, true, c)
          # Create 'new' region
          newR = HRegion.new(rootdir, utils.getLog(), fs, c, newHRI, nil)
          # Add new row. NOTE: Presumption is that only one .META. region. If not,
          # need to do the work to figure proper region to add this new region to.
          LOG.info("Adding to meta: " + newR.toString())
          HRegion.addRegionToMETA(metaRegion, newR)
          LOG.info("Done copying: " + Bytes.toString(key.getRow()))
        end
        # Need to clear value else we keep appending values.
        value.clear()
      end
    ensure
      scanner.close()
    end
  end
ensure
  utils.shutdown()
end
