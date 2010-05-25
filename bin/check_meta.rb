# Script looks at hbase .META. table verifying its content is coherent.
# 
# To see usage for this script, run: 
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main check_meta.rb --help
#

# Copyright 2010 The Apache Software Foundation
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
include Java
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hbase.util.VersionInfo
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.Put

# Name of this script
NAME = 'check_meta'

# Print usage for this script
def usage
  puts 'Usage: %s.rb [--fix]' % NAME
  puts ' fix   Try to fixup meta issues'
  puts 'Script checks consistency of the .META. table.  It reports if .META. has missing entries.'
  puts 'If you pass "--fix", it will try looking in the filesystem for the dropped region and if it'
  puts 'finds a likely candidate, it will try pluggin the .META. hole.'
  exit!
end

def isFixup
  # Are we to do fixup during this run
  usage if ARGV.size > 1
  fixup = nil
  if ARGV.size == 1
    usage unless ARGV[0].downcase.match('--fix.*')
    fixup = 1
  end
  return fixup
end

def getConfiguration
  hbase_twenty = VersionInfo.getVersion().match('0\.20\..*')
  # Get configuration to use.
  if hbase_twenty
    c = HBaseConfiguration.new()
  else
    c = HBaseConfiguration.create()
  end
  # Set hadoop filesystem configuration using the hbase.rootdir.
  # Otherwise, we'll always use localhost though the hbase.rootdir
  # might be pointing at hdfs location. Do old and new key for fs.
  c.set("fs.default.name", c.get(HConstants::HBASE_DIR))
  c.set("fs.defaultFS", c.get(HConstants::HBASE_DIR))
  return c
end

def fixup(leftEdge, rightEdge, metatable, fs, rootdir)
  plugged = nil
  # Try and fix the passed holes in meta.
  tabledir = HTableDescriptor::getTableDir(rootdir, leftEdge.getTableDesc().getName())
  statuses = fs.listStatus(tabledir) 
  for status in statuses
    next unless status.isDir()
    next if status.getPath().getName() == "compaction.dir"
    regioninfofile =  Path.new(status.getPath(), ".regioninfo")
    unless fs.exists(regioninfofile)
      LOG.warn("Missing .regioninfo: " + regioninfofile.toString())
      next
    end
    is = fs.open(regioninfofile) 
    hri = HRegionInfo.new()
    hri.readFields(is)
    is.close() 
    next unless Bytes.equals(leftEdge.getEndKey(), hri.getStartKey())
    # TODO: Check against right edge to make sure this addition does not overflow right edge. 
    # TODO: Check that the schema matches both left and right edges schemas.
    p = Put.new(hri.getRegionName())
    p.add(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER, Writables.getBytes(hri))
    metatable.put(p)
    LOG.info("Plugged hole in .META. at: " + hri.toString())
    plugged = true
  end
  return plugged
end

fixup = isFixup()

# Get configuration
conf = getConfiguration()

# Filesystem
fs = FileSystem.get(conf)

# Rootdir
rootdir = FSUtils.getRootDir(conf)

# Get a logger and a metautils instance.
LOG = LogFactory.getLog(NAME)

# Scan the .META. looking for holes
metatable = HTable.new(conf, HConstants::META_TABLE_NAME)
scan = Scan.new()
scanner = metatable.getScanner(scan)
oldHRI = nil
bad = nil 
while (result = scanner.next())
  rowid = Bytes.toString(result.getRow())
  rowidStr = java.lang.String.new(rowid)
  bytes = result.getValue(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER)
  hri = Writables.getHRegionInfo(bytes)
  if oldHRI
    if oldHRI.isOffline() && Bytes.equals(oldHRI.getStartKey(), hri.getStartKey())
      # Presume offlined parent
    elsif Bytes.equals(oldHRI.getEndKey(), hri.getStartKey())
      # Start key of next matches end key of previous
    else
      LOG.warn("hole after " + oldHRI.toString())
      if fixup
        bad = 1 unless fixup(oldHRI, hri, metatable, fs, rootdir)
      else
        bad = 1
      end
    end
  end 
  oldHRI = hri
end
scanner.close()
if bad
  LOG.info(".META. has holes")
else
  LOG.info(".META. is healthy")
end

# Return 0 if meta is good, else non-zero.
exit bad
