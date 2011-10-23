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

# This script must be used on a live cluster in order to fix .META.'s 
# MEMSTORE_SIZE back to 64MB instead of the 16KB that was configured 
# in 0.20 era. This is only required if .META. was created at that time.
#
# After running this script, HBase needs to be restarted.
#
# To see usage for this script, run: 
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main set_meta_memstore_size.rb
#
include Java
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HConstants
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
NAME = "set_meta_memstore_size.rb"


# Print usage for this script
def usage
  puts 'Usage: %s.rb]' % NAME
  exit!
end

# Get configuration to use.
c = HBaseConfiguration.create()

# Set hadoop filesystem configuration using the hbase.rootdir.
# Otherwise, we'll always use localhost though the hbase.rootdir
# might be pointing at hdfs location.
c.set("fs.default.name", c.get(HConstants::HBASE_DIR))
fs = FileSystem.get(c)

# Get a logger and a metautils instance.
LOG = LogFactory.getLog(NAME)

# Check arguments
if ARGV.size > 0
  usage
end

# Clean mentions of table from .META.
# Scan the .META. and remove all lines that begin with tablename
metaTable = HTable.new(c, HConstants::ROOT_TABLE_NAME)
scan = Scan.new()
scan.addColumn(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER);
scanner = metaTable.getScanner(scan)
while (result = scanner.next())
  rowid = Bytes.toString(result.getRow())
  LOG.info("Settting memstore to 64MB on : " + rowid);
  hriValue = result.getValue(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER)
  hri = Writables.getHRegionInfo(hriValue)
  htd = hri.getTableDesc()
  htd.setMemStoreFlushSize(64 * 1024 * 1024)
  p = Put.new(result.getRow())
  p.add(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER, Writables.getBytes(hri));
  metaTable.put(p)
end
scanner.close()
