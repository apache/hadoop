/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;

/**
 * HConstants holds a bunch of HBase-related constants
 */
public interface HConstants {
  
  // Configuration parameters
  
  // TODO: URL for hbase master like hdfs URLs with host and port.
  // Like jdbc URLs?  URLs could be used to refer to table cells?
  // jdbc:mysql://[host][,failoverhost...][:port]/[database]
  // jdbc:mysql://[host][,failoverhost...][:port]/[database][?propertyName1][=propertyValue1][&propertyName2][=propertyValue2]...
  
  // Key into HBaseConfiguration for the hbase.master address.
  // TODO: Support 'local': i.e. default of all running in single
  // process.  Same for regionserver. TODO: Is having HBase homed
  // on port 60k OK?
  
  /** Parameter name for master address */
  static final String MASTER_ADDRESS = "hbase.master";
  
  static final String DEFAULT_HOST = "0.0.0.0";
  
  /** Default master address */
  static final String DEFAULT_MASTER_ADDRESS = DEFAULT_HOST + ":60000";

  /** Parameter name for hbase.regionserver address. */
  static final String REGIONSERVER_ADDRESS = "hbase.regionserver";
  
  /** Default region server address */
  static final String DEFAULT_REGIONSERVER_ADDRESS = DEFAULT_HOST + ":60010";

  /** Parameter name for how often threads should wake up */
  static final String THREAD_WAKE_FREQUENCY = "hbase.server.thread.wakefrequency";

  /** Parameter name for HBase instance root directory */
  static final String HBASE_DIR = "hbase.rootdir";
  
  /** Default HBase instance root directory */
  static final String DEFAULT_HBASE_DIR = "/hbase";
  
  /** Used to construct the name of the directory in which a HRegion resides */
  static final String HREGIONDIR_PREFIX = "hregion_";
  
  // TODO: Someone may try to name a column family 'log'.  If they
  // do, it will clash with the HREGION log dir subdirectory. FIX.
  
  /** Used to construct the name of the log directory for a region server */
  static final String HREGION_LOGDIR_NAME = "log";

  /** Name of old log file for reconstruction */
  static final String HREGION_OLDLOGFILE_NAME = "oldlogfile.log";
  
  /** Default maximum file size */
  static final long DEFAULT_MAX_FILE_SIZE = 128 * 1024 * 1024;        // 128MB

  // Always store the location of the root table's HRegion.
  // This HRegion is never split.

  // region name = table + startkey + regionid. This is the row key.
  // each row in the root and meta tables describes exactly 1 region
  // Do we ever need to know all the information that we are storing?

  /** The root table's name. */
  static final Text ROOT_TABLE_NAME = new Text("--ROOT--");

  /** The META table's name. */
  static final Text META_TABLE_NAME = new Text("--META--");

  // Defines for the column names used in both ROOT and META HBase 'meta' tables.
  
  /** The ROOT and META column family */
  static final Text COLUMN_FAMILY = new Text("info:");
  
  /** ROOT/META column family member - contains HRegionInfo */
  static final Text COL_REGIONINFO = new Text(COLUMN_FAMILY + "regioninfo");
  
  /** ROOT/META column family member - contains HServerAddress.toString() */
  static final Text COL_SERVER = new Text(COLUMN_FAMILY + "server");
  
  /** ROOT/META column family member - contains server start code (a long) */
  static final Text COL_STARTCODE = new Text(COLUMN_FAMILY + "serverstartcode");

  // Other constants

  /** When we encode strings, we always specify UTF8 encoding */
  static final String UTF8_ENCODING = "UTF-8";

  /** Value stored for a deleted item */
  static final ImmutableBytesWritable DELETE_BYTES =
    new ImmutableBytesWritable("HBASE::DELETEVAL".getBytes());

  /** Value written to HLog on a complete cache flush */
  static final ImmutableBytesWritable COMPLETE_CACHEFLUSH =
    new ImmutableBytesWritable("HBASE::CACHEFLUSH".getBytes());
}
