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

import org.apache.hadoop.io.BytesWritable;
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
  static final String MASTER_ADDRESS = "hbase.master";
  static final String DEFAULT_MASTER_ADDRESS = "localhost:60000";

  // Key for hbase.regionserver address.
  static final String REGIONSERVER_ADDRESS = "hbase.regionserver";
  static final String DEFAULT_REGIONSERVER_ADDRESS = "localhost:60010";

  static final String THREAD_WAKE_FREQUENCY = "hbase.server.thread.wakefrequency";
  static final String HREGION_DIR = "hbase.regiondir";
  static final String DEFAULT_HREGION_DIR = "/hbase";
  static final String HREGIONDIR_PREFIX = "hregion_";
  
  // TODO: Someone may try to name a column family 'log'.  If they
  // do, it will clash with the HREGION log dir subdirectory. FIX.
  static final String HREGION_LOGDIR_NAME = "log";

  // Always store the location of the root table's HRegion.
  // This HRegion is never split.

  // region name = table + startkey + regionid. This is the row key.
  // each row in the root and meta tables describes exactly 1 region
  // Do we ever need to know all the information that we are storing?

  // The root tables' name.
  static final Text ROOT_TABLE_NAME = new Text("--ROOT--");

  // The META tables' name.
  static final Text META_TABLE_NAME = new Text("--META--");

  // Defines for the column names used in both ROOT and META HBase 'meta'
  // tables.
  static final Text COLUMN_FAMILY = new Text("info:");
  static final Text COL_REGIONINFO = new Text(COLUMN_FAMILY + "regioninfo");
  static final Text COL_SERVER = new Text(COLUMN_FAMILY + "server");
  static final Text COL_STARTCODE = new Text(COLUMN_FAMILY + "serverstartcode");

  // Other constants
  
  static final long DESIRED_MAX_FILE_SIZE = 128 * 1024 * 1024;        // 128MB
  static final String UTF8_ENCODING = "UTF-8";
  
  static final BytesWritable DELETE_BYTES = 
    new BytesWritable("HBASE::DELETEVAL".getBytes());
  
  static final BytesWritable COMPLETE_CACHEFLUSH =
    new BytesWritable("HBASE::CACHEFLUSH".getBytes());

}
