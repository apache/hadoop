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

import org.apache.hadoop.io.Text;

/*******************************************************************************
 * HConstants holds a bunch of HBase-related constants
 ******************************************************************************/
public interface HConstants {
  
  // Configuration parameters
  
  static final String MASTER_DEFAULT_NAME = "hbase.master.default.name";
  static final String HREGION_DIR = "hbase.regiondir";
  static final String DEFAULT_HREGION_DIR = "/hbase";
  static final String HREGIONDIR_PREFIX = "hregion_";

  // Always store the location of the root table's HRegion.
  // This HRegion is never split.

  // region name = table + startkey + regionid. This is the row key.
  // each row in the root and meta tables describes exactly 1 region
  // Do we ever need to know all the information that we are storing?
  
  static final Text ROOT_TABLE_NAME = new Text("--ROOT--");
  static final Text ROOT_COLUMN_FAMILY = new Text("info");
  static final Text ROOT_COL_REGIONINFO = new Text(ROOT_COLUMN_FAMILY + ":" + "regioninfo");
  static final Text ROOT_COL_SERVER = new Text(ROOT_COLUMN_FAMILY + ":" + "server");
  static final Text ROOT_COL_STARTCODE = new Text(ROOT_COLUMN_FAMILY + ":" + "serverstartcode");

  static final Text META_TABLE_NAME = new Text("--META--");
  static final Text META_COLUMN_FAMILY = new Text(ROOT_COLUMN_FAMILY);
  static final Text META_COL_REGIONINFO = new Text(ROOT_COL_REGIONINFO);
  static final Text META_COL_SERVER = new Text(ROOT_COL_SERVER);
  static final Text META_COL_STARTCODE = new Text(ROOT_COL_STARTCODE);

  // Other constants
  
  static final long DESIRED_MAX_FILE_SIZE = 128 * 1024 * 1024;        // 128MB
  static final String UTF8_ENCODING = "UTF-8";
  
}
