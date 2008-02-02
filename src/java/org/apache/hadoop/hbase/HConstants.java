/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

/**
 * HConstants holds a bunch of HBase-related constants
 */
public interface HConstants {
  
  // For migration

  /** name of version file */
  static final String VERSION_FILE_NAME = "hbase.version";
  
  /** version of file system */
  static final String FILE_SYSTEM_VERSION = "0.1";
  
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

  /** default host address */
  static final String DEFAULT_HOST = "0.0.0.0";

  /** default port that the master listens on */
  static final int DEFAULT_MASTER_PORT = 60000;
  
  /** Default master address */
  static final String DEFAULT_MASTER_ADDRESS = DEFAULT_HOST + ":" +
    DEFAULT_MASTER_PORT;

  /** default port for master web api */
  static final int DEFAULT_MASTER_INFOPORT = 60010;

  /** Parameter name for hbase.regionserver address. */
  static final String REGIONSERVER_ADDRESS = "hbase.regionserver";
  
  /** Default region server address */
  static final String DEFAULT_REGIONSERVER_ADDRESS = DEFAULT_HOST + ":60020";

  /** default port for region server web api */
  static final int DEFAULT_REGIONSERVER_INFOPORT = 60030;

  /** Parameter name for what region server interface to use. */
  static final String REGION_SERVER_CLASS = "hbase.regionserver.class";
  
  /** Default region server interface class name. */
  static final String DEFAULT_REGION_SERVER_CLASS = HRegionInterface.class.getName();

  /** Parameter name for how often threads should wake up */
  static final String THREAD_WAKE_FREQUENCY = "hbase.server.thread.wakefrequency";

  /** Parameter name for HBase instance root directory */
  static final String HBASE_DIR = "hbase.rootdir";
  
  /** Default HBase instance root directory */
  static final String DEFAULT_HBASE_DIR = "/hbase";
  
  /** Used to construct the name of the log directory for a region server */
  static final String HREGION_LOGDIR_NAME = "log";

  /** Name of old log file for reconstruction */
  static final String HREGION_OLDLOGFILE_NAME = "oldlogfile.log";
  
  /** Default maximum file size */
  static final long DEFAULT_MAX_FILE_SIZE = 256 * 1024 * 1024;

  // Always store the location of the root table's HRegion.
  // This HRegion is never split.
  
  // region name = table + startkey + regionid. This is the row key.
  // each row in the root and meta tables describes exactly 1 region
  // Do we ever need to know all the information that we are storing?

  // Note that the name of the root table starts with "-" and the name of the
  // meta table starts with "." Why? it's a trick. It turns out that when we
  // store region names in memory, we use a SortedMap. Since "-" sorts before
  // "." (and since no other table name can start with either of these
  // characters, the root region will always be the first entry in such a Map,
  // followed by all the meta regions (which will be ordered by their starting
  // row key as well), followed by all user tables. So when the Master is 
  // choosing regions to assign, it will always choose the root region first,
  // followed by the meta regions, followed by user regions. Since the root
  // and meta regions always need to be on-line, this ensures that they will
  // be the first to be reassigned if the server(s) they are being served by
  // should go down.

  /** The root table's name.*/
  static final Text ROOT_TABLE_NAME = new Text("-ROOT-");

  /** The META table's name. */
  static final Text META_TABLE_NAME = new Text(".META.");

  // Defines for the column names used in both ROOT and META HBase 'meta' tables.
  
  /** The ROOT and META column family (string) */
  static final String COLUMN_FAMILY_STR = "info:";

  /** The ROOT and META column family (Text) */
  static final Text COLUMN_FAMILY = new Text(COLUMN_FAMILY_STR);

  /** Array of meta column names */
  static final Text [] COLUMN_FAMILY_ARRAY = new Text [] {COLUMN_FAMILY};
  
  /** ROOT/META column family member - contains HRegionInfo */
  static final Text COL_REGIONINFO = new Text(COLUMN_FAMILY + "regioninfo");

  /** Array of column - contains HRegionInfo */
  static final Text[] COL_REGIONINFO_ARRAY = new Text [] {COL_REGIONINFO};
  
  /** ROOT/META column family member - contains HServerAddress.toString() */
  static final Text COL_SERVER = new Text(COLUMN_FAMILY + "server");
  
  /** ROOT/META column family member - contains server start code (a long) */
  static final Text COL_STARTCODE = new Text(COLUMN_FAMILY + "serverstartcode");

  /** the lower half of a split region */
  static final Text COL_SPLITA = new Text(COLUMN_FAMILY_STR + "splitA");
  
  /** the upper half of a split region */
  static final Text COL_SPLITB = new Text(COLUMN_FAMILY_STR + "splitB");
  
  /** All the columns in the catalog -ROOT- and .META. tables.
   */
  static final Text[] ALL_META_COLUMNS = {COL_REGIONINFO, COL_SERVER,
    COL_STARTCODE, COL_SPLITA, COL_SPLITB};

  // Other constants

  /**
   * An empty instance of Text.
   */
  static final Text EMPTY_TEXT = new Text();
  
  /**
   * Used by scanners, etc when they want to start at the beginning of a region
   */
  static final Text EMPTY_START_ROW = EMPTY_TEXT;

  /** 
    * Used by scanners and others when they're trying to detect the end of a 
    * table 
    */
  static final Text LAST_ROW = EMPTY_TEXT;
  
  /** When we encode strings, we always specify UTF8 encoding */
  static final String UTF8_ENCODING = "UTF-8";

  /**
   * Timestamp to use when we want to refer to the latest cell.
   * This is the timestamp sent by clients when no timestamp is specified on
   * commit.
   */
  static final long LATEST_TIMESTAMP = Long.MAX_VALUE;

  /**
   * Define for 'return-all-versions'.
   */
  static final int ALL_VERSIONS = -1;
}
