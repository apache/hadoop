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

import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HConstants holds a bunch of HBase-related constants
 */
public interface HConstants {

  /** long constant for zero */
  static final Long ZERO_L = Long.valueOf(0L);

  //TODO: NINES is only used in HBaseAdmin and HConnectionManager. Move to client
  //      package and change visibility to default
  static final String NINES = "99999999999999";
  //TODO: ZEROS is only used in HConnectionManager and MetaScanner. Move to
  //      client package and change visibility to default
  static final String ZEROES = "00000000000000";
  
  // For migration

  /** name of version file */
  static final String VERSION_FILE_NAME = "hbase.version";
  
  /**
   * Current version of file system.
   * Version 4 supports only one kind of bloom filter.
   * Version 5 changes versions in catalog table regions.
   * Version 6 enables blockcaching on catalog tables.
   */
  public static final String FILE_SYSTEM_VERSION = "6";
  
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
  
  /** Parameter name for what region server implementation to use. */
  static final String REGION_SERVER_IMPL= "hbase.regionserver.impl";
  
  /** Default region server interface class name. */
  static final String DEFAULT_REGION_SERVER_CLASS = HRegionInterface.class.getName();

  /** Parameter name for how often threads should wake up */
  static final String THREAD_WAKE_FREQUENCY = "hbase.server.thread.wakefrequency";
  
  /** Parameter name for how often a region should should perform a major compaction */
  static final String MAJOR_COMPACTION_PERIOD = "hbase.hregion.majorcompaction";

  /** Parameter name for HBase instance root directory */
  static final String HBASE_DIR = "hbase.rootdir";
  
  /** Used to construct the name of the log directory for a region server */
  static final String HREGION_LOGDIR_NAME = "log";

  /** Name of old log file for reconstruction */
  static final String HREGION_OLDLOGFILE_NAME = "oldlogfile.log";
  
  /** Used to construct the name of the compaction directory during compaction */
  static final String HREGION_COMPACTIONDIR_NAME = "compaction.dir";
  
  /** Default maximum file size */
  static final long DEFAULT_MAX_FILE_SIZE = 256 * 1024 * 1024;
  
  /** Default size of a reservation block   */
  static final int DEFAULT_SIZE_RESERVATION_BLOCK = 1024 * 1024 * 5;
  
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
  static final byte [] ROOT_TABLE_NAME = Bytes.toBytes("-ROOT-");

  /** The META table's name. */
  static final byte [] META_TABLE_NAME = Bytes.toBytes(".META.");  

  /** delimiter used between portions of a region name */
  public static final int META_ROW_DELIMITER = ',';

  // Defines for the column names used in both ROOT and META HBase 'meta' tables.
  
  /** The ROOT and META column family (string) */
  static final String COLUMN_FAMILY_STR = "info:";
  
  /** The META historian column family (string) */
  static final String COLUMN_FAMILY_HISTORIAN_STR = "historian:";

  /** The ROOT and META column family */
  static final byte [] COLUMN_FAMILY = Bytes.toBytes(COLUMN_FAMILY_STR);
  
  /** The META historian column family */
  static final byte [] COLUMN_FAMILY_HISTORIAN = Bytes.toBytes(COLUMN_FAMILY_HISTORIAN_STR);

  /** Array of meta column names */
  static final byte[][] COLUMN_FAMILY_ARRAY = new byte[][] {COLUMN_FAMILY};
  
  /** ROOT/META column family member - contains HRegionInfo */
  static final byte [] COL_REGIONINFO =
    Bytes.toBytes(COLUMN_FAMILY_STR + "regioninfo");

  /** Array of column - contains HRegionInfo */
  static final byte[][] COL_REGIONINFO_ARRAY = new byte[][] {COL_REGIONINFO};
  
  /** ROOT/META column family member - contains HServerAddress.toString() */
  static final byte[] COL_SERVER = Bytes.toBytes(COLUMN_FAMILY_STR + "server");
  
  /** ROOT/META column family member - contains server start code (a long) */
  static final byte [] COL_STARTCODE =
    Bytes.toBytes(COLUMN_FAMILY_STR + "serverstartcode");

  /** the lower half of a split region */
  static final byte [] COL_SPLITA = Bytes.toBytes(COLUMN_FAMILY_STR + "splitA");
  
  /** the upper half of a split region */
  static final byte [] COL_SPLITB = Bytes.toBytes(COLUMN_FAMILY_STR + "splitB");
  
  /** All the columns in the catalog -ROOT- and .META. tables.
   */
  static final byte[][] ALL_META_COLUMNS = {COL_REGIONINFO, COL_SERVER,
    COL_STARTCODE, COL_SPLITA, COL_SPLITB};

  // Other constants

  /**
   * An empty instance.
   */
  static final byte [] EMPTY_BYTE_ARRAY = new byte [0];
  
  /**
   * Used by scanners, etc when they want to start at the beginning of a region
   */
  static final byte [] EMPTY_START_ROW = EMPTY_BYTE_ARRAY;
  
  /**
   * Last row in a table.
   */
  static final byte [] EMPTY_END_ROW = EMPTY_START_ROW;

  /** 
    * Used by scanners and others when they're trying to detect the end of a 
    * table 
    */
  static final byte [] LAST_ROW = EMPTY_BYTE_ARRAY;
  
  /**
   * Max length a row can have because of the limitation in TFile.
   */
  static final int MAX_ROW_LENGTH = 1024*64;
  
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
  static final int ALL_VERSIONS = Integer.MAX_VALUE;
  
  /**
   * Unlimited time-to-live.
   */
  static final int FOREVER = -1;
  
  public static final int WEEK_IN_SECONDS = 7 * 24 * 3600;

  //TODO: HBASE_CLIENT_RETRIES_NUMBER_KEY is only used by TestMigrate. Move it
  //      there.
  public static final String HBASE_CLIENT_RETRIES_NUMBER_KEY =
    "hbase.client.retries.number";

  //TODO: although the following are referenced widely to format strings for
  //      the shell. They really aren't a part of the public API. It would be
  //      nice if we could put them somewhere where they did not need to be
  //      public. They could have package visibility
  static final String NAME = "NAME";
  static final String VERSIONS = "VERSIONS";
  static final String IN_MEMORY = "IN_MEMORY";
  
  /**
   * This is a retry backoff multiplier table similar to the BSD TCP syn
   * backoff table, a bit more aggressive than simple exponential backoff.
   */ 
  public static int RETRY_BACKOFF[] = { 1, 1, 1, 2, 2, 4, 4, 8, 16, 32 };

  /** modifyTable op for replacing the table descriptor */
  public static final int MODIFY_TABLE_SET_HTD = 1;
  /** modifyTable op for forcing a split */
  public static final int MODIFY_TABLE_SPLIT = 2;
  /** modifyTable op for forcing a compaction */
  public static final int MODIFY_TABLE_COMPACT = 3;
  
  // Messages client can send master.
  public static final int MODIFY_CLOSE_REGION = MODIFY_TABLE_COMPACT + 1;
  
  public static final int MODIFY_TABLE_FLUSH = MODIFY_CLOSE_REGION + 1;
  public static final int MODIFY_TABLE_MAJOR_COMPACT = MODIFY_TABLE_FLUSH + 1;
}