/**
 * Copyright 2010 The Apache Software Foundation
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
  static final String NINES = "99999999999999";
  static final String ZEROES = "00000000000000";

  // For migration

  /** name of version file */
  static final String VERSION_FILE_NAME = "hbase.version";

  /**
   * Current version of file system.
   * Version 4 supports only one kind of bloom filter.
   * Version 5 changes versions in catalog table regions.
   * Version 6 enables blockcaching on catalog tables.
   * Version 7 introduces hfile -- hbase 0.19 to 0.20..
   */
  // public static final String FILE_SYSTEM_VERSION = "6";
  public static final String FILE_SYSTEM_VERSION = "7";

  // Configuration parameters

  //TODO: Is having HBase homed on port 60k OK?

  /** Cluster is in distributed mode or not */
  static final String CLUSTER_DISTRIBUTED = "hbase.cluster.distributed";

  /** Cluster is standalone or pseudo-distributed */
  static final String CLUSTER_IS_LOCAL = "false";

  /** Cluster is fully-distributed */
  static final String CLUSTER_IS_DISTRIBUTED = "true";

  /** default host address */
  static final String DEFAULT_HOST = "0.0.0.0";

  /** Parameter name for port master listens on. */
  static final String MASTER_PORT = "hbase.master.port";

  /** default port that the master listens on */
  static final int DEFAULT_MASTER_PORT = 60000;

  /** default port for master web api */
  static final int DEFAULT_MASTER_INFOPORT = 60010;

  /** Name of ZooKeeper quorum configuration parameter. */
  static final String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";

  /** Name of ZooKeeper config file in conf/ directory. */
  static final String ZOOKEEPER_CONFIG_NAME = "zoo.cfg";

  /** Parameter name for number of times to retry writes to ZooKeeper. */
  static final String ZOOKEEPER_RETRIES = "zookeeper.retries";
  /** Default number of times to retry writes to ZooKeeper. */
  static final int DEFAULT_ZOOKEEPER_RETRIES = 5;

  /** Parameter name for ZooKeeper pause between retries. In milliseconds. */
  static final String ZOOKEEPER_PAUSE = "zookeeper.pause";
  /** Default ZooKeeper pause value. In milliseconds. */
  static final int DEFAULT_ZOOKEEPER_PAUSE = 2 * 1000;

  /** default client port that the zookeeper listens on */
  static final int DEFAULT_ZOOKEPER_CLIENT_PORT = 2181;

  /** Parameter name for the root dir in ZK for this cluster */
  static final String ZOOKEEPER_ZNODE_PARENT = "zookeeper.znode.parent";

  static final String DEFAULT_ZOOKEEPER_ZNODE_PARENT = "/hbase";

  /** Parameter name for port region server listens on. */
  static final String REGIONSERVER_PORT = "hbase.regionserver.port";

  /** Default port region server listens on. */
  static final int DEFAULT_REGIONSERVER_PORT = 60020;

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

  /** Used to construct the name of the log directory for a region server
   * Use '.' as a special character to seperate the log files from table data */
  static final String HREGION_LOGDIR_NAME = ".logs";

  /** Like the previous, but for old logs that are about to be deleted */
  static final String HREGION_OLDLOGDIR_NAME = ".oldlogs";

  /** Name of old log file for reconstruction */
  static final String HREGION_OLDLOGFILE_NAME = "oldlogfile.log";

  /** Used to construct the name of the compaction directory during compaction */
  static final String HREGION_COMPACTIONDIR_NAME = "compaction.dir";

  /** Default maximum file size */
  static final long DEFAULT_MAX_FILE_SIZE = 256 * 1024 * 1024;

  /** Default size of a reservation block   */
  static final int DEFAULT_SIZE_RESERVATION_BLOCK = 1024 * 1024 * 5;

  /** Maximum value length, enforced on KeyValue construction */
  static final int MAXIMUM_VALUE_LENGTH = Integer.MAX_VALUE;

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


  //
  // New stuff.  Making a slow transition.
  //

  /** The root table's name.*/
  static final byte [] ROOT_TABLE_NAME = Bytes.toBytes("-ROOT-");

  /** The META table's name. */
  static final byte [] META_TABLE_NAME = Bytes.toBytes(".META.");

  /** delimiter used between portions of a region name */
  public static final int META_ROW_DELIMITER = ',';

  /** The catalog family as a string*/
  static final String CATALOG_FAMILY_STR = "info";

  /** The catalog family */
  static final byte [] CATALOG_FAMILY = Bytes.toBytes(CATALOG_FAMILY_STR);

  /** The catalog historian family */
  static final byte [] CATALOG_HISTORIAN_FAMILY = Bytes.toBytes("historian");

  /** The regioninfo column qualifier */
  static final byte [] REGIONINFO_QUALIFIER = Bytes.toBytes("regioninfo");

  /** The server column qualifier */
  static final byte [] SERVER_QUALIFIER = Bytes.toBytes("server");

  /** The startcode column qualifier */
  static final byte [] STARTCODE_QUALIFIER = Bytes.toBytes("serverstartcode");

  /** The lower-half split region column qualifier */
  static final byte [] SPLITA_QUALIFIER = Bytes.toBytes("splitA");

  /** The upper-half split region column qualifier */
  static final byte [] SPLITB_QUALIFIER = Bytes.toBytes("splitB");

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
  static final int MAX_ROW_LENGTH = Short.MAX_VALUE;

  /** When we encode strings, we always specify UTF8 encoding */
  static final String UTF8_ENCODING = "UTF-8";

  /**
   * Timestamp to use when we want to refer to the latest cell.
   * This is the timestamp sent by clients when no timestamp is specified on
   * commit.
   */
  static final long LATEST_TIMESTAMP = Long.MAX_VALUE;

  /**
   * LATEST_TIMESTAMP in bytes form
   */
  static final byte [] LATEST_TIMESTAMP_BYTES = Bytes.toBytes(LATEST_TIMESTAMP);

  /**
   * Define for 'return-all-versions'.
   */
  static final int ALL_VERSIONS = Integer.MAX_VALUE;

  /**
   * Unlimited time-to-live.
   */
//  static final int FOREVER = -1;
  static final int FOREVER = Integer.MAX_VALUE;

  /**
   * Seconds in a week
   */
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

  public static final String REGION_IMPL = "hbase.hregion.impl";

  /** modifyTable op for replacing the table descriptor */
  public static enum Modify {
    CLOSE_REGION,
    TABLE_COMPACT,
    TABLE_FLUSH,
    TABLE_MAJOR_COMPACT,
    TABLE_SET_HTD,
    TABLE_SPLIT
  }

  /**
   * Scope tag for locally scoped data.
   * This data will not be replicated.
   */
  public static final int REPLICATION_SCOPE_LOCAL = 0;

  /**
   * Scope tag for globally scoped data.
   * This data will be replicated to all peers.
   */
  public static final int REPLICATION_SCOPE_GLOBAL = 1;

  /**
   * Default cluster ID, cannot be used to identify a cluster so a key with
   * this value means it wasn't meant for replication.
   */
  public static final byte DEFAULT_CLUSTER_ID = 0;

    /**
     * Parameter name for maximum number of bytes returned when calling a
     * scanner's next method.
     */
  public static String HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY = "hbase.client.scanner.max.result.size";

  /**
   * Maximum number of bytes returned when calling a scanner's next method.
   * Note that when a single row is larger than this limit the row is still
   * returned completely.
   *
   * The default value is unlimited.
   */
  public static long DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE = Long.MAX_VALUE;


  /**
   * HRegion server lease period in milliseconds. Clients must report in within this period
   * else they are considered dead. Unit measured in ms (milliseconds).
   */
  public static String HBASE_REGIONSERVER_LEASE_PERIOD_KEY   = "hbase.regionserver.lease.period";


  /**
   * Default value of {@link #HBASE_REGIONSERVER_LEASE_PERIOD_KEY}.
   */
  public static long DEFAULT_HBASE_REGIONSERVER_LEASE_PERIOD = 60000;

}
