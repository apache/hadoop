/**
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
package org.apache.hadoop.hdfs.protocol;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;

/************************************
 * Some handy constants
 * 
 ************************************/
@InterfaceAudience.Private
public class HdfsConstants {
  /* Hidden constructor */
  protected HdfsConstants() {
  }
  
  /**
   * HDFS Protocol Names:  
   */
  public static final String CLIENT_NAMENODE_PROTOCOL_NAME = 
      "org.apache.hadoop.hdfs.protocol.ClientProtocol";
  public static final String CLIENT_DATANODE_PROTOCOL_NAME = 
      "org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol";
  
  
  public static final int MIN_BLOCKS_FOR_WRITE = 5;

  // Long that indicates "leave current quota unchanged"
  public static final long QUOTA_DONT_SET = Long.MAX_VALUE;
  public static final long QUOTA_RESET = -1L;

  //
  // Timeouts, constants
  //
  public static final long LEASE_SOFTLIMIT_PERIOD = 60 * 1000;
  public static final long LEASE_HARDLIMIT_PERIOD = 60 * LEASE_SOFTLIMIT_PERIOD;
  public static final long LEASE_RECOVER_PERIOD = 10 * 1000; // in ms

  // We need to limit the length and depth of a path in the filesystem.
  // HADOOP-438
  // Currently we set the maximum length to 8k characters and the maximum depth
  // to 1k.
  public static final int MAX_PATH_LENGTH = 8000;
  public static final int MAX_PATH_DEPTH = 1000;

  // TODO should be conf injected?
  public static final int DEFAULT_DATA_SOCKET_SIZE = 128 * 1024;
  public static final int IO_FILE_BUFFER_SIZE = new HdfsConfiguration().getInt(
      DFSConfigKeys.IO_FILE_BUFFER_SIZE_KEY,
      DFSConfigKeys.IO_FILE_BUFFER_SIZE_DEFAULT);
  // Used for writing header etc.
  public static final int SMALL_BUFFER_SIZE = Math.min(IO_FILE_BUFFER_SIZE / 2,
      512);

  public static final int BYTES_IN_INTEGER = Integer.SIZE / Byte.SIZE;

  // SafeMode actions
  public static enum SafeModeAction {
    SAFEMODE_LEAVE, SAFEMODE_ENTER, SAFEMODE_GET;
  }

  public static enum RollingUpgradeAction {
    QUERY, PREPARE, FINALIZE;
    
    private static final Map<String, RollingUpgradeAction> MAP
        = new HashMap<String, RollingUpgradeAction>();
    static {
      MAP.put("", QUERY);
      for(RollingUpgradeAction a : values()) {
        MAP.put(a.name(), a);
      }
    }

    /** Covert the given String to a RollingUpgradeAction. */
    public static RollingUpgradeAction fromString(String s) {
      return MAP.get(s.toUpperCase());
    }
  }

  // type of the datanode report
  public static enum DatanodeReportType {
    ALL, LIVE, DEAD, DECOMMISSIONING
  }

  // An invalid transaction ID that will never be seen in a real namesystem.
  public static final long INVALID_TXID = -12345;

  // Number of generation stamps reserved for legacy blocks.
  public static final long RESERVED_GENERATION_STAMPS_V1 =
      1024L * 1024 * 1024 * 1024;

  /**
   * URI Scheme for hdfs://namenode/ URIs.
   */
  public static final String HDFS_URI_SCHEME = "hdfs";

  /**
   * A prefix put before the namenode URI inside the "service" field
   * of a delgation token, indicating that the URI is a logical (HA)
   * URI.
   */
  public static final String HA_DT_SERVICE_PREFIX = "ha-hdfs:";

  /**
   * Path components that are reserved in HDFS.
   * <p>
   * .reserved is only reserved under root ("/").
   */
  public static final String[] RESERVED_PATH_COMPONENTS = new String[] {
    HdfsConstants.DOT_SNAPSHOT_DIR,
    FSDirectory.DOT_RESERVED_STRING
  };

  /**
   * Current layout version for NameNode.
   * Please see {@link NameNodeLayoutVersion.Feature} on adding new layout version.
   */
  public static final int NAMENODE_LAYOUT_VERSION
      = NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION;

  /**
   * Current layout version for DataNode.
   * Please see {@link DataNodeLayoutVersion.Feature} on adding new layout version.
   */
  public static final int DATANODE_LAYOUT_VERSION
      = DataNodeLayoutVersion.CURRENT_LAYOUT_VERSION;

  /**
   * A special path component contained in the path for a snapshot file/dir
   */
  public static final String DOT_SNAPSHOT_DIR = ".snapshot";

  public static final byte[] DOT_SNAPSHOT_DIR_BYTES
      = DFSUtil.string2Bytes(DOT_SNAPSHOT_DIR);
  
  public static final String SEPARATOR_DOT_SNAPSHOT_DIR
      = Path.SEPARATOR + DOT_SNAPSHOT_DIR; 
}
