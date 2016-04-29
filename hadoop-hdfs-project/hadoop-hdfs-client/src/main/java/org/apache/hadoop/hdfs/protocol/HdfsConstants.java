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
import org.apache.hadoop.util.StringUtils;

@InterfaceAudience.Private
public final class HdfsConstants {
  // Long that indicates "leave current quota unchanged"
  public static final long QUOTA_DONT_SET = Long.MAX_VALUE;
  public static final long QUOTA_RESET = -1L;
  public static final int BYTES_IN_INTEGER = Integer.SIZE / Byte.SIZE;
  /**
   * URI Scheme for hdfs://namenode/ URIs.
   */
  public static final String HDFS_URI_SCHEME = "hdfs";

  public static final byte MEMORY_STORAGE_POLICY_ID = 15;
  public static final String MEMORY_STORAGE_POLICY_NAME = "LAZY_PERSIST";
  public static final byte ALLSSD_STORAGE_POLICY_ID = 12;
  public static final String ALLSSD_STORAGE_POLICY_NAME = "ALL_SSD";
  public static final byte ONESSD_STORAGE_POLICY_ID = 10;
  public static final String ONESSD_STORAGE_POLICY_NAME = "ONE_SSD";
  public static final byte HOT_STORAGE_POLICY_ID = 7;
  public static final String HOT_STORAGE_POLICY_NAME = "HOT";
  public static final byte WARM_STORAGE_POLICY_ID = 5;
  public static final String WARM_STORAGE_POLICY_NAME = "WARM";
  public static final byte COLD_STORAGE_POLICY_ID = 2;
  public static final String COLD_STORAGE_POLICY_NAME = "COLD";

  // TODO should be conf injected?
  public static final int DEFAULT_DATA_SOCKET_SIZE = 128 * 1024;
  /**
   * A special path component contained in the path for a snapshot file/dir
   */
  public static final String DOT_SNAPSHOT_DIR = ".snapshot";
  public static final String SEPARATOR_DOT_SNAPSHOT_DIR
          = Path.SEPARATOR + DOT_SNAPSHOT_DIR;
  public static final String SEPARATOR_DOT_SNAPSHOT_DIR_SEPARATOR
      = Path.SEPARATOR + DOT_SNAPSHOT_DIR + Path.SEPARATOR;

  /**
   * Generation stamp of blocks that pre-date the introduction
   * of a generation stamp.
   */
  public static final long GRANDFATHER_GENERATION_STAMP = 0;
  /**
   * The inode id validation of lease check will be skipped when the request
   * uses GRANDFATHER_INODE_ID for backward compatibility.
   */
  public static final long GRANDFATHER_INODE_ID = 0;
  public static final byte BLOCK_STORAGE_POLICY_ID_UNSPECIFIED = 0;
  /**
   * A prefix put before the namenode URI inside the "service" field
   * of a delgation token, indicating that the URI is a logical (HA)
   * URI.
   */
  public static final String HA_DT_SERVICE_PREFIX = "ha-";
  // The name of the SafeModeException. FileSystem should retry if it sees
  // the below exception in RPC
  public static final String SAFEMODE_EXCEPTION_CLASS_NAME =
      "org.apache.hadoop.hdfs.server.namenode.SafeModeException";
  /**
   * HDFS Protocol Names:
   */
  public static final String CLIENT_NAMENODE_PROTOCOL_NAME =
      "org.apache.hadoop.hdfs.protocol.ClientProtocol";

  // Timeouts for communicating with DataNode for streaming writes/reads
  public static final int READ_TIMEOUT = 60 * 1000;
  public static final int READ_TIMEOUT_EXTENSION = 5 * 1000;
  public static final int WRITE_TIMEOUT = 8 * 60 * 1000;
  //for write pipeline
  public static final int WRITE_TIMEOUT_EXTENSION = 5 * 1000;

  /**
   * For a HDFS client to write to a file, a lease is granted; During the lease
   * period, no other client can write to the file. The writing client can
   * periodically renew the lease. When the file is closed, the lease is
   * revoked. The lease duration is bound by this soft limit and a
   * {@link HdfsConstants#LEASE_HARDLIMIT_PERIOD hard limit}. Until the
   * soft limit expires, the writer has sole write access to the file. If the
   * soft limit expires and the client fails to close the file or renew the
   * lease, another client can preempt the lease.
   */
  public static final long LEASE_SOFTLIMIT_PERIOD = 60 * 1000;
  /**
   * For a HDFS client to write to a file, a lease is granted; During the lease
   * period, no other client can write to the file. The writing client can
   * periodically renew the lease. When the file is closed, the lease is
   * revoked. The lease duration is bound by a
   * {@link HdfsConstants#LEASE_SOFTLIMIT_PERIOD soft limit} and this hard
   * limit. If after the hard limit expires and the client has failed to renew
   * the lease, HDFS assumes that the client has quit and will automatically
   * close the file on behalf of the writer, and recover the lease.
   */
  public static final long LEASE_HARDLIMIT_PERIOD = 60 * LEASE_SOFTLIMIT_PERIOD;

  // SafeMode actions
  public enum SafeModeAction {
    SAFEMODE_LEAVE, SAFEMODE_ENTER, SAFEMODE_GET, SAFEMODE_FORCE_EXIT
  }

  public enum RollingUpgradeAction {
    QUERY, PREPARE, FINALIZE;

    private static final Map<String, RollingUpgradeAction> MAP
        = new HashMap<>();
    static {
      MAP.put("", QUERY);
      for(RollingUpgradeAction a : values()) {
        MAP.put(a.name(), a);
      }
    }

    /** Convert the given String to a RollingUpgradeAction. */
    public static RollingUpgradeAction fromString(String s) {
      return MAP.get(StringUtils.toUpperCase(s));
    }
  }

  // type of the datanode report
  public enum DatanodeReportType {
    ALL, LIVE, DEAD, DECOMMISSIONING
  }

  public static final byte RS_6_3_POLICY_ID = 0;
  public static final byte RS_3_2_POLICY_ID = 1;
  public static final byte RS_6_3_LEGACY_POLICY_ID = 2;

  /* Hidden constructor */
  protected HdfsConstants() {
  }
}
