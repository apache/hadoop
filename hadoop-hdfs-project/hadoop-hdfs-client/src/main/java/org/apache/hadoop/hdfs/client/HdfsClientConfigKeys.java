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
package org.apache.hadoop.hdfs.client;

/** Client configuration properties */
public interface HdfsClientConfigKeys {
  long SECOND = 1000L;
  long MINUTE = 60 * SECOND;

  String  DFS_BLOCK_SIZE_KEY = "dfs.blocksize";
  long    DFS_BLOCK_SIZE_DEFAULT = 128*1024*1024;
  String  DFS_REPLICATION_KEY = "dfs.replication";
  short   DFS_REPLICATION_DEFAULT = 3;
  String  DFS_WEBHDFS_USER_PATTERN_KEY = "dfs.webhdfs.user.provider.user.pattern";
  String  DFS_WEBHDFS_USER_PATTERN_DEFAULT = "^[A-Za-z_][A-Za-z0-9._-]*[$]?$";
  String DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT =
      "^(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?(,(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?)*$";

  static final String PREFIX = "dfs.client.";
  String  DFS_NAMESERVICES = "dfs.nameservices";
  int     DFS_NAMENODE_HTTP_PORT_DEFAULT = 50070;
  String  DFS_NAMENODE_HTTP_ADDRESS_KEY = "dfs.namenode.http-address";
  int     DFS_NAMENODE_HTTPS_PORT_DEFAULT = 50470;
  String  DFS_NAMENODE_HTTPS_ADDRESS_KEY = "dfs.namenode.https-address";
  String DFS_HA_NAMENODES_KEY_PREFIX = "dfs.ha.namenodes";
  int DFS_NAMENODE_RPC_PORT_DEFAULT = 8020;

  /** dfs.client.retry configuration properties */
  interface Retry {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "retry.";

    String  POLICY_ENABLED_KEY = PREFIX + "policy.enabled";
    boolean POLICY_ENABLED_DEFAULT = false; 
    String  POLICY_SPEC_KEY = PREFIX + "policy.spec";
    String  POLICY_SPEC_DEFAULT = "10000,6,60000,10"; //t1,n1,t2,n2,... 

    String  TIMES_GET_LAST_BLOCK_LENGTH_KEY = PREFIX + "times.get-last-block-length";
    int     TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT = 3;
    String  INTERVAL_GET_LAST_BLOCK_LENGTH_KEY = PREFIX + "interval-ms.get-last-block-length";
    int     INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT = 4000;

    String  MAX_ATTEMPTS_KEY = PREFIX + "max.attempts";
    int     MAX_ATTEMPTS_DEFAULT = 10;

    String  WINDOW_BASE_KEY = PREFIX + "window.base";
    int     WINDOW_BASE_DEFAULT = 3000;
  }

  /** dfs.client.failover configuration properties */
  interface Failover {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "failover.";

    String  PROXY_PROVIDER_KEY_PREFIX = PREFIX + "proxy.provider";
    String  MAX_ATTEMPTS_KEY = PREFIX + "max.attempts";
    int     MAX_ATTEMPTS_DEFAULT = 15;
    String  SLEEPTIME_BASE_KEY = PREFIX + "sleep.base.millis";
    int     SLEEPTIME_BASE_DEFAULT = 500;
    String  SLEEPTIME_MAX_KEY = PREFIX + "sleep.max.millis";
    int     SLEEPTIME_MAX_DEFAULT = 15000;
    String  CONNECTION_RETRIES_KEY = PREFIX + "connection.retries";
    int     CONNECTION_RETRIES_DEFAULT = 0;
    String  CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY = PREFIX + "connection.retries.on.timeouts";
    int     CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT = 0;
  }
  
  /** dfs.client.write configuration properties */
  interface Write {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "write.";

    String  MAX_PACKETS_IN_FLIGHT_KEY = PREFIX + "max-packets-in-flight";
    int     MAX_PACKETS_IN_FLIGHT_DEFAULT = 80;
    String  EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_KEY = PREFIX + "exclude.nodes.cache.expiry.interval.millis";
    long    EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT = 10*MINUTE;

    interface ByteArrayManager {
      String PREFIX = Write.PREFIX + "byte-array-manager.";

      String  ENABLED_KEY = PREFIX + "enabled";
      boolean ENABLED_DEFAULT = false;
      String  COUNT_THRESHOLD_KEY = PREFIX + "count-threshold";
      int     COUNT_THRESHOLD_DEFAULT = 128;
      String  COUNT_LIMIT_KEY = PREFIX + "count-limit";
      int     COUNT_LIMIT_DEFAULT = 2048;
      String  COUNT_RESET_TIME_PERIOD_MS_KEY = PREFIX + "count-reset-time-period-ms";
      long    COUNT_RESET_TIME_PERIOD_MS_DEFAULT = 10*SECOND;
    }
  }

  /** dfs.client.block.write configuration properties */
  interface BlockWrite {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "block.write.";

    String  RETRIES_KEY = PREFIX + "retries";
    int     RETRIES_DEFAULT = 3;
    String  LOCATEFOLLOWINGBLOCK_RETRIES_KEY = PREFIX + "locateFollowingBlock.retries";
    int     LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT = 5;
    String  LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_MS_KEY = PREFIX + "locateFollowingBlock.initial.delay.ms";
    int     LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_MS_DEFAULT = 400;

    interface ReplaceDatanodeOnFailure {
      String PREFIX = BlockWrite.PREFIX + "replace-datanode-on-failure.";

      String  ENABLE_KEY = PREFIX + "enable";
      boolean ENABLE_DEFAULT = true;
      String  POLICY_KEY = PREFIX + "policy";
      String  POLICY_DEFAULT = "DEFAULT";
      String  BEST_EFFORT_KEY = PREFIX + "best-effort";
      boolean BEST_EFFORT_DEFAULT = false;
    }
  }

  /** dfs.client.read configuration properties */
  interface Read {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "read.";
    
    String  PREFETCH_SIZE_KEY = PREFIX + "prefetch.size"; 

    interface ShortCircuit {
      String PREFIX = Read.PREFIX + "shortcircuit.";

      String  KEY = PREFIX.substring(0, PREFIX.length()-1);
      boolean DEFAULT = false;
      String  SKIP_CHECKSUM_KEY = PREFIX + "skip.checksum";
      boolean SKIP_CHECKSUM_DEFAULT = false;
      String  BUFFER_SIZE_KEY = PREFIX + "buffer.size";
      int     BUFFER_SIZE_DEFAULT = 1024 * 1024;

      String  STREAMS_CACHE_SIZE_KEY = PREFIX + "streams.cache.size";
      int     STREAMS_CACHE_SIZE_DEFAULT = 256;
      String  STREAMS_CACHE_EXPIRY_MS_KEY = PREFIX + "streams.cache.expiry.ms";
      long    STREAMS_CACHE_EXPIRY_MS_DEFAULT = 5*MINUTE;
    }
  }

  /** dfs.client.short.circuit configuration properties */
  interface ShortCircuit {
    String PREFIX = Read.PREFIX + "short.circuit.";

    String  REPLICA_STALE_THRESHOLD_MS_KEY = PREFIX + "replica.stale.threshold.ms";
    long    REPLICA_STALE_THRESHOLD_MS_DEFAULT = 30*MINUTE;
  }

  /** dfs.client.mmap configuration properties */
  interface Mmap {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "mmap.";

    String  ENABLED_KEY = PREFIX + "enabled";
    boolean ENABLED_DEFAULT = true;
    String  CACHE_SIZE_KEY = PREFIX + "cache.size";
    int     CACHE_SIZE_DEFAULT = 256;
    String  CACHE_TIMEOUT_MS_KEY = PREFIX + "cache.timeout.ms";
    long    CACHE_TIMEOUT_MS_DEFAULT  = 60*MINUTE;
    String  RETRY_TIMEOUT_MS_KEY = PREFIX + "retry.timeout.ms";
    long    RETRY_TIMEOUT_MS_DEFAULT = 5*MINUTE;
  }

  /** dfs.client.hedged.read configuration properties */
  interface HedgedRead {
    String  THRESHOLD_MILLIS_KEY = PREFIX + "threshold.millis";
    long    THRESHOLD_MILLIS_DEFAULT = 500;
    String  THREADPOOL_SIZE_KEY = PREFIX + "threadpool.size";
    int     THREADPOOL_SIZE_DEFAULT = 0;
  }

  /** dfs.client.read.striped configuration properties */
  interface StripedRead {
    String PREFIX = Read.PREFIX + "striped.";

    String  THREADPOOL_SIZE_KEY = PREFIX + "threadpool.size";
    /**
     * With default RS-6-3-64k erasure coding policy, each normal read could span
     * 6 DNs, so this default value accommodates 3 read streams
     */
    int     THREADPOOL_SIZE_DEFAULT = 18;
  }

  /** dfs.http.client configuration properties */
  interface HttpClient {
    String  PREFIX = "dfs.http.client.";

    // retry
    String  RETRY_POLICY_ENABLED_KEY = PREFIX + "retry.policy.enabled";
    boolean RETRY_POLICY_ENABLED_DEFAULT = false;
    String  RETRY_POLICY_SPEC_KEY = PREFIX + "retry.policy.spec";
    String  RETRY_POLICY_SPEC_DEFAULT = "10000,6,60000,10"; //t1,n1,t2,n2,...
    String  RETRY_MAX_ATTEMPTS_KEY = PREFIX + "retry.max.attempts";
    int     RETRY_MAX_ATTEMPTS_DEFAULT = 10;
    
    // failover
    String  FAILOVER_MAX_ATTEMPTS_KEY = PREFIX + "failover.max.attempts";
    int     FAILOVER_MAX_ATTEMPTS_DEFAULT =  15;
    String  FAILOVER_SLEEPTIME_BASE_KEY = PREFIX + "failover.sleep.base.millis";
    int     FAILOVER_SLEEPTIME_BASE_DEFAULT = 500;
    String  FAILOVER_SLEEPTIME_MAX_KEY = PREFIX + "failover.sleep.max.millis";
    int     FAILOVER_SLEEPTIME_MAX_DEFAULT =  15000;
  }
}
