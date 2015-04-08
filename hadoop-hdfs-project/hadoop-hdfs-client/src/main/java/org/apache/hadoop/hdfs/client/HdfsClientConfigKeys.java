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
  String  DFS_BLOCK_SIZE_KEY = "dfs.blocksize";
  long    DFS_BLOCK_SIZE_DEFAULT = 128*1024*1024;
  String  DFS_REPLICATION_KEY = "dfs.replication";
  short   DFS_REPLICATION_DEFAULT = 3;
  String  DFS_WEBHDFS_USER_PATTERN_DEFAULT = "^[A-Za-z_][A-Za-z0-9._-]*[$]?$";
  String DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT =
      "^(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?(,(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?)*$";

  static final String PREFIX = "dfs.client.";
  /** Client retry configuration properties */
  public interface Retry {
    static final String PREFIX = HdfsClientConfigKeys.PREFIX + "retry.";

    public static final String  POLICY_ENABLED_KEY
        = PREFIX + "policy.enabled";
    public static final boolean POLICY_ENABLED_DEFAULT
        = false; 
    public static final String  POLICY_SPEC_KEY
        = PREFIX + "policy.spec";
    public static final String  POLICY_SPEC_DEFAULT
        = "10000,6,60000,10"; //t1,n1,t2,n2,... 

    public static final String  TIMES_GET_LAST_BLOCK_LENGTH_KEY
        = PREFIX + "times.get-last-block-length";
    public static final int     TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT
        = 3;
    public static final String  INTERVAL_GET_LAST_BLOCK_LENGTH_KEY
        = PREFIX + "interval-ms.get-last-block-length";
    public static final int     INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT
        = 4000;

    public static final String  MAX_ATTEMPTS_KEY
        = PREFIX + "max.attempts";
    public static final int     MAX_ATTEMPTS_DEFAULT
        = 10;

    public static final String  WINDOW_BASE_KEY
        = PREFIX + "window.base";
    public static final int     WINDOW_BASE_DEFAULT
        = 3000;
  }
}
