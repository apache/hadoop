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
package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * The setting of replace-datanode-on-failure feature.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public enum ReplaceDatanodeOnFailure {
  /** The feature is disabled in the entire site. */
  DISABLE,
  /** Never add a new datanode. */
  NEVER,
  /**
   * DEFAULT policy:
   *   Let r be the replication number.
   *   Let n be the number of existing datanodes.
   *   Add a new datanode only if r >= 3 and either
   *   (1) floor(r/2) >= n; or
   *   (2) r > n and the block is hflushed/appended.
   */
  DEFAULT,
  /** Always add a new datanode when an existing datanode is removed. */
  ALWAYS;

  /** Check if the feature is enabled. */
  public void checkEnabled() {
    if (this == DISABLE) {
      throw new UnsupportedOperationException(
          "This feature is disabled.  Please refer to "
          + DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY
          + " configuration property.");
    }
  }

  /** Is the policy satisfied? */
  public boolean satisfy(
      final short replication, final DatanodeInfo[] existings,
      final boolean isAppend, final boolean isHflushed) {
    final int n = existings == null? 0: existings.length;
    if (n == 0 || n >= replication) {
      //don't need to add datanode for any policy.
      return false;
    } else if (this == DISABLE || this == NEVER) {
      return false;
    } else if (this == ALWAYS) {
      return true;
    } else {
      //DEFAULT
      if (replication < 3) {
        return false;
      } else {
        if (n <= (replication/2)) {
          return true;
        } else {
          return isAppend || isHflushed;
        }
      }
    }
  }

  /** Get the setting from configuration. */
  public static ReplaceDatanodeOnFailure get(final Configuration conf) {
    final boolean enabled = conf.getBoolean(
        DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY,
        DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_DEFAULT);
    if (!enabled) {
      return DISABLE;
    }

    final String policy = conf.get(
        DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY,
        DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_DEFAULT);
    for(int i = 1; i < values().length; i++) {
      final ReplaceDatanodeOnFailure rdof = values()[i];
      if (rdof.name().equalsIgnoreCase(policy)) {
        return rdof;
      }
    }
    throw new HadoopIllegalArgumentException("Illegal configuration value for "
        + DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY
        + ": " + policy);
  }

  /** Write the setting to configuration. */
  public void write(final Configuration conf) {
    conf.setBoolean(
        DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY,
        this != DISABLE);
    conf.set(
        DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY,
        name());
  }
}