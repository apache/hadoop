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
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * The setting of replace-datanode-on-failure feature.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReplaceDatanodeOnFailure {
  /**
   * DEFAULT condition:
   *   Let r be the replication number.
   *   Let n be the number of existing datanodes.
   *   Add a new datanode only if r >= 3 and either
   *   (1) floor(r/2) >= n or (2) the block is hflushed/appended.
   */
  private static final Condition CONDITION_DEFAULT = new Condition() {
    @Override
    public boolean satisfy(final short replication,
        final DatanodeInfo[] existings, final int n, final boolean isAppend,
        final boolean isHflushed) {
      return replication >= 3 &&
          (n <= (replication / 2) || isAppend || isHflushed);
    }
  };
  /** Return false unconditionally. */
  private static final Condition CONDITION_FALSE = new Condition() {
    @Override
    public boolean satisfy(short replication, DatanodeInfo[] existings,
        int nExistings, boolean isAppend, boolean isHflushed) {
      return false;
    }
  };
  /** Return true unconditionally. */
  private static final Condition CONDITION_TRUE = new Condition() {
    @Override
    public boolean satisfy(short replication, DatanodeInfo[] existings,
        int nExistings, boolean isAppend, boolean isHflushed) {
      return true;
    }
  };

  /** The replacement policies */
  public enum Policy {
    /** The feature is disabled in the entire site. */
    DISABLE(CONDITION_FALSE),
    /** Never add a new datanode. */
    NEVER(CONDITION_FALSE),
    /** @see ReplaceDatanodeOnFailure#CONDITION_DEFAULT */
    DEFAULT(CONDITION_DEFAULT),
    /** Always add a new datanode when an existing datanode is removed. */
    ALWAYS(CONDITION_TRUE);

    private final Condition condition;

    Policy(Condition condition) {
      this.condition = condition;
    }

    Condition getCondition() {
      return condition;
    }
  }

  /** Datanode replacement condition */
  private interface Condition {

    /** Is the condition satisfied? */
    boolean satisfy(short replication, DatanodeInfo[] existings, int nExistings,
                    boolean isAppend, boolean isHflushed);
  }

  private final Policy policy;
  private final boolean bestEffort;

  public ReplaceDatanodeOnFailure(Policy policy, boolean bestEffort) {
    this.policy = policy;
    this.bestEffort = bestEffort;
  }

  /** Check if the feature is enabled. */
  public void checkEnabled() {
    if (policy == Policy.DISABLE) {
      throw new UnsupportedOperationException(
          "This feature is disabled.  Please refer to "
          + HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY
          + " configuration property.");
    }
  }

  /**
   * Best effort means that the client will try to replace the failed datanode
   * (provided that the policy is satisfied), however, it will continue the
   * write operation in case that the datanode replacement also fails.
   *
   * @return Suppose the datanode replacement fails.
   *     false: An exception should be thrown so that the write will fail.
   *     true : The write should be resumed with the remaining datandoes.
   */
  public boolean isBestEffort() {
    return bestEffort;
  }

  /** Does it need a replacement according to the policy? */
  public boolean satisfy(
      final short replication, final DatanodeInfo[] existings,
      final boolean isAppend, final boolean isHflushed) {
    final int n = existings == null ? 0 : existings.length;
    //don't need to add datanode for any policy.
    return !(n == 0 || n >= replication) &&
        policy.getCondition().satisfy(replication, existings, n, isAppend,
            isHflushed);
  }

  @Override
  public String toString() {
    return policy.toString();
  }

  /** Get the setting from configuration. */
  public static ReplaceDatanodeOnFailure get(final Configuration conf) {
    final Policy policy = getPolicy(conf);
    final boolean bestEffort = conf.getBoolean(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY,
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_DEFAULT);

    return new ReplaceDatanodeOnFailure(policy, bestEffort);
  }

  private static Policy getPolicy(final Configuration conf) {
    final boolean enabled = conf.getBoolean(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_DEFAULT);
    if (!enabled) {
      return Policy.DISABLE;
    }

    final String policy = conf.get(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT);
    for(int i = 1; i < Policy.values().length; i++) {
      final Policy p = Policy.values()[i];
      if (p.name().equalsIgnoreCase(policy)) {
        return p;
      }
    }
    throw new HadoopIllegalArgumentException("Illegal configuration value for "
        + HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY
        + ": " + policy);
  }

  /** Write the setting to configuration. */
  public static void write(final Policy policy,
      final boolean bestEffort, final Configuration conf) {
    conf.setBoolean(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
        policy != Policy.DISABLE);
    conf.set(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
        policy.name());
    conf.setBoolean(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY,
        bestEffort);
  }
}
