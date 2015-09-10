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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.io.erasurecode.ECSchema;

import java.util.Map;
import java.util.TreeMap;

/**
 * This manages erasure coding policies predefined and activated in the system.
 * It loads customized policies and syncs with persisted ones in
 * NameNode image.
 *
 * This class is instantiated by the FSNamesystem.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public final class ErasureCodingPolicyManager {

  /**
   * TODO: HDFS-8095
   */
  private static final int DEFAULT_DATA_BLOCKS = 6;
  private static final int DEFAULT_PARITY_BLOCKS = 3;
  private static final int DEFAULT_CELLSIZE = 64 * 1024;
  private static final String DEFAULT_CODEC_NAME = "rs";
  private static final String DEFAULT_POLICY_NAME = "RS-6-3-64k";
  private static final ECSchema SYS_DEFAULT_SCHEMA = new ECSchema(
      DEFAULT_CODEC_NAME, DEFAULT_DATA_BLOCKS, DEFAULT_PARITY_BLOCKS);
  private static final ErasureCodingPolicy SYS_DEFAULT_POLICY =
      new ErasureCodingPolicy(DEFAULT_POLICY_NAME, SYS_DEFAULT_SCHEMA,
      DEFAULT_CELLSIZE);

  //We may add more later.
  private static ErasureCodingPolicy[] SYS_POLICY = new ErasureCodingPolicy[] {
      SYS_DEFAULT_POLICY
  };

  /**
   * All active policies maintained in NN memory for fast querying,
   * identified and sorted by its name.
   */
  private final Map<String, ErasureCodingPolicy> activePolicies;

  ErasureCodingPolicyManager() {

    this.activePolicies = new TreeMap<>();
    for (ErasureCodingPolicy policy : SYS_POLICY) {
      activePolicies.put(policy.getName(), policy);
    }

    /**
     * TODO: HDFS-7859 persist into NameNode
     * load persistent policies from image and editlog, which is done only once
     * during NameNode startup. This can be done here or in a separate method.
     */
  }

  /**
   * Get system defined policies.
   * @return system policies
   */
  public static ErasureCodingPolicy[] getSystemPolices() {
    return SYS_POLICY;
  }

  /**
   * Get system-wide default policy, which can be used by default
   * when no policy is specified for a path.
   * @return ecPolicy
   */
  public static ErasureCodingPolicy getSystemDefaultPolicy() {
    return SYS_DEFAULT_POLICY;
  }

  /**
   * Get all policies that's available to use.
   * @return all policies
   */
  public ErasureCodingPolicy[] getPolicies() {
    ErasureCodingPolicy[] results = new ErasureCodingPolicy[activePolicies.size()];
    return activePolicies.values().toArray(results);
  }

  /**
   * Get the policy specified by the policy name.
   */
  public ErasureCodingPolicy getPolicy(String name) {
    return activePolicies.get(name);
  }

  /**
   * Clear and clean up
   */
  public void clear() {
    activePolicies.clear();
  }
}
