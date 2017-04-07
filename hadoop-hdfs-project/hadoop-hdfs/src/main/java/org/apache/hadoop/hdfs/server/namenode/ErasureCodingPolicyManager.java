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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;


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
   * TODO: HDFS-8095.
   */
  private static final int DEFAULT_CELLSIZE = 64 * 1024;
  private static final ErasureCodingPolicy SYS_POLICY1 =
      new ErasureCodingPolicy(ErasureCodeConstants.RS_6_3_SCHEMA,
          DEFAULT_CELLSIZE, HdfsConstants.RS_6_3_POLICY_ID);
  private static final ErasureCodingPolicy SYS_POLICY2 =
      new ErasureCodingPolicy(ErasureCodeConstants.RS_3_2_SCHEMA,
          DEFAULT_CELLSIZE, HdfsConstants.RS_3_2_POLICY_ID);
  private static final ErasureCodingPolicy SYS_POLICY3 =
      new ErasureCodingPolicy(ErasureCodeConstants.RS_6_3_LEGACY_SCHEMA,
          DEFAULT_CELLSIZE, HdfsConstants.RS_6_3_LEGACY_POLICY_ID);
  private static final ErasureCodingPolicy SYS_POLICY4 =
      new ErasureCodingPolicy(ErasureCodeConstants.XOR_2_1_SCHEMA,
          DEFAULT_CELLSIZE, HdfsConstants.XOR_2_1_POLICY_ID);
  private static final ErasureCodingPolicy SYS_POLICY5 =
      new ErasureCodingPolicy(ErasureCodeConstants.RS_10_4_SCHEMA,
          DEFAULT_CELLSIZE, HdfsConstants.RS_10_4_POLICY_ID);

  //We may add more later.
  private static final ErasureCodingPolicy[] SYS_POLICIES =
      new ErasureCodingPolicy[]{SYS_POLICY1, SYS_POLICY2, SYS_POLICY3,
          SYS_POLICY4, SYS_POLICY5};

  // Supported storage policies for striped EC files
  private static final byte[] SUITABLE_STORAGE_POLICIES_FOR_EC_STRIPED_MODE =
      new byte[]{
          HdfsConstants.HOT_STORAGE_POLICY_ID,
          HdfsConstants.COLD_STORAGE_POLICY_ID,
          HdfsConstants.ALLSSD_STORAGE_POLICY_ID};
  /**
   * All supported policies maintained in NN memory for fast querying,
   * identified and sorted by its name.
   */
  private static final Map<String, ErasureCodingPolicy> SYSTEM_POLICIES_BY_NAME;

  static {
    // Create a hashmap of all available policies for quick lookup by name
    SYSTEM_POLICIES_BY_NAME = new TreeMap<>();
    for (ErasureCodingPolicy policy : SYS_POLICIES) {
      SYSTEM_POLICIES_BY_NAME.put(policy.getName(), policy);
    }
  }

  /**
   * All enabled policies maintained in NN memory for fast querying,
   * identified and sorted by its name.
   */
  private final Map<String, ErasureCodingPolicy> enabledPoliciesByName;

  ErasureCodingPolicyManager(Configuration conf) {
    // Populate the list of enabled policies from configuration
    final String[] policyNames = conf.getTrimmedStrings(
        DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_ENABLED_DEFAULT);
    this.enabledPoliciesByName = new TreeMap<>();
    for (String policyName : policyNames) {
      if (policyName.trim().isEmpty()) {
        continue;
      }
      ErasureCodingPolicy ecPolicy = SYSTEM_POLICIES_BY_NAME.get(policyName);
      if (ecPolicy == null) {
        String sysPolicies = Arrays.asList(SYS_POLICIES).stream()
            .map(ErasureCodingPolicy::getName)
            .collect(Collectors.joining(", "));
        String msg = String.format("EC policy '%s' specified at %s is not a " +
            "valid policy. Please choose from list of available policies: " +
            "[%s]",
            policyName,
            DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_ENABLED_KEY,
            sysPolicies);
        throw new IllegalArgumentException(msg);
      }
      enabledPoliciesByName.put(ecPolicy.getName(), ecPolicy);
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
  public static ErasureCodingPolicy[] getSystemPolicies() {
    return SYS_POLICIES;
  }

  /**
   * Get a policy by policy ID.
   * @return ecPolicy, or null if not found
   */
  public static ErasureCodingPolicy getPolicyByID(byte id) {
    for (ErasureCodingPolicy policy : SYS_POLICIES) {
      if (policy.getId() == id) {
        return policy;
      }
    }
    return null;
  }

  /**
   * Get a policy by policy name.
   * @return ecPolicy, or null if not found
   */
  public static ErasureCodingPolicy getPolicyByName(String name) {
    return SYSTEM_POLICIES_BY_NAME.get(name);
  }

  /**
   * Get the set of enabled policies.
   * @return all policies
   */
  public ErasureCodingPolicy[] getEnabledPolicies() {
    ErasureCodingPolicy[] results =
        new ErasureCodingPolicy[enabledPoliciesByName.size()];
    return enabledPoliciesByName.values().toArray(results);
  }

  /**
   * Get enabled policy by policy name.
   */
  public ErasureCodingPolicy getEnabledPolicyByName(String name) {
    return enabledPoliciesByName.get(name);
  }

  /**
   * @return if the specified storage policy ID is suitable for striped EC
   * files.
   */
  public static boolean checkStoragePolicySuitableForECStripedMode(
      byte storagePolicyID) {
    boolean isPolicySuitable = false;
    for (byte suitablePolicy : SUITABLE_STORAGE_POLICIES_FOR_EC_STRIPED_MODE) {
      if (storagePolicyID == suitablePolicy) {
        isPolicySuitable = true;
        break;
      }
    }
    return isPolicySuitable;
  }

  /**
   * Clear and clean up.
   */
  public void clear() {
    // TODO: we should only clear policies loaded from NN metadata.
    // This is a placeholder for HDFS-7337.
  }
}
