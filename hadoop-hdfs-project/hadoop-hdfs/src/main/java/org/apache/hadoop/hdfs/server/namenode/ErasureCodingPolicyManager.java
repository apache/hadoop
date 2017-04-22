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
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

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

  // Supported storage policies for striped EC files
  private static final byte[] SUITABLE_STORAGE_POLICIES_FOR_EC_STRIPED_MODE =
      new byte[]{
          HdfsConstants.HOT_STORAGE_POLICY_ID,
          HdfsConstants.COLD_STORAGE_POLICY_ID,
          HdfsConstants.ALLSSD_STORAGE_POLICY_ID};

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
      ErasureCodingPolicy ecPolicy =
          SystemErasureCodingPolicies.getByName(policyName);
      if (ecPolicy == null) {
        String sysPolicies = SystemErasureCodingPolicies.getPolicies().stream()
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
