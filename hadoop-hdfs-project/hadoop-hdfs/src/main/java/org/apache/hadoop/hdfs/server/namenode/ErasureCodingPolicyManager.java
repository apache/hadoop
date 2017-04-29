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
import org.apache.hadoop.hdfs.protocol.IllegalECPolicyException;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This manages erasure coding policies predefined and activated in the system.
 * It loads customized policies and syncs with persisted ones in
 * NameNode image.
 *
 * This class is instantiated by the FSNamesystem.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public final class ErasureCodingPolicyManager {
  private static final byte USER_DEFINED_POLICY_START_ID = 32;

  // Supported storage policies for striped EC files
  private static final byte[] SUITABLE_STORAGE_POLICIES_FOR_EC_STRIPED_MODE =
      new byte[]{
          HdfsConstants.HOT_STORAGE_POLICY_ID,
          HdfsConstants.COLD_STORAGE_POLICY_ID,
          HdfsConstants.ALLSSD_STORAGE_POLICY_ID};

  /**
   * All user defined policies sorted by name for fast querying.
   */
  private Map<String, ErasureCodingPolicy> userPoliciesByName;

  /**
   * All user defined policies sorted by ID for fast querying.
   */
  private Map<Byte, ErasureCodingPolicy> userPoliciesByID;

  /**
   * All enabled policies maintained in NN memory for fast querying,
   * identified and sorted by its name.
   */
  private Map<String, ErasureCodingPolicy> enabledPoliciesByName;

  private volatile static ErasureCodingPolicyManager instance = null;

  public static ErasureCodingPolicyManager getInstance() {
    if (instance == null) {
      instance = new ErasureCodingPolicyManager();
    }
    return instance;
  }

  private ErasureCodingPolicyManager() {}

  public void init(Configuration conf) {
    this.loadPolicies(conf);
  }

  private void loadPolicies(Configuration conf) {
    // Populate the list of enabled policies from configuration
    final String[] policyNames = conf.getTrimmedStrings(
        DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_ENABLED_DEFAULT);
    this.userPoliciesByID = new TreeMap<>();
    this.userPoliciesByName = new TreeMap<>();
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
   * Get all system defined policies and user defined policies.
   * @return all policies
   */
  public List<ErasureCodingPolicy> getPolicies() {
    return Stream.concat(SystemErasureCodingPolicies.getPolicies().stream(),
      this.userPoliciesByID.values().stream()).collect(Collectors.toList());
  }

  /**
   * Get a policy by policy ID, including system policy and user defined policy.
   * @return ecPolicy, or null if not found
   */
  public ErasureCodingPolicy getByID(byte id) {
    ErasureCodingPolicy policy = SystemErasureCodingPolicies.getByID(id);
    if (policy == null) {
      return this.userPoliciesByID.get(id);
    }
    return policy;
  }

  /**
   * Get a policy by policy ID, including system policy and user defined policy.
   * @return ecPolicy, or null if not found
   */
  public ErasureCodingPolicy getByName(String name) {
    ErasureCodingPolicy policy = SystemErasureCodingPolicies.getByName(name);
    if (policy == null) {
      return this.userPoliciesByName.get(name);
    }
    return policy;
  }

  /**
   * Clear and clean up.
   */
  public void clear() {
    // TODO: we should only clear policies loaded from NN metadata.
    // This is a placeholder for HDFS-7337.
  }

  public synchronized void addPolicy(ErasureCodingPolicy policy)
      throws IllegalECPolicyException {
    String assignedNewName = ErasureCodingPolicy.composePolicyName(
        policy.getSchema(), policy.getCellSize());
    for (ErasureCodingPolicy p : getPolicies()) {
      if (p.getName().equals(assignedNewName)) {
        throw new IllegalECPolicyException("The policy name already exists");
      }
      if (p.getSchema().equals(policy.getSchema()) &&
          p.getCellSize() == policy.getCellSize()) {
        throw new IllegalECPolicyException("A policy with same schema and " +
            "cell size already exists");
      }
    }
    policy.setName(assignedNewName);
    policy.setId(getNextAvailablePolicyID());
    this.userPoliciesByName.put(policy.getName(), policy);
    this.userPoliciesByID.put(policy.getId(), policy);
  }

  private byte getNextAvailablePolicyID() {
    byte currentId = this.userPoliciesByID.keySet().stream()
        .max(Byte::compareTo).orElse(USER_DEFINED_POLICY_START_ID);
    return (byte) (currentId + 1);
  }
}
