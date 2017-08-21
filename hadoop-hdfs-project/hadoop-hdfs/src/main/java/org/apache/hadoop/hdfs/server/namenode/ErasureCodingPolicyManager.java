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

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.IllegalECPolicyException;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public static Logger LOG = LoggerFactory.getLogger(
      ErasureCodingPolicyManager.class);
  private int maxCellSize =
      DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_MAX_CELLSIZE_DEFAULT;

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
   * All removed policies sorted by name.
   */
  private Map<String, ErasureCodingPolicy> removedPoliciesByName;

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
    // Populate the list of enabled policies from configuration
    final String[] enablePolicyNames = conf.getTrimmedStrings(
            DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_ENABLED_KEY,
            DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_ENABLED_DEFAULT);
    final String defaultPolicyName = conf.getTrimmed(
            DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY,
            DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY_DEFAULT);
    final String[] policyNames =
            (String[]) ArrayUtils.add(enablePolicyNames, defaultPolicyName);
    this.userPoliciesByID = new TreeMap<>();
    this.userPoliciesByName = new TreeMap<>();
    this.removedPoliciesByName = new TreeMap<>();
    this.enabledPoliciesByName = new TreeMap<>();
    for (String policyName : policyNames) {
      if (policyName.trim().isEmpty()) {
        continue;
      }
      ErasureCodingPolicy ecPolicy =
          SystemErasureCodingPolicies.getByName(policyName);
      if (ecPolicy == null) {
        ecPolicy = userPoliciesByName.get(policyName);
        if (ecPolicy == null) {
          String allPolicies = SystemErasureCodingPolicies.getPolicies()
              .stream().map(ErasureCodingPolicy::getName)
              .collect(Collectors.joining(", ")) + ", " +
              userPoliciesByName.values().stream()
              .map(ErasureCodingPolicy::getName)
              .collect(Collectors.joining(", "));
          String msg = String.format("EC policy '%s' specified at %s is not a "
              + "valid policy. Please choose from list of available "
              + "policies: [%s]",
              policyName,
              DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_ENABLED_KEY,
              allPolicies);
          throw new IllegalArgumentException(msg);
        }
      }
      enabledPoliciesByName.put(ecPolicy.getName(), ecPolicy);
    }

    maxCellSize = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_MAX_CELLSIZE_KEY,
        DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_MAX_CELLSIZE_DEFAULT);

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
    ErasureCodingPolicy ecPolicy = enabledPoliciesByName.get(name);
    if (ecPolicy == null) {
      if (name.equalsIgnoreCase(ErasureCodeConstants.REPLICATION_POLICY_NAME)) {
        ecPolicy = SystemErasureCodingPolicies.getReplicationPolicy();
      }
    }
    return ecPolicy;
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
  public ErasureCodingPolicy[] getPolicies() {
    return Stream.concat(SystemErasureCodingPolicies.getPolicies().stream(),
        userPoliciesByName.values().stream())
        .toArray(ErasureCodingPolicy[]::new);
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

  /**
   * Add an erasure coding policy.
   * @return the added policy
   */
  public synchronized ErasureCodingPolicy addPolicy(ErasureCodingPolicy policy)
      throws IllegalECPolicyException {
    if (!CodecUtil.hasCodec(policy.getCodecName())) {
      throw new IllegalECPolicyException("Codec name "
          + policy.getCodecName() + " is not supported");
    }

    if (policy.getCellSize() > maxCellSize) {
      throw new IllegalECPolicyException("Cell size " + policy.getCellSize()
          + " should not exceed maximum " + maxCellSize + " byte");
    }

    String assignedNewName = ErasureCodingPolicy.composePolicyName(
        policy.getSchema(), policy.getCellSize());
    for (ErasureCodingPolicy p : getPolicies()) {
      if (p.getName().equals(assignedNewName)) {
        throw new IllegalECPolicyException("The policy name " + assignedNewName
            + " already exists");
      }
      if (p.getSchema().equals(policy.getSchema()) &&
          p.getCellSize() == policy.getCellSize()) {
        throw new IllegalECPolicyException("A policy with same schema "
            + policy.getSchema().toString() + " and cell size "
            + p.getCellSize() + " is already exists");
      }
    }
    policy.setName(assignedNewName);
    policy.setId(getNextAvailablePolicyID());
    this.userPoliciesByName.put(policy.getName(), policy);
    this.userPoliciesByID.put(policy.getId(), policy);
    return policy;
  }

  private byte getNextAvailablePolicyID() {
    byte currentId = this.userPoliciesByID.keySet().stream()
        .max(Byte::compareTo).orElse(
            ErasureCodeConstants.USER_DEFINED_POLICY_START_ID);
    return (byte) (currentId + 1);
  }

  /**
   * Remove an User erasure coding policy by policyName.
   */
  public synchronized void removePolicy(String name) {
    if (SystemErasureCodingPolicies.getByName(name) != null) {
      throw new IllegalArgumentException("System erasure coding policy " +
          name + " cannot be removed");
    }
    ErasureCodingPolicy policy = userPoliciesByName.get(name);
    if (policy == null) {
      throw new IllegalArgumentException("The policy name " +
          name + " does not exists");
    }
    enabledPoliciesByName.remove(name);
    removedPoliciesByName.put(name, policy);
  }

  public List<ErasureCodingPolicy> getRemovedPolicies() {
    return removedPoliciesByName.values().stream().collect(Collectors.toList());
  }

  /**
   * Disable an erasure coding policy by policyName.
   */
  public synchronized void disablePolicy(String name) {
    ErasureCodingPolicy sysEcPolicy = SystemErasureCodingPolicies
        .getByName(name);
    ErasureCodingPolicy userEcPolicy = userPoliciesByName.get(name);
    LOG.info("Disable the erasure coding policy " + name);
    if (sysEcPolicy == null &&
        userEcPolicy == null) {
      throw new IllegalArgumentException("The policy name " +
          name + " does not exists");
    }

    if(sysEcPolicy != null){
      enabledPoliciesByName.remove(name);
      removedPoliciesByName.put(name, sysEcPolicy);
    }
    if(userEcPolicy != null){
      enabledPoliciesByName.remove(name);
      removedPoliciesByName.put(name, userEcPolicy);
    }
  }

  /**
   * Enable an erasure coding policy by policyName.
   */
  public synchronized void enablePolicy(String name) {
    ErasureCodingPolicy sysEcPolicy = SystemErasureCodingPolicies
        .getByName(name);
    ErasureCodingPolicy userEcPolicy = userPoliciesByName.get(name);
    LOG.info("Enable the erasure coding policy " + name);
    if (sysEcPolicy == null &&
        userEcPolicy == null) {
      throw new IllegalArgumentException("The policy name " +
          name + " does not exists");
    }

    if(sysEcPolicy != null){
      enabledPoliciesByName.put(name, sysEcPolicy);
      removedPoliciesByName.remove(name);
    }
    if(userEcPolicy != null) {
      enabledPoliciesByName.put(name, userEcPolicy);
      removedPoliciesByName.remove(name);
    }
  }

}
