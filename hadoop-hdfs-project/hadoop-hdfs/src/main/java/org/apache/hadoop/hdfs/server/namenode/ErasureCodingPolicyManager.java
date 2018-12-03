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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyState;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
   * All policies sorted by name for fast querying, include built-in policy,
   * user defined policy, removed policy.
   */
  private Map<String, ErasureCodingPolicyInfo> policiesByName;

  /**
   * All policies sorted by ID for fast querying, including built-in policy,
   * user defined policy, removed policy.
   */
  private Map<Byte, ErasureCodingPolicyInfo> policiesByID;

  /**
   * For better performance when query all Policies.
   */
  private ErasureCodingPolicyInfo[] allPolicies;

  /**
   * All policies in the state as it will be persisted in the fsimage.
   *
   * The difference between persisted policies and all policies is that
   * if a default policy is only enabled at startup,
   * it will appear as disabled in the persisted policy list and in the fsimage.
   */
  private Map<Byte, ErasureCodingPolicyInfo> allPersistedPolicies;

  /**
   * All enabled policies sorted by name for fast querying, including built-in
   * policy, user defined policy.
   */
  private Map<String, ErasureCodingPolicy> enabledPoliciesByName;
  /**
   * For better performance when query all enabled Policies.
   */
  private ErasureCodingPolicy[] enabledPolicies;

  private String defaultPolicyName;

  private volatile static ErasureCodingPolicyManager instance = null;

  public static ErasureCodingPolicyManager getInstance() {
    if (instance == null) {
      instance = new ErasureCodingPolicyManager();
    }
    return instance;
  }

  private ErasureCodingPolicyManager() {}

  public void init(Configuration conf) throws IOException {
    this.policiesByName = new TreeMap<>();
    this.policiesByID = new TreeMap<>();
    this.enabledPoliciesByName = new TreeMap<>();
    this.allPersistedPolicies = new TreeMap<>();

    /**
     * TODO: load user defined EC policy from fsImage HDFS-7859
     * load persistent policies from image and editlog, which is done only once
     * during NameNode startup. This can be done here or in a separate method.
     */

    /*
     * Add all System built-in policies into policy map
     */
    for (ErasureCodingPolicy policy :
        SystemErasureCodingPolicies.getPolicies()) {
      final ErasureCodingPolicyInfo info = new ErasureCodingPolicyInfo(policy);
      policiesByName.put(policy.getName(), info);
      policiesByID.put(policy.getId(), info);
      allPersistedPolicies.put(policy.getId(),
          new ErasureCodingPolicyInfo(policy));
    }

    enableDefaultPolicy(conf);
    updatePolicies();
    maxCellSize = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_MAX_CELLSIZE_KEY,
        DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_MAX_CELLSIZE_DEFAULT);
  }

  /**
   * Get the set of enabled policies.
   * @return all policies
   */
  public ErasureCodingPolicy[] getEnabledPolicies() {
    return enabledPolicies;
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
  public ErasureCodingPolicyInfo[] getPolicies() {
    return allPolicies;
  }

  /**
   * Get all system defined policies and user defined policies
   * as it is written out in the fsimage.
   *
   * The difference between persisted policies and all policies is that
   * if a default policy is only enabled at startup,
   * it will appear as disabled in the persisted policy list and in the fsimage.
   *
   * @return persisted policies
   */
  public ErasureCodingPolicyInfo[] getPersistedPolicies() {
    return allPersistedPolicies.values()
        .toArray(new ErasureCodingPolicyInfo[0]);
  }

  public ErasureCodingPolicyInfo[] getCopyOfPolicies() {
    ErasureCodingPolicyInfo[] copy;
    synchronized (this) {
      copy = Arrays.copyOf(allPolicies, allPolicies.length);
    }
    return copy;
  }

  /**
   * Get a {@link ErasureCodingPolicy} by policy ID, including system policy
   * and user defined policy.
   * @return ecPolicy, or null if not found
   */
  public ErasureCodingPolicy getByID(byte id) {
    final ErasureCodingPolicyInfo ecpi = getPolicyInfoByID(id);
    if (ecpi == null) {
      return null;
    }
    return ecpi.getPolicy();
  }

  /**
   * Get a {@link ErasureCodingPolicyInfo} by policy ID, including system policy
   * and user defined policy.
   */
  private ErasureCodingPolicyInfo getPolicyInfoByID(final byte id) {
    return this.policiesByID.get(id);
  }

  /**
   * Get a {@link ErasureCodingPolicy} by policy name, including system
   * policy and user defined policy.
   * @return ecPolicy, or null if not found
   */
  public ErasureCodingPolicy getByName(String name) {
    final ErasureCodingPolicyInfo ecpi = getPolicyInfoByName(name);
    if (ecpi == null) {
      return null;
    }
    return ecpi.getPolicy();
  }

  /**
   * Get a {@link ErasureCodingPolicyInfo} by policy name, including system
   * policy and user defined policy.
   * @return ecPolicy, or null if not found
   */
  private ErasureCodingPolicyInfo getPolicyInfoByName(final String name) {
    return this.policiesByName.get(name);
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
  public synchronized ErasureCodingPolicy addPolicy(
      ErasureCodingPolicy policy) {
    if (!CodecUtil.hasCodec(policy.getCodecName())) {
      throw new HadoopIllegalArgumentException("Codec name "
          + policy.getCodecName() + " is not supported");
    }

    if (policy.getCellSize() > maxCellSize) {
      throw new HadoopIllegalArgumentException("Cell size " +
          policy.getCellSize() + " should not exceed maximum " +
          maxCellSize + " bytes");
    }

    String assignedNewName = ErasureCodingPolicy.composePolicyName(
        policy.getSchema(), policy.getCellSize());
    for (ErasureCodingPolicyInfo info : getPolicies()) {
      final ErasureCodingPolicy p = info.getPolicy();
      if (p.getName().equals(assignedNewName)) {
        LOG.info("The policy name " + assignedNewName + " already exists");
        return p;
      }
      if (p.getSchema().equals(policy.getSchema()) &&
          p.getCellSize() == policy.getCellSize()) {
        LOG.info("A policy with same schema "
            + policy.getSchema().toString() + " and cell size "
            + p.getCellSize() + " already exists");
        return p;
      }
    }

    if (getCurrentMaxPolicyID() == ErasureCodeConstants.MAX_POLICY_ID) {
      throw new HadoopIllegalArgumentException("Adding erasure coding " +
          "policy failed because the number of policies stored in the " +
          "system already reached the threshold, which is " +
          ErasureCodeConstants.MAX_POLICY_ID);
    }

    policy = new ErasureCodingPolicy(assignedNewName, policy.getSchema(),
        policy.getCellSize(), getNextAvailablePolicyID());
    final ErasureCodingPolicyInfo pi = new ErasureCodingPolicyInfo(policy);
    this.policiesByName.put(policy.getName(), pi);
    this.policiesByID.put(policy.getId(), pi);
    allPolicies =
        policiesByName.values().toArray(new ErasureCodingPolicyInfo[0]);
    allPersistedPolicies.put(policy.getId(),
        new ErasureCodingPolicyInfo(policy));
    return policy;
  }

  private byte getCurrentMaxPolicyID() {
    return policiesByID.keySet().stream().max(Byte::compareTo).orElse((byte)0);
  }

  private byte getNextAvailablePolicyID() {
    byte nextPolicyID = (byte)(getCurrentMaxPolicyID() + 1);
    return nextPolicyID > ErasureCodeConstants.USER_DEFINED_POLICY_START_ID ?
        nextPolicyID : ErasureCodeConstants.USER_DEFINED_POLICY_START_ID;
  }

  /**
   * Remove an User erasure coding policy by policyName.
   */
  public synchronized void removePolicy(String name) {
    final ErasureCodingPolicyInfo info = policiesByName.get(name);
    if (info == null) {
      throw new HadoopIllegalArgumentException("The policy name " +
          name + " does not exist");
    }

    final ErasureCodingPolicy ecPolicy = info.getPolicy();
    if (ecPolicy.isSystemPolicy()) {
      throw new HadoopIllegalArgumentException("System erasure coding policy " +
          name + " cannot be removed");
    }

    if (enabledPoliciesByName.containsKey(name)) {
      enabledPoliciesByName.remove(name);
      enabledPolicies =
          enabledPoliciesByName.values().toArray(new ErasureCodingPolicy[0]);
    }
    info.setState(ErasureCodingPolicyState.REMOVED);
    LOG.info("Remove erasure coding policy " + name);
    allPersistedPolicies.put(ecPolicy.getId(),
        createPolicyInfo(ecPolicy, ErasureCodingPolicyState.REMOVED));
    /*
     * TODO HDFS-12405 postpone the delete removed policy to Namenode restart
     * time.
     * */
  }

  @VisibleForTesting
  public List<ErasureCodingPolicy> getRemovedPolicies() {
    ArrayList<ErasureCodingPolicy> removedPolicies = new ArrayList<>();
    for (ErasureCodingPolicyInfo info : policiesByName.values()) {
      final ErasureCodingPolicy ecPolicy = info.getPolicy();
      if (info.isRemoved()) {
        removedPolicies.add(ecPolicy);
      }
    }
    return removedPolicies;
  }

  /**
   * Disable an erasure coding policy by policyName.
   */
  public synchronized boolean disablePolicy(String name) {
    ErasureCodingPolicyInfo info = policiesByName.get(name);
    if (info == null) {
      throw new HadoopIllegalArgumentException("The policy name " +
          name + " does not exist");
    }

    if (enabledPoliciesByName.containsKey(name)) {
      enabledPoliciesByName.remove(name);
      enabledPolicies =
          enabledPoliciesByName.values().toArray(new ErasureCodingPolicy[0]);
      info.setState(ErasureCodingPolicyState.DISABLED);
      LOG.info("Disable the erasure coding policy " + name);
      allPersistedPolicies.put(info.getPolicy().getId(),
          createPolicyInfo(info.getPolicy(),
              ErasureCodingPolicyState.DISABLED));
      return true;
    }
    return false;
  }

  /**
   * Enable an erasure coding policy by policyName.
   */
  public synchronized boolean enablePolicy(String name) {
    final ErasureCodingPolicyInfo info = policiesByName.get(name);
    if (info == null) {
      throw new HadoopIllegalArgumentException("The policy name " +
          name + " does not exist");
    }
    if (enabledPoliciesByName.containsKey(name)) {
      if (defaultPolicyName.equals(name)) {
        allPersistedPolicies.put(info.getPolicy().getId(),
            createPolicyInfo(info.getPolicy(),
                ErasureCodingPolicyState.ENABLED));
        return true;
      }
      return false;
    }
    final ErasureCodingPolicy ecPolicy = info.getPolicy();
    enabledPoliciesByName.put(name, ecPolicy);
    info.setState(ErasureCodingPolicyState.ENABLED);
    enabledPolicies =
        enabledPoliciesByName.values().toArray(new ErasureCodingPolicy[0]);
    allPersistedPolicies.put(ecPolicy.getId(),
        createPolicyInfo(info.getPolicy(), ErasureCodingPolicyState.ENABLED));
    LOG.info("Enable the erasure coding policy " + name);
    return true;
  }

  /**
   * Load an erasure coding policy into erasure coding manager.
   */
  private void loadPolicy(ErasureCodingPolicyInfo info) {
    Preconditions.checkNotNull(info);
    final ErasureCodingPolicy policy = info.getPolicy();
    if (!CodecUtil.hasCodec(policy.getCodecName()) ||
        policy.getCellSize() > maxCellSize) {
      // If policy is not supported in current system, set the policy state to
      // DISABLED;
      info.setState(ErasureCodingPolicyState.DISABLED);
    }

    this.policiesByName.put(policy.getName(), info);
    this.policiesByID.put(policy.getId(), info);
    if (info.isEnabled()) {
      enablePolicy(policy.getName());
    }
    allPersistedPolicies.put(policy.getId(),
        createPolicyInfo(policy, info.getState()));
  }

  /**
   * Reload erasure coding policies from fsImage.
   *
   * @param ecPolicies contains ErasureCodingPolicy list
   *
   */
  public synchronized void loadPolicies(
      List<ErasureCodingPolicyInfo> ecPolicies, Configuration conf)
      throws IOException{
    Preconditions.checkNotNull(ecPolicies);
    for (ErasureCodingPolicyInfo p : ecPolicies) {
      loadPolicy(p);
    }
    enableDefaultPolicy(conf);
    updatePolicies();
  }

  private void enableDefaultPolicy(Configuration conf) throws IOException {
    defaultPolicyName = conf.getTrimmed(
        DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY,
        DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY_DEFAULT);
    if (!defaultPolicyName.isEmpty()) {
      final ErasureCodingPolicyInfo info =
          policiesByName.get(defaultPolicyName);
      if (info == null) {
        String names = policiesByName.values()
            .stream().map((pi) -> pi.getPolicy().getName())
            .collect(Collectors.joining(", "));
        String msg = String.format("EC policy '%s' specified at %s is not a "
                + "valid policy. Please choose from list of available "
                + "policies: [%s]",
            defaultPolicyName,
            DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY,
            names);
        throw new IOException(msg);
      }
      info.setState(ErasureCodingPolicyState.ENABLED);
      enabledPoliciesByName.put(info.getPolicy().getName(), info.getPolicy());
    }
  }

  private void updatePolicies() {
    enabledPolicies =
        enabledPoliciesByName.values().toArray(new ErasureCodingPolicy[0]);
    allPolicies =
        policiesByName.values().toArray(new ErasureCodingPolicyInfo[0]);
  }

  public String getEnabledPoliciesMetric() {
    return StringUtils.join(", ",
            enabledPoliciesByName.keySet());
  }

  private ErasureCodingPolicyInfo createPolicyInfo(ErasureCodingPolicy p,
                                                   ErasureCodingPolicyState s) {
    ErasureCodingPolicyInfo policyInfo = new ErasureCodingPolicyInfo(p);
    policyInfo.setState(s);
    return policyInfo;
  }
}