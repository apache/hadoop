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
package org.apache.hadoop.hdfs.server.federation.resolver;

import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

/**
 * Status of the namenode.
 */
public class NamenodeStatusReport {

  /** Namenode information. */
  private String nameserviceId = "";
  private String namenodeId = "";
  private String clusterId = "";
  private String blockPoolId = "";
  private String rpcAddress = "";
  private String serviceAddress = "";
  private String lifelineAddress = "";
  private String webAddress = "";
  private String webScheme = "";

  /** Namenode state. */
  private HAServiceState status = HAServiceState.STANDBY;
  private boolean safeMode = false;

  /** Datanodes stats. */
  private int liveDatanodes = -1;
  private int deadDatanodes = -1;
  private int staleDatanodes = -1;
  /** Decommissioning datanodes. */
  private int decomDatanodes = -1;
  /** Live decommissioned datanodes. */
  private int liveDecomDatanodes = -1;
  /** Dead decommissioned datanodes. */
  private int deadDecomDatanodes = -1;

  /** Live in maintenance datanodes. */
  private int inMaintenanceLiveDataNodes = -1;

  /** Dead in maintenance datanodes. */
  private int inMaintenanceDeadDataNodes = -1;

  /** Entering maintenance datanodes. */
  private int enteringMaintenanceDataNodes = -1;

  /** Space stats. */
  private long availableSpace = -1;
  private long numOfFiles = -1;
  private long numOfBlocks = -1;
  private long numOfBlocksMissing = -1;
  private long numOfBlocksPendingReplication = -1;
  private long numOfBlocksUnderReplicated = -1;
  private long numOfBlocksPendingDeletion = -1;
  private long totalSpace = -1;
  private long providedSpace = -1;

  /** If the fields are valid. */
  private boolean registrationValid = false;
  private boolean statsValid = false;
  private boolean haStateValid = false;

  public NamenodeStatusReport(String ns, String nn, String rpc, String service,
      String lifeline, String webScheme, String web) {
    this.nameserviceId = ns;
    this.namenodeId = nn;
    this.rpcAddress = rpc;
    this.serviceAddress = service;
    this.lifelineAddress = lifeline;
    this.webScheme = webScheme;
    this.webAddress = web;
  }

  /**
   * If the statistics are valid.
   *
   * @return If the statistics are valid.
   */
  public boolean statsValid() {
    return this.statsValid;
  }

  /**
   * If the registration is valid.
   *
   * @return If the registration is valid.
   */
  public boolean registrationValid() {
    return this.registrationValid;
  }

  /**
   * If the HA state is valid.
   *
   * @return If the HA state is valid.
   */
  public boolean haStateValid() {
    return this.haStateValid;
  }

  /**
   * Get the state of the Namenode being monitored.
   *
   * @return State of the Namenode.
   */
  public FederationNamenodeServiceState getState() {
    if (!registrationValid) {
      return FederationNamenodeServiceState.UNAVAILABLE;
    } else if (haStateValid) {
      return FederationNamenodeServiceState.getState(status);
    } else {
      return FederationNamenodeServiceState.ACTIVE;
    }
  }

  /**
   * Get the name service identifier.
   *
   * @return The name service identifier.
   */
  public String getNameserviceId() {
    return this.nameserviceId;
  }

  /**
   * Get the namenode identifier.
   *
   * @return The namenode identifier.
   */
  public String getNamenodeId() {
    return this.namenodeId;
  }

  /**
   * Get the cluster identifier.
   *
   * @return The cluster identifier.
   */
  public String getClusterId() {
    return this.clusterId;
  }

  /**
   * Get the block pool identifier.
   *
   * @return The block pool identifier.
   */
  public String getBlockPoolId() {
    return this.blockPoolId;
  }

  /**
   * Get the RPC address.
   *
   * @return The RPC address.
   */
  public String getRpcAddress() {
    return this.rpcAddress;
  }

  /**
   * Get the Service RPC address.
   *
   * @return The Service RPC address.
   */
  public String getServiceAddress() {
    return this.serviceAddress;
  }

  /**
   * Get the Lifeline RPC address.
   *
   * @return The Lifeline RPC address.
   */
  public String getLifelineAddress() {
    return this.lifelineAddress;
  }

  /**
   * Get the web address.
   *
   * @return The web address.
   */
  public String getWebAddress() {
    return this.webAddress;
  }

  /**
   * Get the scheme of web address.
   *
   * @return The scheme of web address.
   */
  public String getWebScheme() {
    return this.webScheme;
  }

  /**
   * Set the HA service state.
   *
   * @param state The HA service state to set.
   */
  public void setHAServiceState(HAServiceState state) {
    this.status = state;
    this.haStateValid = true;
  }

  /**
   * Set the namespace information.
   *
   * @param info Namespace information.
   */
  public void setNamespaceInfo(NamespaceInfo info) {
    this.clusterId = info.getClusterID();
    this.blockPoolId = info.getBlockPoolID();
    this.registrationValid = true;
  }

  public void setSafeMode(boolean safemode) {
    this.safeMode = safemode;
  }

  public boolean getSafemode() {
    return this.safeMode;
  }

  /**
   * Set the datanode information.
   *
   * @param numLive Number of live nodes.
   * @param numDead Number of dead nodes.
   * @param numStale Number of stale nodes.
   * @param numDecom Number of decommissioning nodes.
   * @param numLiveDecom Number of decommissioned live nodes.
   * @param numDeadDecom Number of decommissioned dead nodes.
   * @param numInMaintenanceLive Number of in maintenance live nodes.
   * @param numInMaintenanceDead Number of in maintenance dead nodes.
   * @param numEnteringMaintenance Number of entering maintenance nodes.
   */
  public void setDatanodeInfo(int numLive, int numDead, int numStale,
      int numDecom, int numLiveDecom, int numDeadDecom,
      int numInMaintenanceLive, int numInMaintenanceDead,
      int numEnteringMaintenance) {
    this.liveDatanodes = numLive;
    this.deadDatanodes = numDead;
    this.staleDatanodes = numStale;
    this.decomDatanodes = numDecom;
    this.liveDecomDatanodes = numLiveDecom;
    this.deadDecomDatanodes = numDeadDecom;
    this.inMaintenanceLiveDataNodes = numInMaintenanceLive;
    this.inMaintenanceDeadDataNodes = numInMaintenanceDead;
    this.enteringMaintenanceDataNodes = numEnteringMaintenance;
    this.statsValid = true;
  }

  /**
   * Get the number of live blocks.
   *
   * @return The number of dead nodes.
   */
  public int getNumLiveDatanodes() {
    return this.liveDatanodes;
  }

  /**
   * Get the number of dead nodes.
   *
   * @return The number of dead nodes.
   */
  public int getNumDeadDatanodes() {
    return this.deadDatanodes;
  }

  /**
   * Get the number of stale nodes.
   *
   * @return The number of stale nodes.
   */
  public int getNumStaleDatanodes() {
    return this.staleDatanodes;
  }

  /**
   * Get the number of decommissionining nodes.
   *
   * @return The number of decommissionining nodes.
   */
  public int getNumDecommissioningDatanodes() {
    return this.decomDatanodes;
  }

  /**
   * Get the number of live decommissioned nodes.
   *
   * @return The number of live decommissioned nodes.
   */
  public int getNumDecomLiveDatanodes() {
    return this.liveDecomDatanodes;
  }

  /**
   * Get the number of dead decommissioned nodes.
   *
   * @return The number of dead decommissioned nodes.
   */
  public int getNumDecomDeadDatanodes() {
    return this.deadDecomDatanodes;
  }

  /**
   * Get the number of live in maintenance nodes.
   *
   * @return The number of live in maintenance nodes.
   */
  public int getNumInMaintenanceLiveDataNodes() {
    return this.inMaintenanceLiveDataNodes;
  }

  /**
   * Get the number of dead in maintenance nodes.
   *
   * @return The number of dead in maintenance nodes.
   */
  public int getNumInMaintenanceDeadDataNodes() {
    return this.inMaintenanceDeadDataNodes;
  }

  /**
   * Get the number of entering maintenance nodes.
   *
   * @return The number of entering maintenance nodes.
   */
  public int getNumEnteringMaintenanceDataNodes() {
    return this.enteringMaintenanceDataNodes;
  }

  /**
   * Set the filesystem information.
   *
   * @param available Available capacity.
   * @param total Total capacity.
   * @param numFiles Number of files.
   * @param numBlocks Total number of blocks.
   * @param numBlocksMissing Number of missing blocks.
   * @param numBlocksPendingReplication Number of blocks pending replication.
   * @param numBlocksUnderReplicated Number of blocks under replication.
   * @param numBlocksPendingDeletion Number of blocks pending deletion.
   * @param providedSpace Space in provided storage.
   */
  public void setNamesystemInfo(long available, long total,
      long numFiles, long numBlocks, long numBlocksMissing,
      long numBlocksPendingReplication, long numBlocksUnderReplicated,
      long numBlocksPendingDeletion, long providedSpace) {
    this.totalSpace = total;
    this.availableSpace = available;
    this.numOfBlocks = numBlocks;
    this.numOfBlocksMissing = numBlocksMissing;
    this.numOfBlocksPendingReplication = numBlocksPendingReplication;
    this.numOfBlocksUnderReplicated = numBlocksUnderReplicated;
    this.numOfBlocksPendingDeletion = numBlocksPendingDeletion;
    this.numOfFiles = numFiles;
    this.statsValid = true;
    this.providedSpace = providedSpace;
  }

  /**
   * Get the number of blocks.
   *
   * @return The number of blocks.
   */
  public long getNumBlocks() {
    return this.numOfBlocks;
  }

  /**
   * Get the number of files.
   *
   * @return The number of files.
   */
  public long getNumFiles() {
    return this.numOfFiles;
  }

  /**
   * Get the total space.
   *
   * @return The total space.
   */
  public long getTotalSpace() {
    return this.totalSpace;
  }

  /**
   * Get the available space.
   *
   * @return The available space.
   */
  public long getAvailableSpace() {
    return this.availableSpace;
  }

  /**
   * Get the space occupied by provided storage.
   *
   * @return the provided capacity.
   */
  public long getProvidedSpace() {
    return this.providedSpace;
  }
  /**
   * Get the number of missing blocks.
   *
   * @return Number of missing blocks.
   */
  public long getNumBlocksMissing() {
    return this.numOfBlocksMissing;
  }

  /**
   * Get the number of pending replication blocks.
   *
   * @return Number of pending replication blocks.
   */
  public long getNumOfBlocksPendingReplication() {
    return this.numOfBlocksPendingReplication;
  }

  /**
   * Get the number of under replicated blocks.
   *
   * @return Number of under replicated blocks.
   */
  public long getNumOfBlocksUnderReplicated() {
    return this.numOfBlocksUnderReplicated;
  }

  /**
   * Get the number of pending deletion blocks.
   *
   * @return Number of pending deletion blocks.
   */
  public long getNumOfBlocksPendingDeletion() {
    return this.numOfBlocksPendingDeletion;
  }

  /**
   * Set the validity of registration.
   * @param isValid The desired value to be set.
   */
  public void setRegistrationValid(boolean isValid) {
    this.registrationValid = isValid;
  }

  @Override
  public String toString() {
    return String.format("%s-%s:%s",
        nameserviceId, namenodeId, serviceAddress);
  }
}
