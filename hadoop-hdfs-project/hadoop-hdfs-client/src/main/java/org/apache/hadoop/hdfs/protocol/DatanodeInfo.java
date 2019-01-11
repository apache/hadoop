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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSUtilClient.percent2String;

/**
 * This class extends the primary identifier of a Datanode with ephemeral
 * state, eg usage information, current administrative state, and the
 * network location that is communicated to clients.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeInfo extends DatanodeID implements Node {
  private long capacity;
  private long dfsUsed;
  private long nonDfsUsed;
  private long remaining;
  private long blockPoolUsed;
  private long cacheCapacity;
  private long cacheUsed;
  private long lastUpdate;
  private long lastUpdateMonotonic;
  private int xceiverCount;
  private volatile String location = NetworkTopology.DEFAULT_RACK;
  private String softwareVersion;
  private List<String> dependentHostNames = new LinkedList<>();
  private String upgradeDomain;
  public static final DatanodeInfo[] EMPTY_ARRAY = {};
  private int numBlocks;

  // Datanode administrative states
  public enum AdminStates {
    NORMAL("In Service"),
    DECOMMISSION_INPROGRESS("Decommission In Progress"),
    DECOMMISSIONED("Decommissioned"),
    ENTERING_MAINTENANCE("Entering Maintenance"),
    IN_MAINTENANCE("In Maintenance");

    final String value;

    AdminStates(final String v) {
      this.value = v;
    }

    @Override
    public String toString() {
      return value;
    }

    public static AdminStates fromValue(final String value) {
      for (AdminStates as : AdminStates.values()) {
        if (as.value.equals(value)) return as;
      }
      return NORMAL;
    }
  }

  protected AdminStates adminState;
  private long maintenanceExpireTimeInMS;
  private long lastBlockReportTime;
  private long lastBlockReportMonotonic;

  protected DatanodeInfo(DatanodeInfo from) {
    super(from);
    this.capacity = from.getCapacity();
    this.dfsUsed = from.getDfsUsed();
    this.nonDfsUsed = from.getNonDfsUsed();
    this.remaining = from.getRemaining();
    this.blockPoolUsed = from.getBlockPoolUsed();
    this.cacheCapacity = from.getCacheCapacity();
    this.cacheUsed = from.getCacheUsed();
    this.lastUpdate = from.getLastUpdate();
    this.lastUpdateMonotonic = from.getLastUpdateMonotonic();
    this.xceiverCount = from.getXceiverCount();
    this.location = from.getNetworkLocation();
    this.adminState = from.getAdminState();
    this.upgradeDomain = from.getUpgradeDomain();
    this.lastBlockReportTime = from.getLastBlockReportTime();
    this.lastBlockReportMonotonic = from.getLastBlockReportMonotonic();
    this.numBlocks = from.getNumBlocks();
  }

  protected DatanodeInfo(DatanodeID nodeID) {
    super(nodeID);
    this.capacity = 0L;
    this.dfsUsed = 0L;
    this.nonDfsUsed = 0L;
    this.remaining = 0L;
    this.blockPoolUsed = 0L;
    this.cacheCapacity = 0L;
    this.cacheUsed = 0L;
    this.lastUpdate = 0L;
    this.lastUpdateMonotonic = 0L;
    this.xceiverCount = 0;
    this.adminState = null;
    this.lastBlockReportTime = 0L;
    this.lastBlockReportMonotonic = 0L;
    this.numBlocks = 0;
  }

  protected DatanodeInfo(DatanodeID nodeID, String location) {
    this(nodeID);
    this.location = location;
  }

  /** Constructor. */
  private DatanodeInfo(final String ipAddr, final String hostName,
      final String datanodeUuid, final int xferPort, final int infoPort,
      final int infoSecurePort, final int ipcPort, final long capacity,
      final long dfsUsed, final long nonDfsUsed, final long remaining,
      final long blockPoolUsed, final long cacheCapacity, final long cacheUsed,
      final long lastUpdate, final long lastUpdateMonotonic,
      final int xceiverCount, final String networkLocation,
      final AdminStates adminState, final String upgradeDomain,
      final long lastBlockReportTime, final long lastBlockReportMonotonic,
                       final int blockCount) {
    super(ipAddr, hostName, datanodeUuid, xferPort, infoPort, infoSecurePort,
        ipcPort);
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.nonDfsUsed = nonDfsUsed;
    this.remaining = remaining;
    this.blockPoolUsed = blockPoolUsed;
    this.cacheCapacity = cacheCapacity;
    this.cacheUsed = cacheUsed;
    this.lastUpdate = lastUpdate;
    this.lastUpdateMonotonic = lastUpdateMonotonic;
    this.xceiverCount = xceiverCount;
    this.location = networkLocation;
    this.adminState = adminState;
    this.upgradeDomain = upgradeDomain;
    this.lastBlockReportTime = lastBlockReportTime;
    this.lastBlockReportMonotonic = lastBlockReportMonotonic;
    this.numBlocks = blockCount;
  }

  /** Network location name. */
  @Override
  public String getName() {
    return getXferAddr();
  }

  /** The raw capacity. */
  public long getCapacity() { return capacity; }

  /** The used space by the data node. */
  public long getDfsUsed() { return dfsUsed; }

  /** The used space by the block pool on data node. */
  public long getBlockPoolUsed() { return blockPoolUsed; }

  /** The used space by the data node. */
  public long getNonDfsUsed() {
    return nonDfsUsed;
  }

  /** The used space by the data node as percentage of present capacity */
  public float getDfsUsedPercent() {
    return DFSUtilClient.getPercentUsed(dfsUsed, capacity);
  }

  /** The raw free space. */
  public long getRemaining() { return remaining; }

  /** Used space by the block pool as percentage of present capacity */
  public float getBlockPoolUsedPercent() {
    return DFSUtilClient.getPercentUsed(blockPoolUsed, capacity);
  }

  /** The remaining space as percentage of configured capacity. */
  public float getRemainingPercent() {
    return DFSUtilClient.getPercentRemaining(remaining, capacity);
  }

  /**
   * @return Amount of cache capacity in bytes
   */
  public long getCacheCapacity() {
    return cacheCapacity;
  }

  /**
   * @return Amount of cache used in bytes
   */
  public long getCacheUsed() {
    return cacheUsed;
  }

  /**
   * @return Cache used as a percentage of the datanode's total cache capacity
   */
  public float getCacheUsedPercent() {
    return DFSUtilClient.getPercentUsed(cacheUsed, cacheCapacity);
  }

  /**
   * @return Amount of cache remaining in bytes
   */
  public long getCacheRemaining() {
    return cacheCapacity - cacheUsed;
  }

  /**
   * @return Cache remaining as a percentage of the datanode's total cache
   * capacity
   */
  public float getCacheRemainingPercent() {
    return DFSUtilClient.getPercentRemaining(getCacheRemaining(), cacheCapacity);
  }

  /**
   * Get the last update timestamp.
   * Return value is suitable for Date conversion.
   */
  public long getLastUpdate() { return lastUpdate; }

  /**
   * The time when this information was accurate. <br>
   * Ps: So return value is ideal for calculation of time differences.
   * Should not be used to convert to Date.
   */
  public long getLastUpdateMonotonic() { return lastUpdateMonotonic;}

  /**
   * @return Num of Blocks
   */
  public int getNumBlocks() {
    return numBlocks;
  }

  /**
   * Set lastUpdate monotonic time
   */
  public void setLastUpdateMonotonic(long lastUpdateMonotonic) {
    this.lastUpdateMonotonic = lastUpdateMonotonic;
  }

  /** number of active connections */
  public int getXceiverCount() { return xceiverCount; }

  /** Sets raw capacity. */
  public void setCapacity(long capacity) {
    this.capacity = capacity;
  }

  /** Sets the used space for the datanode. */
  public void setDfsUsed(long dfsUsed) {
    this.dfsUsed = dfsUsed;
  }

  /** Sets the nondfs-used space for the datanode. */
  public void setNonDfsUsed(long nonDfsUsed) {
    this.nonDfsUsed = nonDfsUsed;
  }

  /** Sets raw free space. */
  public void setRemaining(long remaining) {
    this.remaining = remaining;
  }

  /** Sets block pool used space */
  public void setBlockPoolUsed(long bpUsed) {
    this.blockPoolUsed = bpUsed;
  }

  /** Sets cache capacity. */
  public void setCacheCapacity(long cacheCapacity) {
    this.cacheCapacity = cacheCapacity;
  }

  /** Sets cache used. */
  public void setCacheUsed(long cacheUsed) {
    this.cacheUsed = cacheUsed;
  }

  /** Sets time when this information was accurate. */
  public void setLastUpdate(long lastUpdate) {
    this.lastUpdate = lastUpdate;
  }

  /** Sets number of active connections */
  public void setXceiverCount(int xceiverCount) {
    this.xceiverCount = xceiverCount;
  }

  /** Sets number of blocks. */
  public void setNumBlocks(int blockCount) {
    this.numBlocks = blockCount;
  }

  /** network location */
  @Override
  public String getNetworkLocation() {return location;}

  /** Sets the network location */
  @Override
  public void setNetworkLocation(String location) {
    this.location = NodeBase.normalize(location);
  }

  /** Sets the upgrade domain */
  public void setUpgradeDomain(String upgradeDomain) {
    this.upgradeDomain = upgradeDomain;
  }

  /** upgrade domain */
  public String getUpgradeDomain() {
    return upgradeDomain;
  }

  /** Add a hostname to a list of network dependencies */
  public void addDependentHostName(String hostname) {
    dependentHostNames.add(hostname);
  }

  /** List of Network dependencies */
  public List<String> getDependentHostNames() {
    return dependentHostNames;
  }

  /** Sets the network dependencies */
  public void setDependentHostNames(List<String> dependencyList) {
    dependentHostNames = dependencyList;
  }

  /** A formatted string for reporting the status of the DataNode. */
  public String getDatanodeReport() {
    StringBuilder buffer = new StringBuilder();
    long c = getCapacity();
    long r = getRemaining();
    long u = getDfsUsed();
    long nonDFSUsed = getNonDfsUsed();
    float usedPercent = getDfsUsedPercent();
    float remainingPercent = getRemainingPercent();
    long cc = getCacheCapacity();
    long cr = getCacheRemaining();
    long cu = getCacheUsed();
    float cacheUsedPercent = getCacheUsedPercent();
    float cacheRemainingPercent = getCacheRemainingPercent();
    String lookupName = NetUtils.getHostNameOfIP(getName());
    int blockCount = getNumBlocks();

    buffer.append("Name: ").append(getName());
    if (lookupName != null) {
      buffer.append(" (").append(lookupName).append(")");
    }
    buffer.append("\n")
        .append("Hostname: ").append(getHostName()).append("\n");

    if (!NetworkTopology.DEFAULT_RACK.equals(location)) {
      buffer.append("Rack: ").append(location).append("\n");
    }
    if (upgradeDomain != null) {
      buffer.append("Upgrade domain: ").append(upgradeDomain).append("\n");
    }
    buffer.append("Decommission Status : ");
    if (isDecommissioned()) {
      buffer.append("Decommissioned\n");
    } else if (isDecommissionInProgress()) {
      buffer.append("Decommission in progress\n");
    } else if (isInMaintenance()) {
      buffer.append("In maintenance\n");
    } else if (isEnteringMaintenance()) {
      buffer.append("Entering maintenance\n");
    } else {
      buffer.append("Normal\n");
    }
    buffer.append("Configured Capacity: ").append(c).append(" (")
        .append(StringUtils.byteDesc(c)).append(")").append("\n")
        .append("DFS Used: ").append(u).append(" (")
        .append(StringUtils.byteDesc(u)).append(")").append("\n")
        .append("Non DFS Used: ").append(nonDFSUsed).append(" (")
        .append(StringUtils.byteDesc(nonDFSUsed)).append(")").append("\n")
        .append("DFS Remaining: ").append(r).append(" (")
        .append(StringUtils.byteDesc(r)).append(")").append("\n")
        .append("DFS Used%: ").append(percent2String(usedPercent))
        .append("\n")
        .append("DFS Remaining%: ").append(percent2String(remainingPercent))
        .append("\n")
        .append("Configured Cache Capacity: ").append(cc).append(" (")
        .append(StringUtils.byteDesc(cc)).append(")").append("\n")
        .append("Cache Used: ").append(cu).append(" (")
        .append(StringUtils.byteDesc(cu)).append(")").append("\n")
        .append("Cache Remaining: ").append(cr).append(" (")
        .append(StringUtils.byteDesc(cr)).append(")").append("\n")
        .append("Cache Used%: ").append(percent2String(cacheUsedPercent))
        .append("\n")
        .append("Cache Remaining%: ")
        .append(percent2String(cacheRemainingPercent)).append("\n")
        .append("Xceivers: ").append(getXceiverCount()).append("\n")
        .append("Last contact: ").append(new Date(lastUpdate)).append("\n")
        .append("Last Block Report: ")
        .append(
            lastBlockReportTime != 0 ? new Date(lastBlockReportTime) : "Never")
        .append("\n")
        .append("Num of Blocks: ").append(blockCount).append("\n");
    return buffer.toString();
  }

  /** A formatted string for printing the status of the DataNode. */
  public String dumpDatanode() {
    StringBuilder buffer = new StringBuilder();
    long c = getCapacity();
    long r = getRemaining();
    long u = getDfsUsed();
    float usedPercent = getDfsUsedPercent();
    long cc = getCacheCapacity();
    long cr = getCacheRemaining();
    long cu = getCacheUsed();
    float cacheUsedPercent = getCacheUsedPercent();
    buffer.append(getName());
    if (!NetworkTopology.DEFAULT_RACK.equals(location)) {
      buffer.append(" ").append(location);
    }
    if (upgradeDomain != null) {
      buffer.append(" ").append(upgradeDomain);
    }
    if (isDecommissioned()) {
      buffer.append(" DD");
    } else if (isDecommissionInProgress()) {
      buffer.append(" DP");
    } else if (isInMaintenance()) {
      buffer.append(" IM");
    } else if (isEnteringMaintenance()) {
      buffer.append(" EM");
    } else {
      buffer.append(" IN");
    }
    buffer.append(" ").append(c).append("(").append(StringUtils.byteDesc(c))
        .append(")")
        .append(" ").append(u).append("(").append(StringUtils.byteDesc(u))
        .append(")")
        .append(" ").append(percent2String(usedPercent))
        .append(" ").append(r).append("(").append(StringUtils.byteDesc(r))
        .append(")")
        .append(" ").append(cc).append("(").append(StringUtils.byteDesc(cc))
        .append(")")
        .append(" ").append(cu).append("(").append(StringUtils.byteDesc(cu))
        .append(")")
        .append(" ").append(percent2String(cacheUsedPercent))
        .append(" ").append(cr).append("(").append(StringUtils.byteDesc(cr))
        .append(")")
        .append(" ").append(new Date(lastUpdate));
    return buffer.toString();
  }

  /**
   * Start decommissioning a node.
   * old state.
   */
  public void startDecommission() {
    adminState = AdminStates.DECOMMISSION_INPROGRESS;
  }

  /**
   * Stop decommissioning a node.
   * old state.
   */
  public void stopDecommission() {
    adminState = null;
  }

  /**
   * Returns true if the node is in the process of being decommissioned
   */
  public boolean isDecommissionInProgress() {
    return adminState == AdminStates.DECOMMISSION_INPROGRESS;
  }

  /**
   * Returns true if the node has been decommissioned.
   */
  public boolean isDecommissioned() {
    return adminState == AdminStates.DECOMMISSIONED;
  }

  /**
   * Sets the admin state to indicate that decommission is complete.
   */
  public void setDecommissioned() {
    adminState = AdminStates.DECOMMISSIONED;
  }

  /**
   * Start the maintenance operation.
   */
  public void startMaintenance() {
    this.adminState = AdminStates.ENTERING_MAINTENANCE;
  }

  /**
   * Put a node directly to maintenance mode.
   */
  public void setInMaintenance() {
    this.adminState = AdminStates.IN_MAINTENANCE;
  }

  /**
  * @param maintenanceExpireTimeInMS the time that the DataNode is in the
  *        maintenance mode until in the unit of milliseconds.   */
  public void setMaintenanceExpireTimeInMS(long maintenanceExpireTimeInMS) {
    this.maintenanceExpireTimeInMS = maintenanceExpireTimeInMS;
  }

  public long getMaintenanceExpireTimeInMS() {
    return this.maintenanceExpireTimeInMS;
  }

  /** Sets the last block report time. */
  public void setLastBlockReportTime(long lastBlockReportTime) {
    this.lastBlockReportTime = lastBlockReportTime;
  }

  /** Sets the last block report monotonic time. */
  public void setLastBlockReportMonotonic(long lastBlockReportMonotonic) {
    this.lastBlockReportMonotonic = lastBlockReportMonotonic;
  }

  /** Last block report time. */
  public long getLastBlockReportTime() {
    return lastBlockReportTime;
  }

  /** Last block report monotonic time. */
  public long getLastBlockReportMonotonic() {
    return lastBlockReportMonotonic;
  }

  /**
   * Take the node out of maintenance mode.
   */
  public void stopMaintenance() {
    adminState = null;
  }

  public static boolean maintenanceNotExpired(long maintenanceExpireTimeInMS) {
    return Time.now() < maintenanceExpireTimeInMS;
  }
  /**
   * Returns true if the node is is entering_maintenance
   */
  public boolean isEnteringMaintenance() {
    return adminState == AdminStates.ENTERING_MAINTENANCE;
  }

  /**
   * Returns true if the node is in maintenance
   */
  public boolean isInMaintenance() {
    return adminState == AdminStates.IN_MAINTENANCE;
  }

  /**
   * Returns true if the node is entering or in maintenance
   */
  public boolean isMaintenance() {
    return (adminState == AdminStates.ENTERING_MAINTENANCE ||
        adminState == AdminStates.IN_MAINTENANCE);
  }

  public boolean maintenanceExpired() {
    return !maintenanceNotExpired(this.maintenanceExpireTimeInMS);
  }

  public boolean isInService() {
    return getAdminState() == AdminStates.NORMAL;
  }

  /**
   * Retrieves the admin state of this node.
   */
  public AdminStates getAdminState() {
    if (adminState == null) {
      return AdminStates.NORMAL;
    }
    return adminState;
  }

  /**
   * Check if the datanode is in stale state. Here if
   * the namenode has not received heartbeat msg from a
   * datanode for more than staleInterval,
   * the datanode will be treated as stale node.
   *
   * @param staleInterval
   *          the time interval for marking the node as stale. If the last
   *          update time is beyond the given time interval, the node will be
   *          marked as stale.
   * @return true if the node is stale
   */
  public boolean isStale(long staleInterval) {
    return (Time.monotonicNow() - lastUpdateMonotonic) >= staleInterval;
  }

  /**
   * Sets the admin state of this node.
   */
  protected void setAdminState(AdminStates newState) {
    if (newState == AdminStates.NORMAL) {
      adminState = null;
    }
    else {
      adminState = newState;
    }
  }

  private transient int level; //which level of the tree the node resides
  private transient Node parent; //its parent

  /** Return this node's parent */
  @Override
  public Node getParent() { return parent; }
  @Override
  public void setParent(Node parent) {this.parent = parent;}

  /** Return this node's level in the tree.
   * E.g. the root of a tree returns 0 and its children return 1
   */
  @Override
  public int getLevel() { return level; }
  @Override
  public void setLevel(int level) {this.level = level;}

  @Override
  public int hashCode() {
    // Super implementation is sufficient
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    // Sufficient to use super equality as datanodes are uniquely identified
    // by DatanodeID
    return (this == obj) || super.equals(obj);
  }

  public String getSoftwareVersion() {
    return softwareVersion;
  }

  public void setSoftwareVersion(String softwareVersion) {
    this.softwareVersion = softwareVersion;
  }

  /**
   * Building the DataNodeInfo.
   */
  public static class DatanodeInfoBuilder {
    private String location = NetworkTopology.DEFAULT_RACK;
    private long capacity;
    private long dfsUsed;
    private long remaining;
    private long blockPoolUsed;
    private long cacheCapacity;
    private long cacheUsed;
    private long lastUpdate;
    private long lastUpdateMonotonic;
    private int xceiverCount;
    private DatanodeInfo.AdminStates adminState;
    private String upgradeDomain;
    private String ipAddr;
    private String hostName;
    private String datanodeUuid;
    private int xferPort;
    private int infoPort;
    private int infoSecurePort;
    private int ipcPort;
    private long nonDfsUsed = 0L;
    private long lastBlockReportTime = 0L;
    private long lastBlockReportMonotonic = 0L;
    private int numBlocks;


    public DatanodeInfoBuilder setFrom(DatanodeInfo from) {
      this.capacity = from.getCapacity();
      this.dfsUsed = from.getDfsUsed();
      this.nonDfsUsed = from.getNonDfsUsed();
      this.remaining = from.getRemaining();
      this.blockPoolUsed = from.getBlockPoolUsed();
      this.cacheCapacity = from.getCacheCapacity();
      this.cacheUsed = from.getCacheUsed();
      this.lastUpdate = from.getLastUpdate();
      this.lastUpdateMonotonic = from.getLastUpdateMonotonic();
      this.xceiverCount = from.getXceiverCount();
      this.location = from.getNetworkLocation();
      this.adminState = from.getAdminState();
      this.upgradeDomain = from.getUpgradeDomain();
      this.lastBlockReportTime = from.getLastBlockReportTime();
      this.lastBlockReportMonotonic = from.getLastBlockReportMonotonic();
      this.numBlocks = from.getNumBlocks();
      setNodeID(from);
      return this;
    }

    public DatanodeInfoBuilder setNodeID(DatanodeID nodeID) {
      this.ipAddr = nodeID.getIpAddr();
      this.hostName = nodeID.getHostName();
      this.datanodeUuid = nodeID.getDatanodeUuid();
      this.xferPort = nodeID.getXferPort();
      this.infoPort = nodeID.getInfoPort();
      this.infoSecurePort = nodeID.getInfoSecurePort();
      this.ipcPort = nodeID.getIpcPort();
      return this;
    }

    public DatanodeInfoBuilder setCapacity(long capacity) {
      this.capacity = capacity;
      return this;
    }

    public DatanodeInfoBuilder setDfsUsed(long dfsUsed) {
      this.dfsUsed = dfsUsed;
      return this;
    }

    public DatanodeInfoBuilder setRemaining(long remaining) {
      this.remaining = remaining;
      return this;
    }

    public DatanodeInfoBuilder setBlockPoolUsed(long blockPoolUsed) {
      this.blockPoolUsed = blockPoolUsed;
      return this;
    }

    public DatanodeInfoBuilder setCacheCapacity(long cacheCapacity) {
      this.cacheCapacity = cacheCapacity;
      return this;
    }

    public DatanodeInfoBuilder setCacheUsed(long cacheUsed) {
      this.cacheUsed = cacheUsed;
      return this;
    }

    public DatanodeInfoBuilder setLastUpdate(long lastUpdate) {
      this.lastUpdate = lastUpdate;
      return this;
    }

    public DatanodeInfoBuilder setLastUpdateMonotonic(
        long lastUpdateMonotonic) {
      this.lastUpdateMonotonic = lastUpdateMonotonic;
      return this;
    }

    public DatanodeInfoBuilder setXceiverCount(int xceiverCount) {
      this.xceiverCount = xceiverCount;
      return this;
    }

    public DatanodeInfoBuilder setAdminState(
        DatanodeInfo.AdminStates adminState) {
      this.adminState = adminState;
      return this;
    }

    public DatanodeInfoBuilder setUpgradeDomain(String upgradeDomain) {
      this.upgradeDomain = upgradeDomain;
      return this;
    }

    public DatanodeInfoBuilder setIpAddr(String ipAddr) {
      this.ipAddr = ipAddr;
      return this;
    }

    public DatanodeInfoBuilder setHostName(String hostName) {
      this.hostName = hostName;
      return this;
    }

    public DatanodeInfoBuilder setDatanodeUuid(String datanodeUuid) {
      this.datanodeUuid = datanodeUuid;
      return this;
    }

    public DatanodeInfoBuilder setXferPort(int xferPort) {
      this.xferPort = xferPort;
      return this;
    }

    public DatanodeInfoBuilder setInfoPort(int infoPort) {
      this.infoPort = infoPort;
      return this;
    }

    public DatanodeInfoBuilder setInfoSecurePort(int infoSecurePort) {
      this.infoSecurePort = infoSecurePort;
      return this;
    }

    public DatanodeInfoBuilder setIpcPort(int ipcPort) {
      this.ipcPort = ipcPort;
      return this;
    }

    public DatanodeInfoBuilder setNetworkLocation(String networkLocation) {
      this.location = networkLocation;
      return this;
    }

    public DatanodeInfoBuilder setNonDfsUsed(long nonDfsUsed) {
      this.nonDfsUsed = nonDfsUsed;
      return this;
    }

    public DatanodeInfoBuilder setLastBlockReportTime(long time) {
      this.lastBlockReportTime = time;
      return this;
    }

    public DatanodeInfoBuilder setLastBlockReportMonotonic(long time) {
      this.lastBlockReportMonotonic = time;
      return this;
    }
    public DatanodeInfoBuilder setNumBlocks(int blockCount) {
      this.numBlocks = blockCount;
      return this;
    }

    public DatanodeInfo build() {
      return new DatanodeInfo(ipAddr, hostName, datanodeUuid, xferPort,
          infoPort, infoSecurePort, ipcPort, capacity, dfsUsed, nonDfsUsed,
          remaining, blockPoolUsed, cacheCapacity, cacheUsed, lastUpdate,
          lastUpdateMonotonic, xceiverCount, location, adminState,
          upgradeDomain, lastBlockReportTime, lastBlockReportMonotonic,
          numBlocks);
    }
  }
}
