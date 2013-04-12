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

import static org.apache.hadoop.hdfs.DFSUtil.percent2String;

import java.util.Date;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

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
  private long remaining;
  private long blockPoolUsed;
  private long lastUpdate;
  private int xceiverCount;
  private String location = NetworkTopology.DEFAULT_RACK;
  
  // Datanode administrative states
  public enum AdminStates {
    NORMAL("In Service"), 
    DECOMMISSION_INPROGRESS("Decommission In Progress"), 
    DECOMMISSIONED("Decommissioned");

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

  public DatanodeInfo(DatanodeInfo from) {
    super(from);
    this.capacity = from.getCapacity();
    this.dfsUsed = from.getDfsUsed();
    this.remaining = from.getRemaining();
    this.blockPoolUsed = from.getBlockPoolUsed();
    this.lastUpdate = from.getLastUpdate();
    this.xceiverCount = from.getXceiverCount();
    this.location = from.getNetworkLocation();
    this.adminState = from.getAdminState();
  }

  public DatanodeInfo(DatanodeID nodeID) {
    super(nodeID);
    this.capacity = 0L;
    this.dfsUsed = 0L;
    this.remaining = 0L;
    this.blockPoolUsed = 0L;
    this.lastUpdate = 0L;
    this.xceiverCount = 0;
    this.adminState = null;    
  }
  
  public DatanodeInfo(DatanodeID nodeID, String location) {
    this(nodeID);
    this.location = location;
  }
  
  public DatanodeInfo(DatanodeID nodeID, String location,
      final long capacity, final long dfsUsed, final long remaining,
      final long blockPoolUsed, final long lastUpdate, final int xceiverCount,
      final AdminStates adminState) {
    this(nodeID.getIpAddr(), nodeID.getHostName(), nodeID.getStorageID(), nodeID.getXferPort(),
        nodeID.getInfoPort(), nodeID.getIpcPort(), capacity, dfsUsed, remaining,
        blockPoolUsed, lastUpdate, xceiverCount, location, adminState);
  }

  /** Constructor */
  public DatanodeInfo(final String ipAddr, final String hostName,
      final String storageID, final int xferPort, final int infoPort, final int ipcPort,
      final long capacity, final long dfsUsed, final long remaining,
      final long blockPoolUsed, final long lastUpdate, final int xceiverCount,
      final String networkLocation, final AdminStates adminState) {
    super(ipAddr, hostName, storageID, xferPort, infoPort, ipcPort);
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.blockPoolUsed = blockPoolUsed;
    this.lastUpdate = lastUpdate;
    this.xceiverCount = xceiverCount;
    this.location = networkLocation;
    this.adminState = adminState;
  }
  
  /** Network location name */
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
    long nonDFSUsed = capacity - dfsUsed - remaining;
    return nonDFSUsed < 0 ? 0 : nonDFSUsed;
  }

  /** The used space by the data node as percentage of present capacity */
  public float getDfsUsedPercent() { 
    return DFSUtil.getPercentUsed(dfsUsed, capacity);
  }

  /** The raw free space. */
  public long getRemaining() { return remaining; }

  /** Used space by the block pool as percentage of present capacity */
  public float getBlockPoolUsedPercent() {
    return DFSUtil.getPercentUsed(blockPoolUsed, capacity);
  }
  
  /** The remaining space as percentage of configured capacity. */
  public float getRemainingPercent() { 
    return DFSUtil.getPercentRemaining(remaining, capacity);
  }

  /** The time when this information was accurate. */
  public long getLastUpdate() { return lastUpdate; }

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

  /** Sets raw free space. */
  public void setRemaining(long remaining) { 
    this.remaining = remaining; 
  }

  /** Sets block pool used space */
  public void setBlockPoolUsed(long bpUsed) { 
    this.blockPoolUsed = bpUsed; 
  }

  /** Sets time when this information was accurate. */
  public void setLastUpdate(long lastUpdate) { 
    this.lastUpdate = lastUpdate; 
  }

  /** Sets number of active connections */
  public void setXceiverCount(int xceiverCount) { 
    this.xceiverCount = xceiverCount; 
  }

  /** rack name */
  public synchronized String getNetworkLocation() {return location;}
    
  /** Sets the rack name */
  public synchronized void setNetworkLocation(String location) {
    this.location = NodeBase.normalize(location);
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
    String lookupName = NetUtils.getHostNameOfIP(getName());

    buffer.append("Name: "+ getName());
    if (lookupName != null) {
      buffer.append(" (" + lookupName + ")");
    }
    buffer.append("\n");
    buffer.append("Hostname: " + getHostName() + "\n");

    if (!NetworkTopology.DEFAULT_RACK.equals(location)) {
      buffer.append("Rack: "+location+"\n");
    }
    buffer.append("Decommission Status : ");
    if (isDecommissioned()) {
      buffer.append("Decommissioned\n");
    } else if (isDecommissionInProgress()) {
      buffer.append("Decommission in progress\n");
    } else {
      buffer.append("Normal\n");
    }
    buffer.append("Configured Capacity: "+c+" ("+StringUtils.byteDesc(c)+")"+"\n");
    buffer.append("DFS Used: "+u+" ("+StringUtils.byteDesc(u)+")"+"\n");
    buffer.append("Non DFS Used: "+nonDFSUsed+" ("+StringUtils.byteDesc(nonDFSUsed)+")"+"\n");
    buffer.append("DFS Remaining: " +r+ " ("+StringUtils.byteDesc(r)+")"+"\n");
    buffer.append("DFS Used%: "+percent2String(usedPercent) + "\n");
    buffer.append("DFS Remaining%: "+percent2String(remainingPercent) + "\n");
    buffer.append("Last contact: "+new Date(lastUpdate)+"\n");
    return buffer.toString();
  }

  /** A formatted string for printing the status of the DataNode. */
  public String dumpDatanode() {
    StringBuilder buffer = new StringBuilder();
    long c = getCapacity();
    long r = getRemaining();
    long u = getDfsUsed();
    buffer.append(getName());
    if (!NetworkTopology.DEFAULT_RACK.equals(location)) {
      buffer.append(" "+location);
    }
    if (isDecommissioned()) {
      buffer.append(" DD");
    } else if (isDecommissionInProgress()) {
      buffer.append(" DP");
    } else {
      buffer.append(" IN");
    }
    buffer.append(" " + c + "(" + StringUtils.byteDesc(c)+")");
    buffer.append(" " + u + "(" + StringUtils.byteDesc(u)+")");
    buffer.append(" " + percent2String(u/(double)c));
    buffer.append(" " + r + "(" + StringUtils.byteDesc(r)+")");
    buffer.append(" " + new Date(lastUpdate));
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
   * datanode for more than staleInterval (default value is
   * {@link DFSConfigKeys#DFS_NAMENODE_STALE_DATANODE_INTERVAL_MILLI_DEFAULT}),
   * the datanode will be treated as stale node.
   * 
   * @param staleInterval
   *          the time interval for marking the node as stale. If the last
   *          update time is beyond the given time interval, the node will be
   *          marked as stale.
   * @return true if the node is stale
   */
  public boolean isStale(long staleInterval) {
    return (Time.now() - lastUpdate) >= staleInterval;
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
}
