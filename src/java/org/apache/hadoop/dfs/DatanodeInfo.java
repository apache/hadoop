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
package org.apache.hadoop.dfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

/** 
 * DatanodeInfo represents the status of a DataNode.
 * This object is used for communication in the
 * Datanode Protocol and the Client Protocol.
 */
public class DatanodeInfo extends DatanodeID implements Node {
  protected long capacity;
  protected long dfsUsed;
  protected long remaining;
  protected long lastUpdate;
  protected int xceiverCount;
  protected String location = NetworkTopology.UNRESOLVED;

  /** HostName as suplied by the datanode during registration as its 
   * name. Namenode uses datanode IP address as the name.
   */
  protected String hostName = null;
  
  // administrative states of a datanode
  public enum AdminStates {NORMAL, DECOMMISSION_INPROGRESS, DECOMMISSIONED; }
  protected AdminStates adminState;


  DatanodeInfo() {
    super();
    adminState = null;
  }
  
  DatanodeInfo(DatanodeInfo from) {
    super(from);
    this.capacity = from.getCapacity();
    this.dfsUsed = from.getDfsUsed();
    this.remaining = from.getRemaining();
    this.lastUpdate = from.getLastUpdate();
    this.xceiverCount = from.getXceiverCount();
    this.location = from.getNetworkLocation();
    this.adminState = from.adminState;
    this.hostName = from.hostName;
  }

  DatanodeInfo(DatanodeID nodeID) {
    super(nodeID);
    this.capacity = 0L;
    this.dfsUsed = 0L;
    this.remaining = 0L;
    this.lastUpdate = 0L;
    this.xceiverCount = 0;
    this.adminState = null;    
  }
  
  DatanodeInfo(DatanodeID nodeID, String location, String hostName) {
    this(nodeID);
    this.location = location;
    this.hostName = hostName;
  }
  
  /** The raw capacity. */
  public long getCapacity() { return capacity; }
  
  /** The used space by the data node. */
  public long getDfsUsed() { return dfsUsed; }

  /** The raw free space. */
  public long getRemaining() { return remaining; }

  /** The time when this information was accurate. */
  public long getLastUpdate() { return lastUpdate; }

  /** number of active connections */
  public int getXceiverCount() { return xceiverCount; }

  /** Sets raw capacity. */
  void setCapacity(long capacity) { 
    this.capacity = capacity; 
  }

  /** Sets raw free space. */
  void setRemaining(long remaining) { 
    this.remaining = remaining; 
  }

  /** Sets time when this information was accurate. */
  void setLastUpdate(long lastUpdate) { 
    this.lastUpdate = lastUpdate; 
  }

  /** Sets number of active connections */
  void setXceiverCount(int xceiverCount) { 
    this.xceiverCount = xceiverCount; 
  }

  /** rack name **/
  public synchronized String getNetworkLocation() {return location;}
    
  /** Sets the rack name */
  public synchronized void setNetworkLocation(String location) {
    this.location = NodeBase.normalize(location);
  }
  
  public String getHostName() {
    return (hostName == null || hostName.length()==0) ? getHost() : hostName;
  }
  
  public void setHostName(String host) {
    hostName = host;
  }
  
  /** A formatted string for reporting the status of the DataNode. */
  public String getDatanodeReport() {
    StringBuffer buffer = new StringBuffer();
    long c = getCapacity();
    long r = getRemaining();
    long u = getDfsUsed();
    buffer.append("Name: "+name+"\n");
    if (!NetworkTopology.UNRESOLVED.equals(location) && 
        !NetworkTopology.DEFAULT_RACK.equals(location)) {
      buffer.append("Rack: "+location+"\n");
    }
    if (isDecommissioned()) {
      buffer.append("State          : Decommissioned\n");
    } else if (isDecommissionInProgress()) {
      buffer.append("State          : Decommission in progress\n");
    } else {
      buffer.append("State          : In Service\n");
    }
    buffer.append("Total raw bytes: "+c+" ("+FsShell.byteDesc(c)+")"+"\n");
    buffer.append("Remaining raw bytes: " +r+ "("+FsShell.byteDesc(r)+")"+"\n");
    buffer.append("Used raw bytes: "+u+" ("+FsShell.byteDesc(u)+")"+"\n");
    buffer.append("% used: "+FsShell.limitDecimalTo2(((1.0*u)/c)*100)+"%"+"\n");
    buffer.append("Last contact: "+new Date(lastUpdate)+"\n");
    return buffer.toString();
  }

  /** A formatted string for printing the status of the DataNode. */
  String dumpDatanode() {
    StringBuffer buffer = new StringBuffer();
    long c = getCapacity();
    long r = getRemaining();
    long u = getDfsUsed();
    buffer.append(name);
    if (!NetworkTopology.UNRESOLVED.equals(location) &&
        !NetworkTopology.DEFAULT_RACK.equals(location)) {
      buffer.append(" "+location);
    }
    if (isDecommissioned()) {
      buffer.append(" DD");
    } else if (isDecommissionInProgress()) {
      buffer.append(" DP");
    } else {
      buffer.append(" IN");
    }
    buffer.append(" " + c + "(" + FsShell.byteDesc(c)+")");
    buffer.append(" " + u + "(" + FsShell.byteDesc(u)+")");
    buffer.append(" " + FsShell.limitDecimalTo2(((1.0*u)/c)*100)+"%");
    buffer.append(" " + r + "(" + FsShell.byteDesc(r)+")");
    buffer.append(" " + new Date(lastUpdate));
    return buffer.toString();
  }

  /**
   * Start decommissioning a node.
   * old state.
   */
  void startDecommission() {
    adminState = AdminStates.DECOMMISSION_INPROGRESS;
  }

  /**
   * Stop decommissioning a node.
   * old state.
   */
  void stopDecommission() {
    adminState = null;
  }

  /**
   * Returns true if the node is in the process of being decommissioned
   */
  boolean isDecommissionInProgress() {
    if (adminState == AdminStates.DECOMMISSION_INPROGRESS) {
      return true;
    }
    return false;
  }

  /**
   * Returns true if the node has been decommissioned.
   */
  boolean isDecommissioned() {
    if (adminState == AdminStates.DECOMMISSIONED) {
      return true;
    }
    return false;
  }

  /**
   * Sets the admin state to indicate that decommision is complete.
   */
  void setDecommissioned() {
    adminState = AdminStates.DECOMMISSIONED;
  }

  /**
   * Retrieves the admin state of this node.
   */
  AdminStates getAdminState() {
    if (adminState == null) {
      return AdminStates.NORMAL;
    }
    return adminState;
  }

  /**
   * Sets the admin state of this node.
   */
  void setAdminState(AdminStates newState) {
    if (newState == AdminStates.NORMAL) {
      adminState = null;
    }
    else {
      adminState = newState;
    }
  }

  private int level; //which level of the tree the node resides
  private Node parent; //its parent

  /** Return this node's parent */
  public Node getParent() { return parent; }
  public void setParent(Node parent) {this.parent = parent;}
   
  /** Return this node's level in the tree.
   * E.g. the root of a tree returns 0 and its children return 1
   */
  public int getLevel() { return level; }
  public void setLevel(int level) {this.level = level;}

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (DatanodeInfo.class,
       new WritableFactory() {
         public Writable newInstance() { return new DatanodeInfo(); }
       });
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    super.write(out);

    //TODO: move it to DatanodeID once DatanodeID is not stored in FSImage
    out.writeShort(ipcPort);

    out.writeLong(capacity);
    out.writeLong(dfsUsed);
    out.writeLong(remaining);
    out.writeLong(lastUpdate);
    out.writeInt(xceiverCount);
    Text.writeString(out, location);
    Text.writeString(out, hostName == null? "": hostName);
    WritableUtils.writeEnum(out, getAdminState());
  }

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    //TODO: move it to DatanodeID once DatanodeID is not stored in FSImage
    this.ipcPort = in.readShort() & 0x0000ffff;

    this.capacity = in.readLong();
    this.dfsUsed = in.readLong();
    this.remaining = in.readLong();
    this.lastUpdate = in.readLong();
    this.xceiverCount = in.readInt();
    this.location = Text.readString(in);
    this.hostName = Text.readString(in);
    setAdminState(WritableUtils.readEnum(in, AdminStates.class));
  }
}
