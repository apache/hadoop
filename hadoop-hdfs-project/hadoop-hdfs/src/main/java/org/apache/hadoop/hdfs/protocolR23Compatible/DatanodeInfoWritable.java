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
package org.apache.hadoop.hdfs.protocolR23Compatible;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.HadoopIllegalArgumentException;

import org.apache.avro.reflect.Nullable;

/** 
 * DatanodeInfo represents the status of a DataNode.
 * This object is used for communication in the
 * Datanode Protocol and the Client Protocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class DatanodeInfoWritable extends DatanodeIDWritable  {
  protected long capacity;
  protected long dfsUsed;
  protected long remaining;
  protected long blockPoolUsed;
  protected long lastUpdate;
  protected int xceiverCount;
  protected String location = NetworkTopology.DEFAULT_RACK;

  /** HostName as supplied by the datanode during registration as its 
   * name. Namenode uses datanode IP address as the name.
   */
  @Nullable
  protected String hostName = null;
  
  // administrative states of a datanode
  public enum AdminStates {
    NORMAL(DatanodeInfo.AdminStates.NORMAL.toString()), 
    DECOMMISSION_INPROGRESS(DatanodeInfo.AdminStates.DECOMMISSION_INPROGRESS.toString()), 
    DECOMMISSIONED(DatanodeInfo.AdminStates.DECOMMISSIONED.toString());

    final String value;

    AdminStates(final String v) {
      this.value = v;
    }

    public String toString() {
      return value;
    }
    
    public static AdminStates fromValue(final String value) {
      for (AdminStates as : AdminStates.values()) {
        if (as.value.equals(value)) return as;
      }
      throw new HadoopIllegalArgumentException("Unknown Admin State" + value);
    }
  }

  @Nullable
  protected AdminStates adminState;
  
  static public DatanodeInfo convertDatanodeInfo(DatanodeInfoWritable di) {
    if (di == null) return null;
    return new DatanodeInfo(
        new org.apache.hadoop.hdfs.protocol.DatanodeID(di.getName(), di.getStorageID(), di.getInfoPort(), di.getIpcPort()),
        di.getNetworkLocation(), di.getHostName(),
         di.getCapacity(),  di.getDfsUsed(),  di.getRemaining(),
        di.getBlockPoolUsed()  ,  di.getLastUpdate() , di.getXceiverCount() ,
        DatanodeInfo.AdminStates.fromValue(di.getAdminState().value)); 
  }
  
  
  static public DatanodeInfo[] convertDatanodeInfo(DatanodeInfoWritable di[]) {
    if (di == null) return null;
    DatanodeInfo[] result = new DatanodeInfo[di.length];
    for (int i = 0; i < di.length; i++) {
      result[i] = convertDatanodeInfo(di[i]);
    }    
    return result;
  }
  
  static public DatanodeInfoWritable[] convertDatanodeInfo(DatanodeInfo[] di) {
    if (di == null) return null;
    DatanodeInfoWritable[] result = new DatanodeInfoWritable[di.length];
    for (int i = 0; i < di.length; i++) {
      result[i] = new DatanodeInfoWritable(new DatanodeIDWritable(di[i].getName(), di[i].getStorageID(), di[i].getInfoPort(), di[i].getIpcPort()),
          di[i].getNetworkLocation(), di[i].getHostName(),
          di[i].getCapacity(),  di[i].getDfsUsed(),  di[i].getRemaining(),
          di[i].getBlockPoolUsed()  ,  di[i].getLastUpdate() , di[i].getXceiverCount() ,
          AdminStates.fromValue(di[i].getAdminState().toString()));
    }    
    return result;
  }
  
  static public DatanodeInfoWritable convertDatanodeInfo(DatanodeInfo di) {
    if (di == null) return null;
    return new DatanodeInfoWritable(new DatanodeIDWritable(di.getName(),
        di.getStorageID(), di.getInfoPort(), di.getIpcPort()),
        di.getNetworkLocation(), di.getHostName(), di.getCapacity(),
        di.getDfsUsed(), di.getRemaining(), di.getBlockPoolUsed(),
        di.getLastUpdate(), di.getXceiverCount(), 
        AdminStates.fromValue(di.getAdminState().toString()));
  }

  public DatanodeInfoWritable() {
    super();
    adminState = null;
  }
  
  public DatanodeInfoWritable(DatanodeInfoWritable from) {
    super(from);
    this.capacity = from.getCapacity();
    this.dfsUsed = from.getDfsUsed();
    this.remaining = from.getRemaining();
    this.blockPoolUsed = from.getBlockPoolUsed();
    this.lastUpdate = from.getLastUpdate();
    this.xceiverCount = from.getXceiverCount();
    this.location = from.getNetworkLocation();
    this.adminState = from.adminState;
    this.hostName = from.hostName;
  }

  public DatanodeInfoWritable(DatanodeIDWritable nodeID) {
    super(nodeID);
    this.capacity = 0L;
    this.dfsUsed = 0L;
    this.remaining = 0L;
    this.blockPoolUsed = 0L;
    this.lastUpdate = 0L;
    this.xceiverCount = 0;
    this.adminState = null;    
  }
  
  protected DatanodeInfoWritable(DatanodeIDWritable nodeID, String location, String hostName) {
    this(nodeID);
    this.location = location;
    this.hostName = hostName;
  }
  
  public DatanodeInfoWritable(DatanodeIDWritable nodeID, String location, String hostName,
      final long capacity, final long dfsUsed, final long remaining,
      final long blockPoolUsed, final long lastUpdate, final int xceiverCount,
      final AdminStates adminState) {
    this(nodeID, location, hostName);
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.blockPoolUsed = blockPoolUsed;
    this.lastUpdate = lastUpdate;
    this.xceiverCount = xceiverCount;
    this.adminState = adminState;
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
  public String getNetworkLocation() {return location;}
    
  /** Sets the rack name */
  public void setNetworkLocation(String location) {
    this.location = NodeBase.normalize(location);
  }
  
  public String getHostName() {
    return (hostName == null || hostName.length()==0) ? getHost() : hostName;
  }
  
  public void setHostName(String host) {
    hostName = host;
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

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (DatanodeInfoWritable.class,
       new WritableFactory() {
         public Writable newInstance() { return new DatanodeInfoWritable(); }
       });
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    out.writeShort(ipcPort);

    out.writeLong(capacity);
    out.writeLong(dfsUsed);
    out.writeLong(remaining);
    out.writeLong(blockPoolUsed);
    out.writeLong(lastUpdate);
    out.writeInt(xceiverCount);
    Text.writeString(out, location);
    Text.writeString(out, hostName == null? "" : hostName);
    WritableUtils.writeEnum(out, getAdminState());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    this.ipcPort = in.readShort() & 0x0000ffff;

    this.capacity = in.readLong();
    this.dfsUsed = in.readLong();
    this.remaining = in.readLong();
    this.blockPoolUsed = in.readLong();
    this.lastUpdate = in.readLong();
    this.xceiverCount = in.readInt();
    this.location = Text.readString(in);
    this.hostName = Text.readString(in);
    setAdminState(WritableUtils.readEnum(in, AdminStates.class));
  }

  /** Read a DatanodeInfo */
  public static DatanodeInfoWritable read(DataInput in) throws IOException {
    final DatanodeInfoWritable d = new DatanodeInfoWritable();
    d.readFields(in);
    return d;
  }
}
