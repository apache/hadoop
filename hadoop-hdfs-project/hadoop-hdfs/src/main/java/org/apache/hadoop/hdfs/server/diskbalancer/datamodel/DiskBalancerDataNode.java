/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer.datamodel;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * DiskBalancerDataNode represents a DataNode that exists in the cluster. It
 * also contains a metric called nodeDataDensity which allows us to compare
 * between a set of Nodes.
 */
public class DiskBalancerDataNode implements Comparable<DiskBalancerDataNode> {
  private double nodeDataDensity;
  private Map<String, DiskBalancerVolumeSet> volumeSets;
  private String dataNodeUUID;
  private String dataNodeIP;
  private int dataNodePort;
  private String dataNodeName;
  private int volumeCount;

  /**
   * Constructs an Empty Data Node.
   */
  public DiskBalancerDataNode() {
  }

  /**
   * Constructs a DataNode.
   *
   * @param dataNodeID - Node ID
   */
  public DiskBalancerDataNode(String dataNodeID) {
    this.dataNodeUUID = dataNodeID;
    volumeSets = new HashMap<>();
  }

  /**
   * Returns the IP address of this Node.
   *
   * @return IP Address string
   */
  public String getDataNodeIP() {
    return dataNodeIP;
  }

  /**
   * Sets the IP address of this Node.
   *
   * @param ipaddress - IP Address
   */
  public void setDataNodeIP(String ipaddress) {
    this.dataNodeIP = ipaddress;
  }

  /**
   * Returns the Port of this DataNode.
   *
   * @return Port Number
   */
  public int getDataNodePort() {
    return dataNodePort;
  }

  /**
   * Sets the DataNode Port number.
   *
   * @param port - Datanode Port Number
   */
  public void setDataNodePort(int port) {
    this.dataNodePort = port;
  }

  /**
   * Get DataNode DNS name.
   *
   * @return name of the node
   */
  public String getDataNodeName() {
    return dataNodeName;
  }

  /**
   * Sets node's DNS name.
   *
   * @param name - Data node name
   */
  public void setDataNodeName(String name) {
    this.dataNodeName = name;
  }

  /**
   * Returns the Volume sets on this node.
   *
   * @return a Map of VolumeSets
   */
  public Map<String, DiskBalancerVolumeSet> getVolumeSets() {
    return volumeSets;
  }

  /**
   * Returns datanode ID.
   **/
  public String getDataNodeUUID() {
    return dataNodeUUID;
  }

  /**
   * Sets Datanode UUID.
   *
   * @param nodeID - Node ID.
   */
  public void setDataNodeUUID(String nodeID) {
    this.dataNodeUUID = nodeID;
  }

  /**
   * Indicates whether some other object is "equal to" this one.
   */
  @Override
  public boolean equals(Object obj) {
    if ((obj == null) || (obj.getClass() != getClass())) {
      return false;
    }
    DiskBalancerDataNode that = (DiskBalancerDataNode) obj;
    return dataNodeUUID.equals(that.getDataNodeUUID());
  }

  /**
   * Compares this object with the specified object for order.  Returns a
   * negative integer, zero, or a positive integer as this object is less than,
   * equal to, or greater than the specified object.
   *
   * @param that the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object is
   * less than, equal to, or greater than the specified object.
   * @throws NullPointerException if the specified object is null
   * @throws ClassCastException   if the specified object's type prevents it
   *                              from being compared to this object.
   */
  @Override
  public int compareTo(DiskBalancerDataNode that) {
    Preconditions.checkNotNull(that);

    if (Double.compare(this.nodeDataDensity - that.getNodeDataDensity(), 0)
        < 0) {
      return -1;
    }

    if (Double.compare(this.nodeDataDensity - that.getNodeDataDensity(), 0)
        == 0) {
      return 0;
    }

    if (Double.compare(this.nodeDataDensity - that.getNodeDataDensity(), 0)
        > 0) {
      return 1;
    }
    return 0;
  }

  /**
   * Returns a hash code value for the object. This method is supported for the
   * benefit of hash tables such as those provided by {@link HashMap}.
   */
  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /**
   * Returns NodeDataDensity Metric.
   *
   * @return float
   */
  public double getNodeDataDensity() {
    return nodeDataDensity;
  }

  /**
   * Computes nodes data density.
   *
   * This metric allows us to compare different  nodes and how well the data is
   * spread across a set of volumes inside the node.
   */
  public void computeNodeDensity() {
    double sum = 0;
    int volcount = 0;
    for (DiskBalancerVolumeSet vset : volumeSets.values()) {
      for (DiskBalancerVolume vol : vset.getVolumes()) {
        sum += Math.abs(vol.getVolumeDataDensity());
        volcount++;
      }
    }
    nodeDataDensity = sum;
    this.volumeCount = volcount;

  }

  /**
   * Computes if this node needs balancing at all.
   *
   * @param threshold - Percentage
   * @return true or false
   */
  public boolean isBalancingNeeded(double threshold) {
    for (DiskBalancerVolumeSet vSet : getVolumeSets().values()) {
      if (vSet.isBalancingNeeded(threshold)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Adds a volume to the DataNode.
   *
   * It is assumed that we have one thread per node hence this call is not
   * synchronised neither is the map is protected.
   *
   * @param volume - volume
   */
  public void addVolume(DiskBalancerVolume volume) throws Exception {
    Preconditions.checkNotNull(volume, "volume cannot be null");
    Preconditions.checkNotNull(volumeSets, "volume sets cannot be null");
    Preconditions
        .checkNotNull(volume.getStorageType(), "storage type cannot be null");

    String volumeSetKey = volume.getStorageType();
    DiskBalancerVolumeSet vSet;
    if (volumeSets.containsKey(volumeSetKey)) {
      vSet = volumeSets.get(volumeSetKey);
    } else {
      vSet = new DiskBalancerVolumeSet(volume.isTransient());
      vSet.setStorageType(volumeSetKey);
      volumeSets.put(volumeSetKey, vSet);
    }

    vSet.addVolume(volume);
    computeNodeDensity();
  }

  /**
   * Returns how many volumes are in the DataNode.
   *
   * @return int
   */
  public int getVolumeCount() {
    return volumeCount;
  }


}
