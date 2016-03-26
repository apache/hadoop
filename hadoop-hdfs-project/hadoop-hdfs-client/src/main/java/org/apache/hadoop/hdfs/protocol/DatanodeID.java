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

import com.google.common.annotations.VisibleForTesting;

import java.net.InetSocketAddress;

/**
 * This class represents the primary identifier for a Datanode.
 * Datanodes are identified by how they can be contacted (hostname
 * and ports) and their storage ID, a unique number that associates
 * the Datanodes blocks with a particular Datanode.
 *
 * {@link DatanodeInfo#getName()} should be used to get the network
 * location (for topology) of a datanode, instead of using
 * {@link DatanodeID#getXferAddr()} here. Helpers are defined below
 * for each context in which a DatanodeID is used.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeID implements Comparable<DatanodeID> {
  public static final DatanodeID[] EMPTY_ARRAY = {};
  public static final DatanodeID EMPTY_DATANODE_ID = new DatanodeID("null",
      "null", "null", 0, 0, 0, 0);

  private String ipAddr;     // IP address
  private String hostName;   // hostname claimed by datanode
  private String peerHostName; // hostname from the actual connection
  private int xferPort;      // data streaming port
  private int infoPort;      // info server port
  private int infoSecurePort; // info server port
  private int ipcPort;       // IPC server port
  private String xferAddr;

  /**
   * UUID identifying a given datanode. For upgraded Datanodes this is the
   * same as the StorageID that was previously used by this Datanode.
   * For newly formatted Datanodes it is a UUID.
   */
  private final String datanodeUuid;

  public DatanodeID(DatanodeID from) {
    this(from.getDatanodeUuid(), from);
  }

  @VisibleForTesting
  public DatanodeID(String datanodeUuid, DatanodeID from) {
    this(from.getIpAddr(),
        from.getHostName(),
        datanodeUuid,
        from.getXferPort(),
        from.getInfoPort(),
        from.getInfoSecurePort(),
        from.getIpcPort());
    this.peerHostName = from.getPeerHostName();
  }

  /**
   * Create a DatanodeID
   * @param ipAddr IP
   * @param hostName hostname
   * @param datanodeUuid data node ID, UUID for new Datanodes, may be the
   *                     storage ID for pre-UUID datanodes. NULL if unknown
   *                     e.g. if this is a new datanode. A new UUID will
   *                     be assigned by the namenode.
   * @param xferPort data transfer port
   * @param infoPort info server port
   * @param ipcPort ipc server port
   */
  public DatanodeID(String ipAddr, String hostName, String datanodeUuid,
      int xferPort, int infoPort, int infoSecurePort, int ipcPort) {
    setIpAndXferPort(ipAddr, xferPort);
    this.hostName = hostName;
    this.datanodeUuid = checkDatanodeUuid(datanodeUuid);
    this.infoPort = infoPort;
    this.infoSecurePort = infoSecurePort;
    this.ipcPort = ipcPort;
  }

  public void setIpAddr(String ipAddr) {
    //updated during registration, preserve former xferPort
    setIpAndXferPort(ipAddr, xferPort);
  }

  private void setIpAndXferPort(String ipAddr, int xferPort) {
    // build xferAddr string to reduce cost of frequent use
    this.ipAddr = ipAddr;
    this.xferPort = xferPort;
    this.xferAddr = ipAddr + ":" + xferPort;
  }

  public void setPeerHostName(String peerHostName) {
    this.peerHostName = peerHostName;
  }

  /**
   * @return data node ID.
   */
  public String getDatanodeUuid() {
    return datanodeUuid;
  }

  private String checkDatanodeUuid(String uuid) {
    if (uuid == null || uuid.isEmpty()) {
      return null;
    } else {
      return uuid;
    }
  }

  /**
   * @return ipAddr;
   */
  public String getIpAddr() {
    return ipAddr;
  }

  /**
   * @return hostname
   */
  public String getHostName() {
    return hostName;
  }

  /**
   * @return hostname from the actual connection
   */
  public String getPeerHostName() {
    return peerHostName;
  }

  /**
   * @return IP:xferPort string
   */
  public String getXferAddr() {
    return xferAddr;
  }

  /**
   * @return IP:ipcPort string
   */
  private String getIpcAddr() {
    return ipAddr + ":" + ipcPort;
  }

  /**
   * @return IP:infoPort string
   */
  public String getInfoAddr() {
    return ipAddr + ":" + infoPort;
  }

  /**
   * @return IP:infoPort string
   */
  public String getInfoSecureAddr() {
    return ipAddr + ":" + infoSecurePort;
  }

  /**
   * @return hostname:xferPort
   */
  public String getXferAddrWithHostname() {
    return hostName + ":" + xferPort;
  }

  /**
   * @return hostname:ipcPort
   */
  private String getIpcAddrWithHostname() {
    return hostName + ":" + ipcPort;
  }

  /**
   * @param useHostname true to use the DN hostname, use the IP otherwise
   * @return name:xferPort
   */
  public String getXferAddr(boolean useHostname) {
    return useHostname ? getXferAddrWithHostname() : getXferAddr();
  }

  /**
   * @param useHostname true to use the DN hostname, use the IP otherwise
   * @return name:ipcPort
   */
  public String getIpcAddr(boolean useHostname) {
    return useHostname ? getIpcAddrWithHostname() : getIpcAddr();
  }

  /**
   * @return xferPort (the port for data streaming)
   */
  public int getXferPort() {
    return xferPort;
  }

  /**
   * @return infoPort (the port at which the HTTP server bound to)
   */
  public int getInfoPort() {
    return infoPort;
  }

  /**
   * @return infoSecurePort (the port at which the HTTPS server bound to)
   */
  public int getInfoSecurePort() {
    return infoSecurePort;
  }

  /**
   * @return ipcPort (the port at which the IPC server bound to)
   */
  public int getIpcPort() {
    return ipcPort;
  }

  @Override
  public boolean equals(Object to) {
    return this == to ||
        (to instanceof DatanodeID &&
            getXferAddr().equals(((DatanodeID) to).getXferAddr()) &&
            datanodeUuid.equals(((DatanodeID) to).getDatanodeUuid()));
  }

  @Override
  public int hashCode() {
    return datanodeUuid.hashCode();
  }

  @Override
  public String toString() {
    return getXferAddr();
  }

  /**
   * Update fields when a new registration request comes in.
   * Note that this does not update storageID.
   */
  public void updateRegInfo(DatanodeID nodeReg) {
    setIpAndXferPort(nodeReg.getIpAddr(), nodeReg.getXferPort());
    hostName = nodeReg.getHostName();
    peerHostName = nodeReg.getPeerHostName();
    infoPort = nodeReg.getInfoPort();
    infoSecurePort = nodeReg.getInfoSecurePort();
    ipcPort = nodeReg.getIpcPort();
  }

  /**
   * Compare based on data transfer address.
   *
   * @param that datanode to compare with
   * @return as specified by Comparable
   */
  @Override
  public int compareTo(DatanodeID that) {
    return getXferAddr().compareTo(that.getXferAddr());
  }

  public InetSocketAddress getResolvedAddress() {
    return new InetSocketAddress(this.getIpAddr(), this.getXferPort());
  }
}
