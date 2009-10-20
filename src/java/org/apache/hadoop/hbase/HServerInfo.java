/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.WritableComparable;


/**
 * HServerInfo contains metainfo about an HRegionServer, Currently it only
 * contains the server start code.
 * 
 * In the future it will contain information about the source machine and
 * load statistics.
 */
public class HServerInfo implements WritableComparable<HServerInfo> {
  private HServerAddress serverAddress;
  private long startCode;
  private HServerLoad load;
  private int infoPort;
  private String serverName = null;
  private String name;
  private static Map<String,String> dnsCache = new HashMap<String,String>();

  /** default constructor - used by Writable */
  public HServerInfo() {
    this(new HServerAddress(), 0, 
        HConstants.DEFAULT_REGIONSERVER_INFOPORT, "default name");
  }
  
  /**
   * Constructor
   * @param serverAddress
   * @param startCode
   * @param infoPort Port the info server is listening on.
   */
  public HServerInfo(HServerAddress serverAddress, long startCode,
      final int infoPort, String name) {
    this.serverAddress = serverAddress;
    this.startCode = startCode;
    this.load = new HServerLoad();
    this.infoPort = infoPort;
    this.name = name;
  }
  
  /**
   * Construct a new object using another as input (like a copy constructor)
   * @param other
   */
  public HServerInfo(HServerInfo other) {
    this.serverAddress = new HServerAddress(other.getServerAddress());
    this.startCode = other.getStartCode();
    this.load = other.getLoad();
    this.infoPort = other.getInfoPort();
    this.name = other.getName();
  }

  /**
   * @return the load
   */
  public HServerLoad getLoad() {
    return load;
  }

  /**
   * @param load the load to set
   */
  public void setLoad(HServerLoad load) {
    this.load = load;
  }

  /** @return the server address */
  public synchronized HServerAddress getServerAddress() {
    return new HServerAddress(serverAddress);
  }
  
  /**
   * Change the server address.
   * @param serverAddress New server address
   */
  public synchronized void setServerAddress(HServerAddress serverAddress) {
    this.serverAddress = serverAddress;
    this.serverName = null;
  }
 
  /** @return the server start code */
  public synchronized long getStartCode() {
    return startCode;
  }
  
  /**
   * @return Port the info server is listening on.
   */
  public int getInfoPort() {
    return this.infoPort;
  }
  
  /**
   * @param infoPort - new port of info server
   */
  public void setInfoPort(int infoPort) {
    this.infoPort = infoPort;
  }
  
  /**
   * @param startCode the startCode to set
   */
  public synchronized void setStartCode(long startCode) {
    this.startCode = startCode;
    this.serverName = null;
  }
  
  /**
   * @return the server name in the form hostname_startcode_port
   */
  public synchronized String getServerName() {
    if (this.serverName == null) {
      this.serverName = getServerName(this.serverAddress, this.startCode);
    }
    return this.serverName;
  }
  
  /**
   * Get the hostname of the server
   * @return hostname
   */
  public String getName() {
    return name;
  }
 
  /**
   * Set the hostname of the server
   * @param name hostname
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "address: " + this.serverAddress + ", startcode: " + this.startCode
    + ", load: (" + this.load.toString() + ")";
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    return compareTo((HServerInfo)obj) == 0;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return this.getServerName().hashCode();
  }


  // Writable
  
  public void readFields(DataInput in) throws IOException {
    this.serverAddress.readFields(in);
    this.startCode = in.readLong();
    this.load.readFields(in);
    this.infoPort = in.readInt();
    this.name = in.readUTF();
  }

  public void write(DataOutput out) throws IOException {
    this.serverAddress.write(out);
    out.writeLong(this.startCode);
    this.load.write(out);
    out.writeInt(this.infoPort);
    out.writeUTF(name);
  }

  public int compareTo(HServerInfo o) {
    return this.getServerName().compareTo(o.getServerName());
  }

  /**
   * @param info
   * @return the server name in the form hostname_startcode_port
   */
  public static String getServerName(HServerInfo info) {
    return getServerName(info.getServerAddress(), info.getStartCode());
  }
  
  /**
   * @param serverAddress in the form hostname:port
   * @param startCode
   * @return the server name in the form hostname_startcode_port
   */
  public static String getServerName(String serverAddress, long startCode) {
    String name = null;
    if (serverAddress != null) {
      int colonIndex = serverAddress.lastIndexOf(':');
      if(colonIndex < 0) {
        throw new IllegalArgumentException("Not a host:port pair: " + serverAddress);
      }
      String host = serverAddress.substring(0, colonIndex);
      int port =
        Integer.valueOf(serverAddress.substring(colonIndex + 1)).intValue();
      if(!dnsCache.containsKey(host)) {
        HServerAddress address = new HServerAddress(serverAddress);
        dnsCache.put(host, address.getHostname());
      }
      host = dnsCache.get(host);
      name = getServerName(host, port, startCode);
    }
    return name;
  }

  /**
   * @param address
   * @param startCode
   * @return the server name in the form hostname_startcode_port
   */
  public static String getServerName(HServerAddress address, long startCode) {
    return getServerName(address.getHostname(), address.getPort(), startCode);
  }

  private static String getServerName(String hostName, int port, long startCode) {
    StringBuilder name = new StringBuilder(hostName);
    name.append(",");
    name.append(port);
    name.append(",");
    name.append(startCode);
    return name.toString();
  }
}
