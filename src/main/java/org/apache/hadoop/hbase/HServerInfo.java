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
 * HServerInfo is meta info about an {@link HRegionServer}.
 * Holds hostname, ports, regionserver startcode, and load.  Each server has
 * a <code>servername</code> where servername is made up of a concatenation of
 * hostname, port, and regionserver startcode.
 */
public class HServerInfo implements WritableComparable<HServerInfo> {
  /**
   * This character is used as separator making up the <code>servername</code>.
   * Servername is made of host, port, and startcode formatted as
   * <code>&lt;hostname> '{@link #SERVERNAME_SEPARATOR}' &lt;port> '{@ink #SEPARATOR"}' &lt;startcode></code>
   * where {@link SEPARATOR is usually a ','.
   */
  public static final String SERVERNAME_SEPARATOR = ",";

  private HServerAddress serverAddress;
  private long startCode;
  private HServerLoad load;
  private int infoPort;
  // Servername is made of hostname, port and startcode.
  private String serverName = null;
  // Hostname of the regionserver.
  private String hostname;
  private static Map<String,String> dnsCache = new HashMap<String,String>();

  public HServerInfo() {
    this(new HServerAddress(), 0,
        HConstants.DEFAULT_REGIONSERVER_INFOPORT, "default name");
  }

  public HServerInfo(HServerAddress serverAddress, long startCode,
      final int infoPort, String hostname) {
    this.serverAddress = serverAddress;
    this.startCode = startCode;
    this.load = new HServerLoad();
    this.infoPort = infoPort;
    this.hostname = hostname;
  }

  /**
   * Copy-constructor
   * @param other
   */
  public HServerInfo(HServerInfo other) {
    this.serverAddress = new HServerAddress(other.getServerAddress());
    this.startCode = other.getStartCode();
    this.load = other.getLoad();
    this.infoPort = other.getInfoPort();
    this.hostname = other.hostname;
  }

  public HServerLoad getLoad() {
    return load;
  }

  public void setLoad(HServerLoad load) {
    this.load = load;
  }

  public synchronized HServerAddress getServerAddress() {
    return new HServerAddress(serverAddress);
  }

  public synchronized void setServerAddress(HServerAddress serverAddress) {
    this.serverAddress = serverAddress;
    this.serverName = null;
  }

  public synchronized long getStartCode() {
    return startCode;
  }

  public int getInfoPort() {
    return this.infoPort;
  }

  public void setInfoPort(int infoPort) {
    this.infoPort = infoPort;
  }

  public synchronized void setStartCode(long startCode) {
    this.startCode = startCode;
    this.serverName = null;
  }

  /**
   * @return Server name made of the concatenation of hostname, port and
   * startcode formatted as <code>&lt;hostname> ',' &lt;port> ',' &lt;startcode></code>
   */
  public synchronized String getServerName() {
    if (this.serverName == null) {
      // if we have the hostname of the RS, use it
      if(this.hostname != null) {
        this.serverName =
          getServerName(this.hostname, this.serverAddress.getPort(), this.startCode);
      }
      // go to DNS name resolution only if we dont have the name of the RS
      else {
      this.serverName = getServerName(this.serverAddress, this.startCode);
    }
    }
    return this.serverName;
  }

  /**
   * @param serverAddress In form <code>&lt;hostname> ':' &lt;port></code>
   * @param startCode Server startcode
   * @return Server name made of the concatenation of hostname, port and
   * startcode formatted as <code>&lt;hostname> ',' &lt;port> ',' &lt;startcode></code>
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
   * @param address Server address
   * @param startCode Server startcode
   * @return Server name made of the concatenation of hostname, port and
   * startcode formatted as <code>&lt;hostname> ',' &lt;port> ',' &lt;startcode></code>
   */
  public static String getServerName(HServerAddress address, long startCode) {
    return getServerName(address.getHostname(), address.getPort(), startCode);
  }

  /*
   * @param hostName
   * @param port
   * @param startCode
   * @return Server name made of the concatenation of hostname, port and
   * startcode formatted as <code>&lt;hostname> ',' &lt;port> ',' &lt;startcode></code>
   */
  private static String getServerName(String hostName, int port, long startCode) {
    StringBuilder name = new StringBuilder(hostName);
    name.append(SERVERNAME_SEPARATOR);
    name.append(port);
    name.append(SERVERNAME_SEPARATOR);
    name.append(startCode);
    return name.toString();
  }

  /**
   * @return ServerName and load concatenated.
   * @see #getServerName()
   * @see #getLoad()
   */
  @Override
  public String toString() {
    return "serverName=" + getServerName() +
      ", load=(" + this.load.toString() + ")";
  }

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

  @Override
  public int hashCode() {
    return this.getServerName().hashCode();
  }

  public void readFields(DataInput in) throws IOException {
    this.serverAddress.readFields(in);
    this.startCode = in.readLong();
    this.load.readFields(in);
    this.infoPort = in.readInt();
    this.hostname = in.readUTF();
  }

  public void write(DataOutput out) throws IOException {
    this.serverAddress.write(out);
    out.writeLong(this.startCode);
    this.load.write(out);
    out.writeInt(this.infoPort);
    out.writeUTF(hostname);
  }

  public int compareTo(HServerInfo o) {
    return this.getServerName().compareTo(o.getServerName());
  }
}
