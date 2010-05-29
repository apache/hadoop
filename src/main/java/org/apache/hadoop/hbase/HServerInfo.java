/**
 * Copyright 2010 The Apache Software Foundation
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
import java.net.InetSocketAddress;
import java.util.Set;

import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


/**
 * HServerInfo is meta info about an {@link HRegionServer}.  It is the token
 * by which a master distingushes a particular regionserver from the rest.
 * It holds hostname, ports, regionserver startcode, and load.  Each server has
 * a <code>servername</code> where servername is made up of a concatenation of
 * hostname, port, and regionserver startcode.  This servername is used in
 * various places identifying this regionserver.  Its even used as part of
 * a pathname in the filesystem.  As part of the initialization,
 * master will pass the regionserver the address that it knows this regionserver
 * by.  In subsequent communications, the regionserver will pass a HServerInfo
 * with the master-supplied address.
 */
public class HServerInfo implements WritableComparable<HServerInfo> {
  /*
   * This character is used as separator between server hostname and port and
   * its startcode. Servername is formatted as
   * <code>&lt;hostname> '{@ink #SERVERNAME_SEPARATOR"}' &lt;port> '{@ink #SERVERNAME_SEPARATOR"}' &lt;startcode></code>.
   */
  private static final String SERVERNAME_SEPARATOR = ",";

  private HServerAddress serverAddress;
  private long startCode;
  private HServerLoad load;
  private int infoPort;
  // Servername is made of hostname, port and startcode.
  private String serverName = null;
  // Hostname of the regionserver.
  private String hostname;
  private String cachedHostnamePort = null;

  public HServerInfo() {
    this(new HServerAddress(), 0, HConstants.DEFAULT_REGIONSERVER_INFOPORT,
      "default name");
  }

  /**
   * Constructor that creates a HServerInfo with a generated startcode and an
   * empty load.
   * @param serverAddress An {@link InetSocketAddress} encased in a {@link Writable}
   * @param infoPort Port the webui runs on.
   * @param hostname Server hostname.
   */
  public HServerInfo(HServerAddress serverAddress, final int infoPort,
      final String hostname) {
    this(serverAddress, System.currentTimeMillis(), infoPort, hostname);
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

  public String getHostname() {
    return this.hostname;
  }

  /**
   * @return The hostname and port concatenated with a ':' as separator.
   */
  public synchronized String getHostnamePort() {
    if (this.cachedHostnamePort == null) {
      this.cachedHostnamePort = getHostnamePort(this.hostname, this.serverAddress.getPort());
    }
    return this.cachedHostnamePort;
  }

  /**
   * @param hostname
   * @param port
   * @return The hostname and port concatenated with a ':' as separator.
   */
  public static String getHostnamePort(final String hostname, final int port) {
    return hostname + ":" + port;
  }

  /**
   * @return Server name made of the concatenation of hostname, port and
   * startcode formatted as <code>&lt;hostname> ',' &lt;port> ',' &lt;startcode></code>
   */
  public synchronized String getServerName() {
    if (this.serverName == null) {
      this.serverName = getServerName(this.hostname,
        this.serverAddress.getPort(), this.startCode);
    }
    return this.serverName;
  }

  public static synchronized String getServerName(final String hostAndPort,
      final long startcode) {
    int index = hostAndPort.indexOf(":");
    if (index <= 0) throw new IllegalArgumentException("Expected <hostname> ':' <port>");
    return getServerName(hostAndPort.substring(0, index),
      Integer.parseInt(hostAndPort.substring(index + 1)), startcode);
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
  public static String getServerName(String hostName, int port, long startCode) {
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

  /**
   * Utility method that does a find of a servername or a hostandport combination
   * in the passed Set.
   * @param servers Set of server names
   * @param serverName Name to look for
   * @param hostAndPortOnly If <code>serverName</code> is a
   * <code>hostname ':' port</code>
   * or <code>hostname , port , startcode</code>.
   * @return True if <code>serverName</code> found in <code>servers</code>
   */
  public static boolean isServer(final Set<String> servers,
      final String serverName, final boolean hostAndPortOnly) {
    if (!hostAndPortOnly) return servers.contains(serverName);
    String serverNameColonReplaced =
      serverName.replaceFirst(":", SERVERNAME_SEPARATOR);
    for (String hostPortStartCode: servers) {
      int index = hostPortStartCode.lastIndexOf(SERVERNAME_SEPARATOR);
      String hostPortStrippedOfStartCode = hostPortStartCode.substring(0, index);
      if (hostPortStrippedOfStartCode.equals(serverNameColonReplaced)) return true;
    }
    return false;
  }
}
