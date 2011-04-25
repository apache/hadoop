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

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.InetAddress;

/**
 * HServerAddress is a "label" for a HBase server made of host and port number.
 */
public class HServerAddress implements WritableComparable<HServerAddress> {
  private InetSocketAddress address;
  String stringValue;

  public HServerAddress() {
    this.address = null;
    this.stringValue = null;
  }

  /**
   * Construct an instance from an {@link InetSocketAddress}.
   * @param address InetSocketAddress of server
   */
  public HServerAddress(InetSocketAddress address) {
    this.address = address;
    this.stringValue = address.getAddress().getHostName() + ":" +
      address.getPort();
    checkBindAddressCanBeResolved();
  }

  /**
   * @param hostAndPort Hostname and port formatted as <code>&lt;hostname> ':' &lt;port></code>
   */
  public HServerAddress(String hostAndPort) {
    int colonIndex = hostAndPort.lastIndexOf(':');
    if (colonIndex < 0) {
      throw new IllegalArgumentException("Not a host:port pair: " + hostAndPort);
    }
    String host = hostAndPort.substring(0, colonIndex);
    int port = Integer.parseInt(hostAndPort.substring(colonIndex + 1));
    this.address = new InetSocketAddress(host, port);
    this.stringValue = address.getHostName() + ":" + port;
    checkBindAddressCanBeResolved();
  }

  /**
   * @param bindAddress Hostname
   * @param port Port number
   */
  public HServerAddress(String bindAddress, int port) {
    this.address = new InetSocketAddress(bindAddress, port);
    this.stringValue = address.getHostName() + ":" + port;
    checkBindAddressCanBeResolved();
  }

  /**
   * Copy-constructor.
   * @param other HServerAddress to copy from
   */
  public HServerAddress(HServerAddress other) {
    String bindAddress = other.getBindAddress();
    int port = other.getPort();
    this.address = new InetSocketAddress(bindAddress, port);
    stringValue = other.stringValue;
    checkBindAddressCanBeResolved();
  }

  /** @return Bind address */
  public String getBindAddress() {
    final InetAddress addr = address.getAddress();
    if (addr != null) {
      return addr.getHostAddress();
    } else {
      LogFactory.getLog(HServerAddress.class).error("Could not resolve the"
          + " DNS name of " + stringValue);
      return null;
    }
  }

  private void checkBindAddressCanBeResolved() {
    if (getBindAddress() == null) {
      throw new IllegalArgumentException("Could not resolve the"
          + " DNS name of " + stringValue);
    }
  }

  /** @return Port number */
  public int getPort() {
    return address.getPort();
  }

  /** @return Hostname */
  public String getHostname() {
    return address.getHostName();
  }

  /** @return The InetSocketAddress */
  public InetSocketAddress getInetSocketAddress() {
    return address;
  }

  /**
   * @return String formatted as <code>&lt;bind address> ':' &lt;port></code>
   */
  @Override
  public String toString() {
    return stringValue == null ? "" : stringValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    return compareTo((HServerAddress) o) == 0;
  }

  @Override
  public int hashCode() {
    int result = address == null? 0: address.hashCode();
    result ^= toString().hashCode();
    return result;
  }

  //
  // Writable
  //

  public void readFields(DataInput in) throws IOException {
    String hostname = in.readUTF();
    int port = in.readInt();

    if (hostname == null || hostname.length() == 0) {
      address = null;
      stringValue = null;
    } else {
      address = new InetSocketAddress(hostname, port);
      stringValue = hostname + ":" + port;
      checkBindAddressCanBeResolved();
    }
  }

  public void write(DataOutput out) throws IOException {
    if (address == null) {
      out.writeUTF("");
      out.writeInt(0);
    } else {
      out.writeUTF(address.getAddress().getHostName());
      out.writeInt(address.getPort());
    }
  }

  //
  // Comparable
  //

  public int compareTo(HServerAddress o) {
    // Addresses as Strings may not compare though address is for the one
    // server with only difference being that one address has hostname
    // resolved whereas other only has IP.
    if (address.equals(o.address)) return 0;
    return toString().compareTo(o.toString());
  }
}
