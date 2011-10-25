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
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableComparable;

/**
 * HServerAddress hosts a {@link InetSocketAddress} and makes it
 * {@link WritableComparable}.  Resolves on construction AND on
 * deserialization -- since we're internally creating an InetSocketAddress --
 * so could end up with different results if the two ends of serialization have
 * different resolvers. Be careful where you use it.  Should only be used when
 * you need to pass an InetSocketAddress across an RPC.  Even then its a bad
 * idea because of the above resolve issue.
 * @deprecated Use {@link InetSocketAddress} or {@link ServerName} or
 * a hostname String and port.
 */
public class HServerAddress implements WritableComparable<HServerAddress> {
  // Hard to deprecate this class. Its in the API as internal class,
  // in particular as an inner class of HRegionLocation.  Besides, sometimes
  // we do want to serialize a InetSocketAddress; this class can be used then.
  private InetSocketAddress address = null;
  private String cachedToString = "";

  /**
   * Constructor for deserialization use only.
   */
  public HServerAddress() {
    super();
  }

  /**
   * Construct an instance from an {@link InetSocketAddress}.
   * @param address InetSocketAddress of server
   */
  public HServerAddress(InetSocketAddress address) {
    this.address = address;
    checkBindAddressCanBeResolved();
    this.cachedToString = createCachedToString();
  }

  private String createCachedToString() {
    return this.address.toString();
  }

  /**
   * @param hostname Hostname
   * @param port Port number
   */
  public HServerAddress(final String hostname, final int port) {
    this(getResolvedAddress(new InetSocketAddress(hostname, port)));
  }

  /**
   * Copy-constructor.
   * @param other HServerAddress to copy from
   */
  public HServerAddress(HServerAddress other) {
    this(getResolvedAddress(new InetSocketAddress(other.getHostname(), other.getPort())));
  }

   private static InetSocketAddress getResolvedAddress(InetSocketAddress address) {
     String bindAddress = getBindAddressInternal(address);
     int port = address.getPort();
     return new InetSocketAddress(bindAddress, port);
   }
  
  /** @return Bind address -- the raw IP, the result of a call to
   * InetSocketAddress#getAddress()#getHostAddress() --
   * or null if cannot resolve */
  public String getBindAddress() {
    return getBindAddressInternal(address);
  }

  private static String getBindAddressInternal(InetSocketAddress address) {
    final InetAddress addr = address.getAddress();
    if (addr != null) {
      return addr.getHostAddress();
    } else {
      LogFactory.getLog(HServerAddress.class).error("Could not resolve the"
          + " DNS name of " + address.getHostName());
      return null;
    }
  }
  
  private void checkBindAddressCanBeResolved() {
    if (getBindAddress() == null) {
      throw new IllegalArgumentException("Could not resolve the"
          + " DNS name of " + this.address.toString());
    }
  }

  /** @return Port number */
  public int getPort() {
    return this.address.getPort();
  }

  /** @return Hostname */
  public String getHostname() {
    return this.address.getHostName();
  }

  /**
   * @return Returns <hostname> ':' <port>
   */
  public String getHostnameAndPort() {
    return getHostname() + ":" + getPort();
  }

  /** @return The InetSocketAddress */
  public InetSocketAddress getInetSocketAddress() {
    return this.address;
  }

  /**
   * @return String formatted as <code>&lt;bind address> ':' &lt;port></code>
   */
  @Override
  public String toString() {
    return this.cachedToString;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null) return false;
    if (getClass() != o.getClass()) return false;
    return compareTo((HServerAddress)o) == 0;
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
    if (hostname != null && hostname.length() > 0) {
      this.address = getResolvedAddress(new InetSocketAddress(hostname, port));
      checkBindAddressCanBeResolved();
      createCachedToString();
    }
  }

  public void write(DataOutput out) throws IOException {
    if (this.address == null) {
      out.writeUTF("");
      out.writeInt(0);
    } else {
      out.writeUTF(this.address.getAddress().getHostName());
      out.writeInt(this.address.getPort());
    }
  }

  //
  // Comparable
  //

  public int compareTo(HServerAddress o) {
    // Addresses as Strings may not compare though address is for the one
    // server with only difference being that one address has hostname
    // resolved whereas other only has IP.
    if (this.address.equals(o.address)) return 0;
    return toString().compareTo(o.toString());
  }
}
