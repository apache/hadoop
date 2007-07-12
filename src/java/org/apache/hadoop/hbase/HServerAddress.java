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

import org.apache.hadoop.io.*;

import java.io.*;
import java.net.InetSocketAddress;

/*******************************************************************************
 * HServerAddress is a "label" for a HBase server that combines the host
 * name and port number.
 ******************************************************************************/
public class HServerAddress implements Writable {
  private InetSocketAddress address;
  private String stringValue;

  public HServerAddress() {
    this.address = null;
    this.stringValue = null;
  }
  
  public HServerAddress(InetSocketAddress address) {
    this.address = address;
    this.stringValue = address.getAddress().getHostAddress() + ":" +
      address.getPort();
  }
  
  public HServerAddress(String hostAndPort) {
    int colonIndex = hostAndPort.indexOf(':');
    if(colonIndex < 0) {
      throw new IllegalArgumentException("Not a host:port pair: " + hostAndPort);
    }
    String host = hostAndPort.substring(0, colonIndex);
    int port =
      Integer.valueOf(hostAndPort.substring(colonIndex + 1)).intValue();
    this.address = new InetSocketAddress(host, port);
    this.stringValue = hostAndPort;
  }
  
  public HServerAddress(String bindAddress, int port) {
    this.address = new InetSocketAddress(bindAddress, port);
    this.stringValue = bindAddress + ":" + port;
  }
  
  public HServerAddress(HServerAddress other) {
    String bindAddress = other.getBindAddress();
    int port = other.getPort();
    address = new InetSocketAddress(bindAddress, port);
    stringValue = bindAddress + ":" + port;
  }
  
  public String getBindAddress() {
    return address.getAddress().getHostAddress();
  }
  
  public int getPort() {
    return address.getPort();
  }
  
  public InetSocketAddress getInetSocketAddress() {
    return address;
  }
  
  public String toString() {
    return (stringValue == null ? "" : stringValue);
  }

  //////////////////////////////////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////////////////////////////////

  public void readFields(DataInput in) throws IOException {
    String bindAddress = in.readUTF();
    int port = in.readInt();
    
    if(bindAddress == null || bindAddress.length() == 0) {
      address = null;
      stringValue = null;
      
    } else {
      address = new InetSocketAddress(bindAddress, port);
      stringValue = bindAddress + ":" + port;
    }
  }

  public void write(DataOutput out) throws IOException {
    if(address == null) {
      out.writeUTF("");
      out.writeInt(0);
      
    } else {
      out.writeUTF(address.getAddress().getHostAddress());
      out.writeInt(address.getPort());
    }
  }
}
