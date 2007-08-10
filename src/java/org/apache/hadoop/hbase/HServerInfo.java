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

/**
 * HServerInfo contains metainfo about an HRegionServer, Currently it only
 * contains the server start code.
 * 
 * In the future it will contain information about the source machine and
 * load statistics.
 */
public class HServerInfo implements Writable {
  private HServerAddress serverAddress;
  private long startCode;
  private HServerLoad load;

  /** default constructor - used by Writable */
  public HServerInfo() {
    this.serverAddress = new HServerAddress();
    this.startCode = 0;
    this.load = new HServerLoad();
  }
  
  /**
   * Constructor
   * @param serverAddress
   * @param startCode
   */
  public HServerInfo(HServerAddress serverAddress, long startCode) {
    this.serverAddress = new HServerAddress(serverAddress);
    this.startCode = startCode;
    this.load = new HServerLoad();
  }
  
  /**
   * Construct a new object using another as input (like a copy constructor)
   * @param other
   */
  public HServerInfo(HServerInfo other) {
    this.serverAddress = new HServerAddress(other.getServerAddress());
    this.startCode = other.getStartCode();
    this.load = other.getLoad();
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
  public HServerAddress getServerAddress() {
    return serverAddress;
  }
 
  /** @return the server start code */
  public long getStartCode() {
    return startCode;
  }
  
  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "address: " + this.serverAddress + ", startcode: " + this.startCode
    + ", load: (" + this.load.toString() + ")";
  }

  // Writable

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    this.serverAddress.readFields(in);
    this.startCode = in.readLong();
    this.load.readFields(in);
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    this.serverAddress.write(out);
    out.writeLong(this.startCode);
    this.load.write(out);
  }
}