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

import org.apache.hadoop.io.Writable;


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
  private int infoPort;

  /** default constructor - used by Writable */
  public HServerInfo() {
    this(new HServerAddress(), 0, HConstants.DEFAULT_REGIONSERVER_INFOPORT);
  }
  
  /**
   * Constructor
   * @param serverAddress
   * @param startCode
   * @param infoPort Port the info server is listening on.
   */
  public HServerInfo(HServerAddress serverAddress, long startCode,
      final int infoPort) {
    this.serverAddress = serverAddress;
    this.startCode = startCode;
    this.load = new HServerLoad();
    this.infoPort = infoPort;
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
  
  /**
   * @return Port the info server is listening on.
   */
  public int getInfoPort() {
    return this.infoPort;
  }
  
  /**
   * @param startCode the startCode to set
   */
  public void setStartCode(long startCode) {
    this.startCode = startCode;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "address: " + this.serverAddress + ", startcode: " + this.startCode
    + ", load: (" + this.load.toString() + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof HServerInfo)) {
      return false;
    }
    HServerInfo that = (HServerInfo)obj;
    if (!this.serverAddress.equals(that.serverAddress)) {
      return false;
    }
    if (this.infoPort != that.infoPort) {
      return false;
    }
    if (this.startCode != that.startCode) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = this.serverAddress.hashCode();
    result ^= this.infoPort;
    result ^= this.startCode;
    return result;
  }


  // Writable
  public void readFields(DataInput in) throws IOException {
    this.serverAddress.readFields(in);
    this.startCode = in.readLong();
    this.load.readFields(in);
    this.infoPort = in.readInt();
  }

  public void write(DataOutput out) throws IOException {
    this.serverAddress.write(out);
    out.writeLong(this.startCode);
    this.load.write(out);
    out.writeInt(this.infoPort);
  }
}
