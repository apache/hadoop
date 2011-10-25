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

import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.io.VersionedWritable;
import org.apache.hadoop.io.WritableComparable;


/**
 * HServerInfo is meta info about an {@link HRegionServer}.  It hosts the
 * {@link HServerAddress}, its webui port, and its server startcode.  It was
 * used to pass meta info about a server across an RPC but we've since made
 * it so regionserver info is up in ZooKeeper and so this class is on its
 * way out. It used to carry {@link HServerLoad} but as off HBase 0.92.0, the
 * HServerLoad is passed independent of this class. Also, we now no longer pass
 * the webui from regionserver to master (TODO: Fix).
 * @deprecated Use {@link InetSocketAddress} and or {@link ServerName} and or
 * {@link HServerLoad}
 */
public class HServerInfo extends VersionedWritable
implements WritableComparable<HServerInfo> {
  private static final byte VERSION = 1;
  private HServerAddress serverAddress = new HServerAddress();
  private long startCode;
  private int webuiport;

  public HServerInfo() {
    super();
  }

  /**
   * Constructor that creates a HServerInfo with a generated startcode
   * @param serverAddress
   * @param webuiport Port the webui runs on.
   */
  public HServerInfo(final HServerAddress serverAddress, final int webuiport) {
    this(serverAddress, System.currentTimeMillis(), webuiport);
  }

  public HServerInfo(HServerAddress serverAddress, long startCode,
      final int webuiport) {
    this.serverAddress = serverAddress;
    this.startCode = startCode;
    this.webuiport = webuiport;
  }

  /**
   * Copy-constructor
   * @param other
   */
  public HServerInfo(HServerInfo other) {
    this.serverAddress = new HServerAddress(other.getServerAddress());
    this.startCode = other.getStartCode();
    this.webuiport = other.getInfoPort();
  }

  /** @return the object version number */
  public byte getVersion() {
    return VERSION;
  }

  public synchronized HServerAddress getServerAddress() {
    return new HServerAddress(serverAddress);
  }

  public synchronized long getStartCode() {
    return startCode;
  }

  public int getInfoPort() {
    return getWebuiPort();
  }

  public int getWebuiPort() {
    return this.webuiport;
  }

  public String getHostname() {
    return this.serverAddress.getHostname();
  }

  /**
   * @return ServerName and load concatenated.
   */
  @Override
  public synchronized String toString() {
    return ServerName.getServerName(this.serverAddress.getHostnameAndPort(),
      this.startCode);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    return compareTo((HServerInfo)obj) == 0;
  }

  @Override
  public int hashCode() {
    int code = this.serverAddress.hashCode();
    code ^= this.webuiport;
    code ^= this.startCode;
    return code;
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.serverAddress.readFields(in);
    this.startCode = in.readLong();
    this.webuiport = in.readInt();
  }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    this.serverAddress.write(out);
    out.writeLong(this.startCode);
    out.writeInt(this.webuiport);
  }

  public int compareTo(HServerInfo o) {
    int compare = this.serverAddress.compareTo(o.getServerAddress());
    if (compare != 0) return compare;
    if (this.webuiport != o.getInfoPort()) return this.webuiport - o.getInfoPort();
    if (this.startCode != o.getStartCode()) return (int)(this.startCode - o.getStartCode());
    return 0;
  }
}
