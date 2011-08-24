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

import org.apache.hadoop.hbase.util.Addressing;

/**
 * Data structure to hold HRegionInfo and the address for the hosting
 * HRegionServer.  Immutable.  Comparable, but we compare the 'location' only:
 * i.e. the hostname and port, and *not* the regioninfo.  This means two
 * instances are the same if they refer to the same 'location' (the same
 * hostname and port), though they may be carrying different regions.
 */
public class HRegionLocation implements Comparable<HRegionLocation> {
  private final HRegionInfo regionInfo;
  private final String hostname;
  private final int port;
  // Cache of the 'toString' result.
  private String cachedString = null;
  // Cache of the hostname + port
  private String cachedHostnamePort;

  /**
   * Constructor
   * @param regionInfo the HRegionInfo for the region
   * @param hostname Hostname
   * @param port port
   */
  public HRegionLocation(HRegionInfo regionInfo, final String hostname,
      final int port) {
    this.regionInfo = regionInfo;
    this.hostname = hostname;
    this.port = port;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public synchronized String toString() {
    if (this.cachedString == null) {
      this.cachedString = "region=" + this.regionInfo.getRegionNameAsString() +
      ", hostname=" + this.hostname + ", port=" + this.port;
    }
    return this.cachedString;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (!(o instanceof HRegionLocation)) {
      return false;
    }
    return this.compareTo((HRegionLocation)o) == 0;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    int result = this.hostname.hashCode();
    result ^= this.port;
    return result;
  }

  /** @return HRegionInfo */
  public HRegionInfo getRegionInfo(){
    return regionInfo;
  }

  /**
   * Do not use!!! Creates a HServerAddress instance which will do a resolve.
   * @return HServerAddress
   * @deprecated Use {@link #getHostnamePort}
   */
  public HServerAddress getServerAddress() {
    return new HServerAddress(this.hostname, this.port);
  }

  public String getHostname() {
    return this.hostname;
  }

  public int getPort() {
    return this.port;
  }

  /**
   * @return String made of hostname and port formatted as per {@link Addressing#createHostAndPortStr(String, int)}
   */
  public synchronized String getHostnamePort() {
    if (this.cachedHostnamePort == null) {
      this.cachedHostnamePort =
        Addressing.createHostAndPortStr(this.hostname, this.port);
    }
    return this.cachedHostnamePort;
  }

  //
  // Comparable
  //

  public int compareTo(HRegionLocation o) {
    int result = this.hostname.compareTo(o.getHostname());
    if (result != 0) return result;
    return this.port - o.getPort();
  }
}