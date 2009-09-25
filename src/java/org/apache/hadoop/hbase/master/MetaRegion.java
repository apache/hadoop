/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.util.Bytes;


/** Describes a meta region and its server */
public class MetaRegion implements Comparable<MetaRegion> {
  private final HServerAddress server;
  private HRegionInfo regionInfo;

  MetaRegion(final HServerAddress server, HRegionInfo regionInfo) {
    if (server == null) {
      throw new IllegalArgumentException("server cannot be null");
    }
    this.server = server;
    if (regionInfo == null) {
      throw new IllegalArgumentException("regionInfo cannot be null");
    }
    this.regionInfo = regionInfo;
  }
  
  @Override
  public String toString() {
    return "{server: " + this.server.toString() + ", regionname: " +
        regionInfo.getRegionNameAsString() + ", startKey: <" +
        Bytes.toString(regionInfo.getStartKey()) + ">}";
  }

  /** @return the regionName */
  public byte [] getRegionName() {
    return regionInfo.getRegionName();
  }

  /** @return the server */
  public HServerAddress getServer() {
    return server;
  }

  /** @return the startKey */
  public byte [] getStartKey() {
    return regionInfo.getStartKey();
  }

  
  /** @return the endKey */
  public byte [] getEndKey() {
    return regionInfo.getEndKey();
  }

  
  public HRegionInfo getRegionInfo() {
    return regionInfo;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof MetaRegion && this.compareTo((MetaRegion)o) == 0;
  }

  @Override
  public int hashCode() {
    return regionInfo.hashCode();
  }

  // Comparable

  public int compareTo(MetaRegion other) {
    int cmp = regionInfo.compareTo(other.regionInfo);
    if(cmp == 0) {
      // Might be on different host?
      cmp = this.server.compareTo(other.server);
    }
    return cmp;
  }
}