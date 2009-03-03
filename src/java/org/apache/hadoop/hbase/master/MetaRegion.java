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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.util.Bytes;


/** Describes a meta region and its server */
public class MetaRegion implements Comparable<MetaRegion> {
  private final HServerAddress server;
  private final byte [] regionName;
  private final byte [] startKey;

  MetaRegion(final HServerAddress server, final byte [] regionName) {
    this (server, regionName, HConstants.EMPTY_START_ROW);
  }

  MetaRegion(final HServerAddress server, final byte [] regionName,
      final byte [] startKey) {
    if (server == null) {
      throw new IllegalArgumentException("server cannot be null");
    }
    this.server = server;
    if (regionName == null) {
      throw new IllegalArgumentException("regionName cannot be null");
    }
    this.regionName = regionName;
    this.startKey = startKey;
  }
  
  @Override
  public String toString() {
    return "{regionname: " + Bytes.toString(this.regionName) +
      ", startKey: <" + Bytes.toString(this.startKey) +
      ">, server: " + this.server.toString() + "}";
  }

  /** @return the regionName */
  public byte [] getRegionName() {
    return regionName;
  }

  /** @return the server */
  public HServerAddress getServer() {
    return server;
  }

  /** @return the startKey */
  public byte [] getStartKey() {
    return startKey;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof MetaRegion && this.compareTo((MetaRegion)o) == 0;
  }

  @Override
  public int hashCode() {
    int result = this.regionName.hashCode();
    result ^= this.startKey.hashCode();
    return result;
  }

  // Comparable

  public int compareTo(MetaRegion other) {
    int result = Bytes.compareTo(this.regionName, other.getRegionName());
    if(result == 0) {
      result = Bytes.compareTo(this.startKey, other.getStartKey());
      if (result == 0) {
        // Might be on different host?
        result = this.server.compareTo(other.server);
      }
    }
    return result;
  }
}