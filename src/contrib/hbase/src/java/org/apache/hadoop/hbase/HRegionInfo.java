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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * HRegion information.
 * Contains HRegion id, start and end keys, a reference to this
 * HRegions' table descriptor, etc.
 */
public class HRegionInfo implements WritableComparable {
  /** delimiter used between portions of a region name */
  public static final char DELIMITER = ',';
  
  /**
   * Extracts table name prefix from a region name.
   * Presumes region names are ASCII characters only.
   * @param regionName A region name.
   * @return The table prefix of a region name.
   */
  public static Text getTableNameFromRegionName(final Text regionName) {
    int index = -1;
    byte [] bytes = regionName.getBytes();
    for (int i = 0; i < bytes.length; i++) {
      if (((char) bytes[i]) == DELIMITER) {
        index = i;
        break;
      }
    }
    if (index == -1) {
      throw new IllegalArgumentException(regionName.toString() + " does not " +
        "contain " + DELIMITER + " character");
    }
    byte [] tableName = new byte[index];
    System.arraycopy(bytes, 0, tableName, 0, index);
    return new Text(tableName);
  }

  Text regionName;
  long regionId;
  Text startKey;
  Text endKey;
  boolean offLine;
  boolean split;
  HTableDescriptor tableDesc;
  
  /** Default constructor - creates empty object */
  public HRegionInfo() {
    this.regionId = 0;
    this.tableDesc = new HTableDescriptor();
    this.startKey = new Text();
    this.endKey = new Text();
    this.regionName = new Text();
    this.offLine = false;
    this.split = false;
  }
  
  /**
   * Construct HRegionInfo with explicit parameters
   * 
   * @param regionId the region id
   * @param tableDesc the table descriptor
   * @param startKey first key in region
   * @param endKey end of key range
   * @throws IllegalArgumentException
   */
  public HRegionInfo(long regionId, HTableDescriptor tableDesc, Text startKey,
      Text endKey)
  throws IllegalArgumentException {
    this(regionId, tableDesc, startKey, endKey, false);
  }

  /**
   * Construct HRegionInfo with explicit parameters
   * 
   * @param regionId the region id
   * @param tableDesc the table descriptor
   * @param startKey first key in region
   * @param endKey end of key range
   * @param split true if this region has split and we have daughter regions
   * regions that may or may not hold references to this region.
   * @throws IllegalArgumentException
   */
  public HRegionInfo(long regionId, HTableDescriptor tableDesc, Text startKey,
      Text endKey, final boolean split)
  throws IllegalArgumentException {
    
    this.regionId = regionId;
    
    if(tableDesc == null) {
      throw new IllegalArgumentException("tableDesc cannot be null");
    }
    
    this.tableDesc = tableDesc;
    
    this.startKey = new Text();
    if(startKey != null) {
      this.startKey.set(startKey);
    }
    
    this.endKey = new Text();
    if(endKey != null) {
      this.endKey.set(endKey);
    }
    
    this.regionName = new Text(tableDesc.getName().toString() + DELIMITER +
      (startKey == null ? "" : startKey.toString()) + DELIMITER +
      regionId);
    
    this.offLine = false;
    this.split = split;
  }
  
  /** @return the endKey */
  public Text getEndKey(){
    return endKey;
  }

  /** @return the regionId */
  public long getRegionId(){
    return regionId;
  }

  /** @return the regionName */
  public Text getRegionName(){
    return regionName;
  }

  /** @return the startKey */
  public Text getStartKey(){
    return startKey;
  }

  /** @return the tableDesc */
  public HTableDescriptor getTableDesc(){
    return tableDesc;
  }
  
  /**
   * @return True if has been split and has daughters.
   */
  public boolean isSplit() {
    return this.split;
  }
  
  /**
   * @return True if this region is offline.
   */
  public boolean isOffline() {
    return this.offLine;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "regionname: " + this.regionName.toString() + ", startKey: <" +
      this.startKey.toString() + ">," +
      (isOffline()? " offline: true,": "") +
      (isSplit()? " split: true,": "") +
      " tableDesc: {" + this.tableDesc.toString() + "}";
  }
    
  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object o) {
    return this.compareTo(o) == 0;
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    int result = this.regionName.hashCode();
    result ^= Long.valueOf(this.regionId).hashCode();
    result ^= this.startKey.hashCode();
    result ^= this.endKey.hashCode();
    result ^= Boolean.valueOf(this.offLine).hashCode();
    result ^= this.tableDesc.hashCode();
    return result;
  }

  //
  // Writable
  //

  /**
   * {@inheritDoc}
   */
  public void write(DataOutput out) throws IOException {
    out.writeLong(regionId);
    tableDesc.write(out);
    startKey.write(out);
    endKey.write(out);
    regionName.write(out);
    out.writeBoolean(offLine);
    out.writeBoolean(split);
  }
  
  /**
   * {@inheritDoc}
   */
  public void readFields(DataInput in) throws IOException {
    this.regionId = in.readLong();
    this.tableDesc.readFields(in);
    this.startKey.readFields(in);
    this.endKey.readFields(in);
    this.regionName.readFields(in);
    this.offLine = in.readBoolean();
    this.split = in.readBoolean();
  }
  
  //
  // Comparable
  //
  
  /**
   * {@inheritDoc}
   */
  public int compareTo(Object o) {
    HRegionInfo other = (HRegionInfo) o;
    
    // Are regions of same table?
    int result = this.tableDesc.compareTo(other.tableDesc);
    if (result != 0) {
      return result;
    }

    // Compare start keys.
    result = this.startKey.compareTo(other.startKey);
    if (result != 0) {
      return result;
    }
    
    // Compare end keys.
    return this.endKey.compareTo(other.endKey);
  }
}
