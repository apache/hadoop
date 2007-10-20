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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * HRegion information.
 * Contains HRegion id, start and end keys, a reference to this
 * HRegions' table descriptor, etc.
 */
public class HRegionInfo implements WritableComparable {
  private static MessageDigest encoder = null;
  
  static {
    try {
      if (encoder == null) {
        encoder = MessageDigest.getInstance("SHA");
      }
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
  }

  /**
   * @param regionName
   * @return the encodedName
   */
  public static String encodeRegionName(final Text regionName) {
    byte[] bytes = null;
    synchronized (encoder) {
      encoder.update(regionName.getBytes(), 0, regionName.getLength());
      bytes = encoder.digest();
      encoder.reset();
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < bytes.length; i++) {
      sb.append(bytes[i]);
    }
    return sb.toString();
  }

  /** delimiter used between portions of a region name */
  private static final String DELIMITER = ",";

  /** HRegionInfo for root region */
  public static final HRegionInfo rootRegionInfo =
    new HRegionInfo(0L, HTableDescriptor.rootTableDesc);

  /** HRegionInfo for first meta region */
  public static final HRegionInfo firstMetaRegionInfo =
    new HRegionInfo(1L, HTableDescriptor.metaTableDesc);
  
  /**
   * Extracts table name prefix from a region name.
   * Presumes region names are ASCII characters only.
   * @param regionName A region name.
   * @return The table prefix of a region name.
   */
  public static Text getTableNameFromRegionName(final Text regionName) {
    int offset = regionName.find(DELIMITER);
    if (offset == -1) {
      throw new IllegalArgumentException(regionName.toString() + " does not " +
        "contain '" + DELIMITER + "' character");
    }
    byte [] tableName = new byte[offset];
    System.arraycopy(regionName.getBytes(), 0, tableName, 0, offset);
    return new Text(tableName);
  }

  private Text endKey;
  private boolean offLine;
  private long regionId;
  private Text regionName;
  private boolean split;
  private Text startKey;
  private HTableDescriptor tableDesc;

  /** Used to construct the HRegionInfo for the root and first meta regions */
  private HRegionInfo(long regionId, HTableDescriptor tableDesc) {
    this.regionId = regionId;
    this.tableDesc = tableDesc;
    this.endKey = new Text();
    this.offLine = false;
    this.regionName = new Text(tableDesc.getName().toString() + DELIMITER +
        DELIMITER + regionId);
    this.split = false;
    this.startKey = new Text();
  }

  /** Default constructor - creates empty object */
  public HRegionInfo() {
    this.endKey = new Text();
    this.offLine = false;
    this.regionId = 0;
    this.regionName = new Text();
    this.split = false;
    this.startKey = new Text();
    this.tableDesc = new HTableDescriptor();
  }
  
  /**
   * Construct HRegionInfo with explicit parameters
   * 
   * @param tableDesc the table descriptor
   * @param startKey first key in region
   * @param endKey end of key range
   * @throws IllegalArgumentException
   */
  public HRegionInfo(HTableDescriptor tableDesc, Text startKey, Text endKey)
    throws IllegalArgumentException {
    this(tableDesc, startKey, endKey, false);
  }

  /**
   * Construct HRegionInfo with explicit parameters
   * 
   * @param tableDesc the table descriptor
   * @param startKey first key in region
   * @param endKey end of key range
   * @param split true if this region has split and we have daughter regions
   * regions that may or may not hold references to this region.
   * @throws IllegalArgumentException
   */
  public HRegionInfo(HTableDescriptor tableDesc, Text startKey, Text endKey,
      final boolean split) throws IllegalArgumentException {

    if(tableDesc == null) {
      throw new IllegalArgumentException("tableDesc cannot be null");
    }

    this.endKey = new Text();
    if(endKey != null) {
      this.endKey.set(endKey);
    }
    
    this.offLine = false;
    this.regionId = System.currentTimeMillis();
    
    this.regionName = new Text(tableDesc.getName().toString() + DELIMITER +
        (startKey == null ? "" : startKey.toString()) + DELIMITER +
        regionId);
      
    this.split = split;

    this.startKey = new Text();
    if(startKey != null) {
      this.startKey.set(startKey);
    }
    
    this.tableDesc = tableDesc;
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
   * @param split set split status
   */
  public void setSplit(boolean split) {
    this.split = split;
  }

  /**
   * @return True if this region is offline.
   */
  public boolean isOffline() {
    return this.offLine;
  }

  /**
   * @param offLine set online - offline status
   */
  public void setOffline(boolean offLine) {
    this.offLine = offLine;
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
    endKey.write(out);
    out.writeBoolean(offLine);
    out.writeLong(regionId);
    regionName.write(out);
    out.writeBoolean(split);
    startKey.write(out);
    tableDesc.write(out);
  }
  
  /**
   * {@inheritDoc}
   */
  public void readFields(DataInput in) throws IOException {
    this.endKey.readFields(in);
    this.offLine = in.readBoolean();
    this.regionId = in.readLong();
    this.regionName.readFields(in);
    this.split = in.readBoolean();
    this.startKey.readFields(in);
    this.tableDesc.readFields(in);
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
