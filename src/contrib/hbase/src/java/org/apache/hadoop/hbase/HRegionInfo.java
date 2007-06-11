/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
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
  Text regionName;
  long regionId;
  Text startKey;
  Text endKey;
  boolean offLine;
  HTableDescriptor tableDesc;
  
  /** Default constructor - creates empty object */
  public HRegionInfo() {
    this.regionId = 0;
    this.tableDesc = new HTableDescriptor();
    this.startKey = new Text();
    this.endKey = new Text();
    this.regionName = new Text();
    this.offLine = false;
  }
  
  /**
   * Construct a HRegionInfo object from byte array
   * 
   * @param serializedBytes
   * @throws IOException
   */
  public HRegionInfo(final byte [] serializedBytes) throws IOException {
    this();
    readFields(new DataInputStream(new ByteArrayInputStream(serializedBytes)));
  }

  /**
   * Construct HRegionInfo with explicit parameters
   * 
   * @param regionId    - the regionid
   * @param tableDesc   - the table descriptor
   * @param startKey    - first key in region
   * @param endKey      - end of key range
   * @throws IllegalArgumentException
   */
  public HRegionInfo(long regionId, HTableDescriptor tableDesc, Text startKey,
      Text endKey) throws IllegalArgumentException {
    
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
    
    this.regionName = new Text(tableDesc.getName() + "_" +
      (startKey == null ? "" : startKey.toString()) + "_" +
      regionId);
    
    this.offLine = false;
  }
  
  @Override
  public String toString() {
    return "regionname: " + this.regionName.toString() + ", startKey: <" +
      this.startKey.toString() + ">, tableDesc: {" +
      this.tableDesc.toString() + "}";
  }
    
  @Override
  public boolean equals(Object o) {
    return this.compareTo(o) == 0;
  }
  
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

  //////////////////////////////////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    out.writeLong(regionId);
    tableDesc.write(out);
    startKey.write(out);
    endKey.write(out);
    regionName.write(out);
    out.writeBoolean(offLine);
  }
  
  public void readFields(DataInput in) throws IOException {
    this.regionId = in.readLong();
    this.tableDesc.readFields(in);
    this.startKey.readFields(in);
    this.endKey.readFields(in);
    this.regionName.readFields(in);
    this.offLine = in.readBoolean();
  }

  //////////////////////////////////////////////////////////////////////////////
  // Comparable
  //////////////////////////////////////////////////////////////////////////////
  
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
