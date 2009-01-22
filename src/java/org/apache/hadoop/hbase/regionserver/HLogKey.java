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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;

import java.io.*;

/**
 * A Key for an entry in the change log.
 * 
 * The log intermingles edits to many tables and rows, so each log entry 
 * identifies the appropriate table and row.  Within a table and row, they're 
 * also sorted.
 * 
 * Some Transactional edits (START, COMMIT, ABORT) will not have an associated row.
 */
public class HLogKey implements WritableComparable<HLogKey> {
  private byte [] regionName;
  private byte [] tablename;
  private byte [] row;
  private long logSeqNum;

  /** Create an empty key useful when deserializing */
  public HLogKey() {
    this(null, null, null, 0L);
  }
  
  /**
   * Create the log key!
   * We maintain the tablename mainly for debugging purposes.
   * A regionName is always a sub-table object.
   *
   * @param regionName  - name of region
   * @param tablename   - name of table
   * @param row         - row key
   * @param logSeqNum   - log sequence number
   */
  public HLogKey(final byte [] regionName, final byte [] tablename,
      final byte [] row, long logSeqNum) {
    this.regionName = regionName;
    this.tablename = tablename;
    this.row = row;
    this.logSeqNum = logSeqNum;
  }

  //////////////////////////////////////////////////////////////////////////////
  // A bunch of accessors
  //////////////////////////////////////////////////////////////////////////////

  /** @return region name */
  public byte [] getRegionName() {
    return regionName;
  }
  
  /** @return table name */
  public byte [] getTablename() {
    return tablename;
  }
  
  /** @return row key */
  public byte [] getRow() {
    return row;
  }
  
  /** @return log sequence number */
  public long getLogSeqNum() {
    return logSeqNum;
  }
  
  @Override
  public String toString() {
    return Bytes.toString(tablename) + "/" + Bytes.toString(regionName) + "/" +
      Bytes.toString(row) + "/" + logSeqNum;
  }
  
  @Override
  public boolean equals(Object obj) {
    return compareTo((HLogKey)obj) == 0;
  }
  
  @Override
  public int hashCode() {
    int result = this.regionName.hashCode();
    result ^= this.row.hashCode(); 
    result ^= this.logSeqNum;
    return result;
  }

  //
  // Comparable
  //

  public int compareTo(HLogKey o) {
    int result = Bytes.compareTo(this.regionName, o.regionName);
    
    if(result == 0) {
      result = Bytes.compareTo(this.row, o.row);
      
      if(result == 0) {
        
        if (this.logSeqNum < o.logSeqNum) {
          result = -1;
          
        } else if (this.logSeqNum > o.logSeqNum) {
          result = 1;
        }
      }
    }
    return result;
  }

  //
  // Writable
  //

  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.regionName);
    Bytes.writeByteArray(out, this.tablename);
    Bytes.writeByteArray(out, this.row);
    out.writeLong(logSeqNum);
  }
  
  public void readFields(DataInput in) throws IOException {
    this.regionName = Bytes.readByteArray(in);
    this.tablename = Bytes.readByteArray(in);
    this.row = Bytes.readByteArray(in);
    this.logSeqNum = in.readLong();
  }
}