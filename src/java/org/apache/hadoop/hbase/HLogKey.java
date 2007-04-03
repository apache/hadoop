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

import org.apache.hadoop.io.*;

import java.io.*;

/*******************************************************************************
 * A Key for an entry in the change log.
 * 
 * The log intermingles edits to many tables and rows, so each log entry 
 * identifies the appropriate table and row.  Within a table and row, they're 
 * also sorted.
 ******************************************************************************/
public class HLogKey implements WritableComparable {
  Text regionName = new Text();
  Text tablename = new Text();
  Text row = new Text();
  long logSeqNum = 0L;

  /**
   * Create the log key!
   * We maintain the tablename mainly for debugging purposes.
   * A regionName is always a sub-table object.
   */
  public HLogKey() {
  }
  
  public HLogKey(Text regionName, Text tablename, Text row, long logSeqNum) {
    this.regionName.set(regionName);
    this.tablename.set(tablename);
    this.row.set(row);
    this.logSeqNum = logSeqNum;
  }

  //////////////////////////////////////////////////////////////////////////////
  // A bunch of accessors
  //////////////////////////////////////////////////////////////////////////////

  public Text getRegionName() {
    return regionName;
  }
  
  public Text getTablename() {
    return tablename;
  }
  
  public Text getRow() {
    return row;
  }
  
  public long getLogSeqNum() {
    return logSeqNum;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Comparable
  //////////////////////////////////////////////////////////////////////////////

  /**
   * When sorting through log entries, we want to group items
   * first in the same table, then to the same row, then finally
   * ordered by write-order.
   */
  public int compareTo(Object o) {
    HLogKey other = (HLogKey) o;
    int result = this.regionName.compareTo(other.regionName);
    
    if(result == 0) {
      result = this.row.compareTo(other.row);
      
      if(result == 0) {
        
        if (this.logSeqNum < other.logSeqNum) {
          result = -1;
          
        } else if (this.logSeqNum > other.logSeqNum) {
          result = 1;
        }
      }
    }
    return result;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    this.regionName.write(out);
    this.tablename.write(out);
    this.row.write(out);
    out.writeLong(logSeqNum);
  }
  
  public void readFields(DataInput in) throws IOException {
    this.regionName.readFields(in);
    this.tablename.readFields(in);
    this.row.readFields(in);
    this.logSeqNum = in.readLong();
  }
}

