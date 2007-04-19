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
 * A Key for a stored row
 ******************************************************************************/
public class HStoreKey implements WritableComparable {
  public static final byte[] DELETE_BYTES = "HSTOREKEY::DELETEVAL".getBytes();
  public static final byte[] COMPLETE_CACHEFLUSH = "HSTOREKEY::CACHEFLUSH".getBytes();

  public static Text extractFamily(Text col) throws IOException {
    String column = col.toString();
    int colpos = column.indexOf(":");
    if (colpos < 0) {
      throw new IllegalArgumentException("Illegal column name has no family indicator: " + column);
    }
    return new Text(column.substring(0, colpos));
  }

  Text row;
  Text column;
  long timestamp;

  public HStoreKey() {
    this.row = new Text();
    this.column = new Text();
    this.timestamp = Long.MAX_VALUE;
  }
  
  public HStoreKey(Text row) {
    this.row = new Text(row);
    this.column = new Text();
    this.timestamp = Long.MAX_VALUE;
  }
  
  public HStoreKey(Text row, long timestamp) {
    this.row = new Text(row);
    this.column = new Text();
    this.timestamp = timestamp;
  }
  
  public HStoreKey(Text row, Text column) {
    this.row = new Text(row);
    this.column = new Text(column);
    this.timestamp = Long.MAX_VALUE;
  }
  
  public HStoreKey(Text row, Text column, long timestamp) {
    this.row = new Text(row);
    this.column = new Text(column);
    this.timestamp = timestamp;
  }
  
  public void setRow(Text newrow) {
    this.row.set(newrow);
  }
  
  public void setColumn(Text newcol) {
    this.column.set(newcol);
  }
  
  public void setVersion(long timestamp) {
    this.timestamp = timestamp;
  }
  
  public Text getRow() {
    return row;
  }
  
  public Text getColumn() {
    return column;
  }
  
  public long getTimestamp() {
    return timestamp;
  }
  
  public boolean matchesRowCol(HStoreKey other) {
    if (this.row.compareTo(other.row) == 0 &&
        this.column.compareTo(other.column) == 0) {
      return true;
      
    } else {
      return false;
    }
  }
  
  public boolean matchesWithoutColumn(HStoreKey other) {
    if ((this.row.compareTo(other.row) == 0) &&
        (this.timestamp >= other.getTimestamp())) {
      return true;
      
    } else {
      return false;
    }
  }
  
  public String toString() {
    return row.toString() + "/" + column.toString() + "/" + timestamp;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Comparable
  //////////////////////////////////////////////////////////////////////////////

  public int compareTo(Object o) {
    HStoreKey other = (HStoreKey) o;
    int result = this.row.compareTo(other.row);
    if (result == 0) {
      result = this.column.compareTo(other.column);
      
      if (result == 0) {
        if (this.timestamp < other.timestamp) {
          result = 1;
          
        } else if (this.timestamp > other.timestamp) {
          result = -1;
        }
      }
    }
    return result;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    row.write(out);
    column.write(out);
    out.writeLong(timestamp);
  }

  public void readFields(DataInput in) throws IOException {
    row.readFields(in);
    column.readFields(in);
    timestamp = in.readLong();
  }
}

