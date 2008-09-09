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
package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * Cell - Used to transport a cell value (byte[]) and the timestamp it was 
 * stored with together as a result for get and getRow methods. This promotes
 * the timestamp of a cell to a first-class value, making it easy to take 
 * note of temporal data. Cell is used all the way from HStore up to HTable.
 */
public class Cell implements Writable, Iterable<Cell> {
  protected byte[][] values;
  protected long[] timestamps;
  
  /** For Writable compatibility */
  public Cell() {
    values = null;
    timestamps = null;
  }

  /**
   * Create a new Cell with a given value and timestamp. Used by HStore.
   * @param value
   * @param timestamp
   */
  public Cell(String value, long timestamp) {
    this(Bytes.toBytes(value), timestamp);
  }

  /**
   * Create a new Cell with a given value and timestamp. Used by HStore.
   * @param value
   * @param timestamp
   */
  public Cell(byte[] value, long timestamp) {
    this.values = new byte[1][];
    this.values[0] = value;
    this.timestamps = new long[1];
    this.timestamps[0] = timestamp;
  }
  
  /**
   * @param vals array of values
   * @param ts array of timestamps
   */
  public Cell(String[] vals, long[] ts) {
    if (vals.length != ts.length) {
      throw new IllegalArgumentException(
          "number of values must be the same as the number of timestamps");
    }
    this.values = new byte[vals.length][];
    this.timestamps = new long[ts.length];
    for (int i = 0; i < values.length; i++) {
      this.values[i] = Bytes.toBytes(vals[i]);
      this.timestamps[i] = ts[i];
    }
  }
  
  /**
   * @param vals array of values
   * @param ts array of timestamps
   */
  public Cell(byte[][] vals, long[] ts) {
    if (vals.length != ts.length) {
      throw new IllegalArgumentException(
          "number of values must be the same as the number of timestamps");
    }
    this.values = new byte[vals.length][];
    this.timestamps = new long[ts.length];
    System.arraycopy(vals, 0, this.values, 0, vals.length);
    System.arraycopy(ts, 0, this.timestamps, 0, ts.length);
  }
  
  /** @return the current cell's value */
  public byte[] getValue() {
    return values[0];
  }
  
  /** @return the current cell's timestamp */
  public long getTimestamp() {
    return timestamps[0];
  }
  
  @Override
  public String toString() {
    if (this.values.length == 1) {
      return "timestamp=" + this.timestamps[0] + ", value=" +
        Bytes.toString(this.values[0]);
    }
    StringBuilder s = new StringBuilder("{ ");
    for (int i = 0; i < this.values.length; i++) {
      if (i > 0) {
        s.append(", ");
      }
      s.append("[timestamp=");
      s.append(timestamps[i]);
      s.append(", value=");
      s.append(Bytes.toString(values[i]));
      s.append("]");
    }
    s.append(" }");
    return s.toString();
  }
  
  //
  // Writable
  //

  public void readFields(final DataInput in) throws IOException {
    int nvalues = in.readInt();
    this.timestamps = new long[nvalues];
    this.values = new byte[nvalues][];
    for (int i = 0; i < nvalues; i++) {
      this.timestamps[i] = in.readLong();
    }
    for (int i = 0; i < nvalues; i++) {
      this.values[i] = Bytes.readByteArray(in);
    }
  }

  public void write(final DataOutput out) throws IOException {
    out.writeInt(this.values.length);
    for (int i = 0; i < this.timestamps.length; i++) {
      out.writeLong(this.timestamps[i]);
    }
    for (int i = 0; i < this.values.length; i++) {
      Bytes.writeByteArray(out, this.values[i]);
    }
  }
  
  //
  // Iterable
  //

  public Iterator<Cell> iterator() {
    return new CellIterator();
  }
  private class CellIterator implements Iterator<Cell> {
    private int currentValue = -1;
    CellIterator() {
    }
    
    public boolean hasNext() {
      return currentValue < values.length;
    }
    
    public Cell next() {
      currentValue += 1;
      return new Cell(values[currentValue], timestamps[currentValue]);
    }
    
    public void remove() throws UnsupportedOperationException {
      throw new UnsupportedOperationException("remove is not supported");
    }
  }
}