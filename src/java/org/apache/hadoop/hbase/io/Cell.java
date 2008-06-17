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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * Cell - Used to transport a cell value (byte[]) and the timestamp it was 
 * stored with together as a result for get and getRow methods. This promotes
 * the timestamp of a cell to a first-class value, making it easy to take 
 * note of temporal data. Cell is used all the way from HStore up to HTable.
 */
public class Cell implements Writable {
  protected byte[] value;
  protected long timestamp;
  
  /** For Writable compatibility */
  public Cell() {
    value = null;
    timestamp = 0;
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
    this.value = value;
    this.timestamp = timestamp;
  }
  
  /**
   * Get the cell's value.
   *
   * @return cell's value
   */
  public byte[] getValue() {
    return value;
  }
  
  /**
   * Get teh cell's timestamp
   *
   * @return cell's timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }
  
  @Override
  public String toString() {
    return "timestamp=" + this.timestamp + ", value=" +
      Bytes.toString(this.value);
  }
  //
  // Writable
  //

  /** {@inheritDoc} */
  public void readFields(final DataInput in) throws IOException {
    timestamp = in.readLong();
    this.value = Bytes.readByteArray(in);
  }

  /** {@inheritDoc} */
  public void write(final DataOutput out) throws IOException {
    out.writeLong(timestamp);
    Bytes.writeByteArray(out, this.value);
  } 
}