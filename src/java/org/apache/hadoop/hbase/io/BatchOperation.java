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
package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * Batch update operation.
 * 
 * If value is null, its a DELETE operation.  If its non-null, its a PUT.
 * This object is purposely bare-bones because many instances are created
 * during bulk uploads.  We have one class for DELETEs and PUTs rather than
 * a class per type because it makes the serialization easier.
 * @see BatchUpdate 
 */
public class BatchOperation implements Writable, HeapSize {
  /**
   * Estimated size of this object.
   */
  // JHat says this is 32 bytes.
  public final int ESTIMATED_HEAP_TAX = 36;
  
  private byte [] column = null;
  
  // A null value defines DELETE operations.
  private byte [] value = null;
  
  /**
   * Default constructor
   */
  public BatchOperation() {
    this((byte [])null);
  }

  /**
   * Creates a DELETE batch operation.
   * @param column column name
   */
  public BatchOperation(final byte [] column) {
    this(column, null);
  }

  /**
   * Creates a DELETE batch operation.
   * @param column column name
   */
  public BatchOperation(final String column) {
    this(Bytes.toBytes(column), null);
  }

  /**
   * Create a batch operation.
   * @param column column name
   * @param value column value.  If non-null, this is a PUT operation.
   */
  public BatchOperation(final String column, String value) {
    this(Bytes.toBytes(column), Bytes.toBytes(value));
  }

  /**
   * Create a batch operation.
   * @param column column name
   * @param value column value.  If non-null, this is a PUT operation.
   */
  public BatchOperation(final byte [] column, final byte [] value) {
    this.column = column;
    this.value = value;
  }

  /**
   * @return the column
   */
  public byte [] getColumn() {
    return this.column;
  }

  /**
   * @return the value
   */
  public byte[] getValue() {
    return this.value;
  }

  /**
   * @return True if this is a PUT operation (this.value is not null).
   */
  public boolean isPut() {
    return this.value != null;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "column => " + Bytes.toString(this.column) + ", value => '...'";
  }
  
  // Writable methods

  // This is a hotspot when updating deserializing incoming client submissions.
  // In Performance Evaluation sequentialWrite, 70% of object allocations are
  // done in here.
  public void readFields(final DataInput in) throws IOException {
    this.column = Bytes.readByteArray(in);
    // Is there a value to read?
    if (in.readBoolean()) {
      this.value = new byte[in.readInt()];
      in.readFully(this.value);
    }
  }

  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.column);
    boolean p = isPut();
    out.writeBoolean(p);
    if (p) {
      out.writeInt(value.length);
      out.write(value);
    }
  }
  
  public long heapSize() {
    return Bytes.ESTIMATED_HEAP_TAX * 2 + this.column.length +
      this.value.length + ESTIMATED_HEAP_TAX;
  }
}