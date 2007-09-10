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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Batch update operations such as put, delete, and deleteAll.
 */
public class BatchOperation implements Writable {
  /** 
   * Operation types.
   * @see org.apache.hadoop.io.SequenceFile.Writer
   */
  public static enum Operation {PUT, DELETE}

  private Operation op;
  private Text column;
  private byte[] value;
  
  /** default constructor used by Writable */
  public BatchOperation() {
    this(new Text());
  }
  /**
   * Creates a DELETE operation
   * 
   * @param column column name
   */
  public BatchOperation(final Text column) {
    this(Operation.DELETE, column, null);
  }

  /**
   * Creates a PUT operation
   * 
   * @param column column name
   * @param value column value
   */
  public BatchOperation(final Text column, final byte [] value) {
    this(Operation.PUT, column, value);
  }
  
  /**
   * Creates a put operation
   * 
   * @param column column name
   * @param value column value
   */
  public BatchOperation(final Operation operation, final Text column,
      final byte[] value) {
    this.op = operation;
    this.column = column;
    this.value = value;
  }

  /**
   * @return the column
   */
  public Text getColumn() {
    return column;
  }

  /**
   * @return the operation
   */
  public Operation getOp() {
    return this.op;
  }

  /**
   * @return the value
   */
  public byte[] getValue() {
    return value;
  }
  
  //
  // Writable
  //

  /**
   * {@inheritDoc}
   */
  public void readFields(DataInput in) throws IOException {
    int ordinal = in.readInt();
    this.op = Operation.values()[ordinal];
    column.readFields(in);
    if (this.op == Operation.PUT) {
      value = new byte[in.readInt()];
      in.readFully(value);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.op.ordinal());
    column.write(out);
    if (this.op == Operation.PUT) {
      out.writeInt(value.length);
      out.write(value);
    }
  }
}