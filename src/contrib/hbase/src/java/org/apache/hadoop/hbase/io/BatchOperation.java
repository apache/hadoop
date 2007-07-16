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
 * batch update operation
 */
public class BatchOperation implements Writable {
  /** put operation */
  public static final int PUT_OP = 1;
  
  /** delete operation */
  public static final int DELETE_OP = 2;
  
  private int op;
  private Text column;
  private byte[] value;
  
  /** default constructor used by Writable */
  public BatchOperation() {
    this.op = 0;
    this.column = new Text();
    this.value = null;
  }
  
  /**
   * Creates a put operation
   * 
   * @param column column name
   * @param value column value
   */
  public BatchOperation(Text column, byte[] value) {
    this.op = PUT_OP;
    this.column = column;
    this.value = value;
  }
  
  /**
   * Creates a delete operation
   * 
   * @param column name of column to delete
   */
  public BatchOperation(Text column) {
    this.op = DELETE_OP;
    this.column = column;
    this.value = null;
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
  public int getOp() {
    return op;
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
    op = in.readInt();
    column.readFields(in);
    if(op == PUT_OP) {
      value = new byte[in.readInt()];
      in.readFully(value);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(op);
    column.write(out);
    if(op == PUT_OP) {
      out.writeInt(value.length);
      out.write(value);
    }
  }
}
