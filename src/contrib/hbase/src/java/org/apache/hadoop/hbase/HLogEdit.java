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

import org.apache.hadoop.io.*;

import java.io.*;

/*******************************************************************************
 * A log value.
 *
 * These aren't sortable; you need to sort by the matching HLogKey.
 * The table and row are already identified in HLogKey.
 * This just indicates the column and value.
 ******************************************************************************/
public class HLogEdit implements Writable {
  private Text column = new Text();
  private byte [] val;
  private long timestamp;

  public HLogEdit() {
    super();
  }

  public HLogEdit(Text column, byte [] bval, long timestamp) {
    this.column.set(column);
    this.val = bval;
    this.timestamp = timestamp;
  }

  public Text getColumn() {
    return this.column;
  }

  public byte [] getVal() {
    return this.val;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  @Override
  public String toString() {
    return getColumn().toString() + " " + this.getTimestamp() + " " +
      new String(getVal()).trim();
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    this.column.write(out);
    out.writeShort(this.val.length);
    out.write(this.val);
    out.writeLong(timestamp);
  }
  
  public void readFields(DataInput in) throws IOException {
    this.column.readFields(in);
    this.val = new byte[in.readShort()];
    in.readFully(this.val);
    this.timestamp = in.readLong();
  }
}