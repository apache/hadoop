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
 * A log value.
 *
 * These aren't sortable; you need to sort by the matching HLogKey.
 * The table and row are already identified in HLogKey.
 * This just indicates the column and value.
 ******************************************************************************/
public class HLogEdit implements Writable {
  Text column = new Text();
  BytesWritable val = new BytesWritable();
  long timestamp;

  public HLogEdit() {
  }

  public HLogEdit(Text column, byte[] bval, long timestamp) {
    this.column.set(column);
    this.val = new BytesWritable(bval);
    this.timestamp = timestamp;
  }

  public Text getColumn() {
    return this.column;
  }

  public BytesWritable getVal() {
    return this.val;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    this.column.write(out);
    this.val.write(out);
    out.writeLong(timestamp);
  }
  
  public void readFields(DataInput in) throws IOException {
    this.column.readFields(in);
    this.val.readFields(in);
    this.timestamp = in.readLong();
  }
}

