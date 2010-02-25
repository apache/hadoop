/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client.tableindexed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Creates index row keys which exactly match the indexed column. This allows a
 * direct get() lookup on the index table, but at the cost that the column
 * values must be unique.
 * 
 * If you are indexing a column which can have duplicated values, consider
 * {@link SimpleIndexKeyGenerator}.
 */
public class UniqueIndexKeyGenerator implements IndexKeyGenerator {
  private byte[] column;

  /**
   * @param column the column to index
   */
  public UniqueIndexKeyGenerator(byte[] column) {
    this.column = column;
  }

  public UniqueIndexKeyGenerator() {
    // For Writable
  }

  /** {@inheritDoc} */
  public byte[] createIndexKey(byte[] rowKey, Map<byte[], byte[]> columns) {
    return columns.get(column).clone();
  }

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    column = Bytes.readByteArray(in);
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, column);
  }

}
