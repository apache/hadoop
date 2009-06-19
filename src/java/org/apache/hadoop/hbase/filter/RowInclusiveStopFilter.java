/*
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

package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;

/**
 * A Filter that stops after the given row.  There is no "RowStopFilter" because
 * the Scan spec allows you to specify a stop row.
 *
 * Use this filter to include the stop row, eg: [A,Z].
 */
public class RowInclusiveStopFilter implements Filter {
  private byte [] stopRowKey;

  public RowInclusiveStopFilter() {
    super();
  }

  public RowInclusiveStopFilter(final byte [] stopRowKey) {
    this.stopRowKey = stopRowKey;
  }

  @Override
  public void reset() {
    // noop, no state
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    if (buffer == null) {
      if (this.stopRowKey == null) {
        return true; //filter...
      }
      return false;
    }
    // if stopRowKey is <= buffer, then true, filter row.
    return Bytes.compareTo(stopRowKey, 0, stopRowKey.length, buffer, offset, length) < 0;
  }

  @Override
  public boolean filterAllRemaining() {
    return false;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    // include everything.
    return ReturnCode.INCLUDE;
  }

  @Override
  public boolean filterRow() {
    return false;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.stopRowKey);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.stopRowKey = Bytes.readByteArray(in);
  }
}