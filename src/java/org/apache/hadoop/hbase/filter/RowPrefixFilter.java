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

public class RowPrefixFilter implements Filter {

  protected byte [] prefix;

  public RowPrefixFilter(final byte [] prefix) {
    this.prefix = prefix;
  }

  public RowPrefixFilter() {
  }

  @Override
  public void reset() {
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    if (buffer == null)
      return true;
    if (length < prefix.length)
      return true;
    // if they are equal, return false => pass row
    // else return true, filter row
    return Bytes.compareTo(buffer, offset, prefix.length, prefix, 0, prefix.length) != 0;
  }

  @Override
  public boolean filterAllRemaining() {
    return false;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    return ReturnCode.INCLUDE;
  }

  @Override
  public boolean filterRow() {
    return false;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, prefix);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    prefix = Bytes.readByteArray(in);
  }
}
