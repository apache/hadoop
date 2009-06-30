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
 * Pass results that have same row prefix.
 */
public class PrefixFilter implements Filter {
  protected byte [] prefix = null;

  public PrefixFilter(final byte [] prefix) {
    this.prefix = prefix;
  }

  public PrefixFilter() {
    super();
  }

  public void reset() {
    // Noop
  }

  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    if (buffer == null || this.prefix == null)
      return true;
    if (length < prefix.length)
      return true;
    // if they are equal, return false => pass row
    // else return true, filter row
    return Bytes.compareTo(buffer, offset, this.prefix.length, this.prefix, 0,
      this.prefix.length) != 0;
  }

  public boolean filterAllRemaining() {
    return false;
  }

  public ReturnCode filterKeyValue(KeyValue v) {
    return ReturnCode.INCLUDE;
  }

  public boolean filterRow() {
    return false;
  }

  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.prefix);
  }

  public void readFields(DataInput in) throws IOException {
    this.prefix = Bytes.readByteArray(in);
  }
}