/**
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** Base class, combines Comparable<byte []> and Writable. */
public abstract class WritableByteArrayComparable implements Writable, Comparable<byte[]> {

  byte[] value;

  /**
   * Nullary constructor, for Writable
   */
  public WritableByteArrayComparable() { }

  /**
   * Constructor.
   * @param value the value to compare against
   */
  public WritableByteArrayComparable(byte [] value) {
    this.value = value;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    value = Bytes.readByteArray(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, value);
  }

  @Override
  public int compareTo(byte [] value) {
    return Bytes.compareTo(this.value, value);
  }

}
