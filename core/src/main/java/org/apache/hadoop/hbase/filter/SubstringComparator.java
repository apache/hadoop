/**
 * Copyright 2008 The Apache Software Foundation
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * This comparator is for use with ColumnValueFilter, for filtering based on
 * the value of a given column. Use it to test if a given substring appears
 * in a cell value in the column. The comparison is case insensitive.
 * <p>
 * Only EQUAL or NOT_EQUAL tests are valid with this comparator. 
 * <p>
 * For example:
 * <p>
 * <pre>
 * ColumnValueFilter cvf =
 *   new ColumnValueFilter("col", ColumnValueFilter.CompareOp.EQUAL,
 *     new SubstringComparator("substr"));
 * </pre>
 */
public class SubstringComparator extends WritableByteArrayComparable {

  private String substr;

  /** Nullary constructor for Writable, do not use */
  public SubstringComparator() {
    super();
  }

  /**
   * Constructor
   * @param substr the substring
   */
  public SubstringComparator(String substr) {
    super(Bytes.toBytes(substr.toLowerCase()));
    this.substr = substr.toLowerCase();
  }

  @Override
  public byte[] getValue() {
    return Bytes.toBytes(substr);
  }

  @Override
  public int compareTo(byte[] value) {
    return Bytes.toString(value).toLowerCase().contains(substr) ? 0 : 1;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String substr = in.readUTF();
    this.value = Bytes.toBytes(substr);
    this.substr = substr;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(substr);
  }

}
