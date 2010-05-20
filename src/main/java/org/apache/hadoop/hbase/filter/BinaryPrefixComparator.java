/*
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

/**
 * A comparator which compares against a specified byte array, but only compares
 * up to the length of this byte array. For the rest it is similar to
 * {@link BinaryComparator}.
 */
public class BinaryPrefixComparator extends WritableByteArrayComparable {

  /** Nullary constructor for Writable, do not use */
  public BinaryPrefixComparator() { }

  /**
   * Constructor
   * @param value value
   */
  public BinaryPrefixComparator(byte[] value) {
    super(value);
  }

  @Override
  public int compareTo(byte [] value) {
    return Bytes.compareTo(this.value, 0, this.value.length, value, 0,
      this.value.length);
  }

}
