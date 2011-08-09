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

import org.apache.hadoop.hbase.KeyValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Simple filter that returns first N columns on row only.
 * This filter was written to test filters in Get and as soon as it gets
 * its quota of columns, {@link #filterAllRemaining()} returns true.  This
 * makes this filter unsuitable as a Scan filter.
 */
public class ColumnCountGetFilter extends FilterBase {
  private int limit = 0;
  private int count = 0;

  /**
   * Used during serialization.
   * Do not use.
   */
  public ColumnCountGetFilter() {
    super();
  }

  public ColumnCountGetFilter(final int n) {
    if (n < 0) {
      throw new IllegalArgumentException("Limit must not be negative");
    }
    this.limit = n;
  }

  public int getLimit() {
    return limit;
  }

  @Override
  public boolean filterAllRemaining() {
    return this.count > this.limit;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    this.count++;
    return filterAllRemaining() ? ReturnCode.SKIP: ReturnCode.INCLUDE;
  }

  @Override
  public void reset() {
    this.count = 0;
  }

  @Override
  public Filter createFilterFromArguments (ArrayList<byte []> filterArguments) {
    if (filterArguments.size() != 1) {
      throw new IllegalArgumentException("Incorrect Arguments passed to ColumnCountGetFilter. " +
                                         "Expected: 1 but got: " + filterArguments.size());
    }
    int limit = ParseFilter.convertByteArrayToInt(filterArguments.get(0));
    return new ColumnCountGetFilter(limit);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.limit = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.limit);
  }
}