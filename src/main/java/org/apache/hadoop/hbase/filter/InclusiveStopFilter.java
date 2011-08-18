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
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import com.google.common.base.Preconditions;

/**
 * A Filter that stops after the given row.  There is no "RowStopFilter" because
 * the Scan spec allows you to specify a stop row.
 *
 * Use this filter to include the stop row, eg: [A,Z].
 */
public class InclusiveStopFilter extends FilterBase {
  private byte [] stopRowKey;
  private boolean done = false;

  public InclusiveStopFilter() {
    super();
  }

  public InclusiveStopFilter(final byte [] stopRowKey) {
    this.stopRowKey = stopRowKey;
  }

  public byte[] getStopRowKey() {
    return this.stopRowKey;
  }

  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    if (buffer == null) {
      //noinspection RedundantIfStatement
      if (this.stopRowKey == null) {
        return true; //filter...
      }
      return false;
    }
    // if stopRowKey is <= buffer, then true, filter row.
    int cmp = Bytes.compareTo(stopRowKey, 0, stopRowKey.length,
      buffer, offset, length);

    if(cmp < 0) {
      done = true;
    }
    return done;
  }

  public boolean filterAllRemaining() {
    return done;
  }

  public static Filter createFilterFromArguments (ArrayList<byte []> filterArguments) {
    Preconditions.checkArgument(filterArguments.size() == 1,
                                "Expected 1 but got: %s", filterArguments.size());
    byte [] stopRowKey = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
    return new InclusiveStopFilter(stopRowKey);
  }

  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.stopRowKey);
  }

  public void readFields(DataInput in) throws IOException {
    this.stopRowKey = Bytes.readByteArray(in);
  }
}