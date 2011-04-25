/*
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client.coprocessor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * a concrete column interpreter implementation. The cell value is a Long value
 * and its promoted data type is also a Long value. For computing aggregation
 * function, this class is used to find the datatype of the cell value. Client
 * is supposed to instantiate it and passed along as a parameter. See
 * {@link TestAggregateProtocol} methods for its sample usage.
 * Its methods handle null arguments gracefully. 
 */
public class LongColumnInterpreter implements ColumnInterpreter<Long, Long> {

  public Long getValue(byte[] colFamily, byte[] colQualifier, KeyValue kv)
      throws IOException {
    if (kv == null || kv.getValue().length != Bytes.SIZEOF_LONG)
      return null;
    return Bytes.toLong(kv.getValue());
  }

   @Override
  public Long add(Long l1, Long l2) {
    if (l1 == null ^ l2 == null) {
      return (l1 == null) ? l2 : l1; // either of one is null.
    } else if (l1 == null) // both are null
      return null;
    return l1 + l2;
  }

  @Override
  public int compare(final Long l1, final Long l2) {
    if (l1 == null ^ l2 == null) {
      return l1 == null ? -1 : 1; // either of one is null.
    } else if (l1 == null)
      return 0; // both are null
    return l1.compareTo(l2); // natural ordering.
  }

  @Override
  public Long getMaxValue() {
    return Long.MAX_VALUE;
  }

  @Override
  public Long increment(Long o) {
    return o == null ? null : (o + 1l);
  }

  @Override
  public Long multiply(Long l1, Long l2) {
    return (l1 == null || l2 == null) ? null : l1 * l2;
  }

  @Override
  public Long getMinValue() {
    return Long.MIN_VALUE;
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    // nothing to serialize
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
     // nothing to serialize
  }

  @Override
  public double divideForAvg(Long l1, Long l2) {
    return (l2 == null || l1 == null) ? Double.NaN : (l1.doubleValue() / l2
        .doubleValue());
  }

  @Override
  public Long castToReturnType(Long o) {
    return o;
  }

}