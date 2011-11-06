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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Pair;

/**
 * A concrete AggregateProtocol implementation. Its system level coprocessor
 * that computes the aggregate function at a region level.
 */
public class AggregateImplementation extends BaseEndpointCoprocessor implements
    AggregateProtocol {
  protected static Log log = LogFactory.getLog(AggregateImplementation.class);

  @Override
  public ProtocolSignature getProtocolSignature(
      String protocol, long version, int clientMethodsHashCode)
  throws IOException {
    if (AggregateProtocol.class.getName().equals(protocol)) {
      return new ProtocolSignature(AggregateProtocol.VERSION, null);
    }
    throw new IOException("Unknown protocol: " + protocol);
  }

  @Override
  public <T, S> T getMax(ColumnInterpreter<T, S> ci, Scan scan)
      throws IOException {
    T temp;
    T max = null;
    InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
        .getRegion().getScanner(scan);
    List<KeyValue> results = new ArrayList<KeyValue>();
    byte[] colFamily = scan.getFamilies()[0];
    byte[] qualifier = scan.getFamilyMap().get(colFamily).pollFirst();
    // qualifier can be null.
    try {
      boolean hasMoreRows = false;
      do {
        hasMoreRows = scanner.next(results);
        for (KeyValue kv : results) {
          temp = ci.getValue(colFamily, qualifier, kv);
          max = (max == null || ci.compare(temp, max) > 0) ? temp : max;
        }
        results.clear();
      } while (hasMoreRows);
    } finally {
      scanner.close();
    }
    log.info("Maximum from this region is "
        + ((RegionCoprocessorEnvironment) getEnvironment()).getRegion()
            .getRegionNameAsString() + ": " + max);
    return max;
  }

  @Override
  public <T, S> T getMin(ColumnInterpreter<T, S> ci, Scan scan)
      throws IOException {
    T min = null;
    T temp;
    InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
        .getRegion().getScanner(scan);
    List<KeyValue> results = new ArrayList<KeyValue>();
    byte[] colFamily = scan.getFamilies()[0];
    byte[] qualifier = scan.getFamilyMap().get(colFamily).pollFirst();
    try {
      boolean hasMoreRows = false;
      do {
        hasMoreRows = scanner.next(results);
        for (KeyValue kv : results) {
          temp = ci.getValue(colFamily, qualifier, kv);
          min = (min == null || ci.compare(temp, min) < 0) ? temp : min;
        }
        results.clear();
      } while (hasMoreRows);
    } finally {
      scanner.close();
    }
    log.info("Minimum from this region is "
        + ((RegionCoprocessorEnvironment) getEnvironment()).getRegion()
            .getRegionNameAsString() + ": " + min);
    return min;
  }

  @Override
  public <T, S> S getSum(ColumnInterpreter<T, S> ci, Scan scan)
      throws IOException {
    long sum = 0l;
    S sumVal = null;
    T temp;
    InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
        .getRegion().getScanner(scan);
    byte[] colFamily = scan.getFamilies()[0];
    byte[] qualifier = scan.getFamilyMap().get(colFamily).pollFirst();
    List<KeyValue> results = new ArrayList<KeyValue>();
    try {
      boolean hasMoreRows = false;
      do {
        hasMoreRows = scanner.next(results);
        for (KeyValue kv : results) {
          temp = ci.getValue(colFamily, qualifier, kv);
          if (temp != null)
            sumVal = ci.add(sumVal, ci.castToReturnType(temp));
        }
        results.clear();
      } while (hasMoreRows);
    } finally {
      scanner.close();
    }
    log.debug("Sum from this region is "
        + ((RegionCoprocessorEnvironment) getEnvironment()).getRegion()
            .getRegionNameAsString() + ": " + sum);
    return sumVal;
  }

  @Override
  public <T, S> long getRowNum(ColumnInterpreter<T, S> ci, Scan scan)
      throws IOException {
    long counter = 0l;
    List<KeyValue> results = new ArrayList<KeyValue>();
    byte[] colFamily = scan.getFamilies()[0];
    byte[] qualifier = scan.getFamilyMap().get(colFamily).pollFirst();
    if (scan.getFilter() == null && qualifier == null)
      scan.setFilter(new FirstKeyOnlyFilter());
    InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
        .getRegion().getScanner(scan);
    try {
      boolean hasMoreRows = false;
      do {
        hasMoreRows = scanner.next(results);
        if (results.size() > 0) {
          counter++;
        }
        results.clear();
      } while (hasMoreRows);
    } finally {
      scanner.close();
    }
    log.info("Row counter from this region is "
        + ((RegionCoprocessorEnvironment) getEnvironment()).getRegion()
            .getRegionNameAsString() + ": " + counter);
    return counter;
  }

  @Override
  public <T, S> Pair<S, Long> getAvg(ColumnInterpreter<T, S> ci, Scan scan)
      throws IOException {
    S sumVal = null;
    Long rowCountVal = 0l;
    InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
        .getRegion().getScanner(scan);
    byte[] colFamily = scan.getFamilies()[0];
    byte[] qualifier = scan.getFamilyMap().get(colFamily).pollFirst();
    List<KeyValue> results = new ArrayList<KeyValue>();
    boolean hasMoreRows = false;
    try {
      do {
        results.clear();
        hasMoreRows = scanner.next(results);
        for (KeyValue kv : results) {
          sumVal = ci.add(sumVal, ci.castToReturnType(ci.getValue(colFamily,
              qualifier, kv)));
        }
        rowCountVal++;
      } while (hasMoreRows);
    } finally {
      scanner.close();
    }
    Pair<S, Long> pair = new Pair<S, Long>(sumVal, rowCountVal);
    return pair;
  }

  @Override
  public <T, S> Pair<List<S>, Long> getStd(ColumnInterpreter<T, S> ci, Scan scan)
      throws IOException {
    S sumVal = null, sumSqVal = null, tempVal = null;
    long rowCountVal = 0l;
    InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
        .getRegion().getScanner(scan);
    byte[] colFamily = scan.getFamilies()[0];
    byte[] qualifier = scan.getFamilyMap().get(colFamily).pollFirst();
    List<KeyValue> results = new ArrayList<KeyValue>();

    boolean hasMoreRows = false;
    try {
      do {
        tempVal = null;
        hasMoreRows = scanner.next(results);
        for (KeyValue kv : results) {
          tempVal = ci.add(tempVal, ci.castToReturnType(ci.getValue(colFamily,
              qualifier, kv)));
        }
        results.clear();
        sumVal = ci.add(sumVal, tempVal);
        sumSqVal = ci.add(sumSqVal, ci.multiply(tempVal, tempVal));
        rowCountVal++;
      } while (hasMoreRows);
    } finally {
      scanner.close();
    }
    List<S> l = new ArrayList<S>();
    l.add(sumVal);
    l.add(sumSqVal);
    Pair<List<S>, Long> p = new Pair<List<S>, Long>(l, rowCountVal);
    return p;
  }

}
