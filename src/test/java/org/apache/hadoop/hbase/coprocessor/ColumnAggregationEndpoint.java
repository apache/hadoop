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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * The aggregation implementation at a region.
 */
public class ColumnAggregationEndpoint extends BaseEndpointCoprocessor
implements ColumnAggregationProtocol {

  @Override
  public long sum(byte[] family, byte[] qualifier)
  throws IOException {
    // aggregate at each region
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    int sumResult = 0;

    InternalScanner scanner = ((RegionCoprocessorEnvironment)getEnvironment())
        .getRegion().getScanner(scan);
    try {
      List<KeyValue> curVals = new ArrayList<KeyValue>();
      boolean done = false;
      do {
        curVals.clear();
        done = scanner.next(curVals);
        KeyValue kv = curVals.get(0);
        sumResult += Bytes.toInt(kv.getValue());
      } while (done);
    } finally {
      scanner.close();
    }
    return sumResult;
  }
}
