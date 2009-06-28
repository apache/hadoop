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

package org.apache.hadoop.hbase.stargate;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;

public class RowResultGenerator extends ResultGenerator {
  private Iterator<KeyValue> valuesI;

  public RowResultGenerator(String tableName, RowSpec rowspec)
      throws IllegalArgumentException, IOException {
    HTablePool pool = RESTServlet.getInstance().getTablePool(tableName); 
    HTable table = pool.get();
    try {
      Get get = new Get(rowspec.getRow());
      if (rowspec.hasColumns()) {
        get.addColumns(rowspec.getColumns());
      } else {
        // rowspec does not explicitly specify columns, return them all
        for (HColumnDescriptor family: 
            table.getTableDescriptor().getFamilies()) {
          get.addFamily(family.getName());
        }
      }
      get.setTimeRange(rowspec.getStartTime(), rowspec.getEndTime());
      get.setMaxVersions(rowspec.getMaxVersions());
      Result result = table.get(get);
      if (result != null && !result.isEmpty()) {
        valuesI = result.list().iterator();
      }
    } finally {
      pool.put(table);
    }
  }

  public void close() {
  }

  public boolean hasNext() {
    if (valuesI == null) {
      return false;
    }
    return valuesI.hasNext();
  }

  public KeyValue next() {
    if (valuesI == null) {
      return null;
    }
    try {
      return valuesI.next();
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  public void remove() {
    throw new UnsupportedOperationException("remove not supported");
  }
}
