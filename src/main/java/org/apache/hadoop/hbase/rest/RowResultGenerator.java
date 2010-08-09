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

package org.apache.hadoop.hbase.rest;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;

public class RowResultGenerator extends ResultGenerator {
  private static final Log LOG = LogFactory.getLog(RowResultGenerator.class);

  private Iterator<KeyValue> valuesI;
  private KeyValue cache;

  public RowResultGenerator(final String tableName, final RowSpec rowspec,
      final Filter filter) throws IllegalArgumentException, IOException {
    HTablePool pool = RESTServlet.getInstance().getTablePool(); 
    HTableInterface table = pool.getTable(tableName);
    try {
      Get get = new Get(rowspec.getRow());
      if (rowspec.hasColumns()) {
        for (byte[] col: rowspec.getColumns()) {
          byte[][] split = KeyValue.parseColumn(col);
          if (split.length == 2 && split[1].length != 0) {
            get.addColumn(split[0], split[1]);
          } else {
            get.addFamily(split[0]);
          }
        }
      } else {
        // rowspec does not explicitly specify columns, return them all
        for (HColumnDescriptor family: 
            table.getTableDescriptor().getFamilies()) {
          get.addFamily(family.getName());
        }
      }
      get.setTimeRange(rowspec.getStartTime(), rowspec.getEndTime());
      get.setMaxVersions(rowspec.getMaxVersions());
      if (filter != null) {
        get.setFilter(filter);
      }
      Result result = table.get(get);
      if (result != null && !result.isEmpty()) {
        valuesI = result.list().iterator();
      }
    } catch (NoSuchColumnFamilyException e) {
      // Warn here because Stargate will return 404 in the case if multiple
      // column families were specified but one did not exist -- currently
      // HBase will fail the whole Get.
      // Specifying multiple columns in a URI should be uncommon usage but
      // help to avoid confusion by leaving a record of what happened here in
      // the log.
      LOG.warn(StringUtils.stringifyException(e));
    } finally {
      pool.putTable(table);
    }
  }

  public void close() {
  }

  public boolean hasNext() {
    if (cache != null) {
      return true;
    }
    if (valuesI == null) {
      return false;
    }
    return valuesI.hasNext();
  }

  public KeyValue next() {
    if (cache != null) {
      KeyValue kv = cache;
      cache = null;
      return kv;
    }
    if (valuesI == null) {
      return null;
    }
    try {
      return valuesI.next();
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  public void putBack(KeyValue kv) {
    this.cache = kv;
  }

  public void remove() {
    throw new UnsupportedOperationException("remove not supported");
  }
}
