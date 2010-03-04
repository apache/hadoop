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

package org.apache.hadoop.hbase.stargate;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.stargate.model.ScannerModel;
import org.apache.hadoop.util.StringUtils;

public class ScannerResultGenerator extends ResultGenerator {

  private static final Log LOG =
    LogFactory.getLog(ScannerResultGenerator.class);

  public static Filter buildFilterFromModel(ScannerModel model) 
      throws Exception {
    String filter = model.getFilter();
    if (filter == null || filter.length() == 0) {
      return null;
    }
    return buildFilter(filter);
  }

  private String id;
  private Iterator<KeyValue> rowI;
  private ResultScanner scanner;
  private Result cached;

  public ScannerResultGenerator(final String tableName, final RowSpec rowspec,
      final Filter filter) throws IllegalArgumentException, IOException {
    HTablePool pool = RESTServlet.getInstance().getTablePool(); 
    HTableInterface table = pool.getTable(tableName);
    try {
      Scan scan;
      if (rowspec.hasEndRow()) {
        scan = new Scan(rowspec.getStartRow(), rowspec.getEndRow());
      } else {
        scan = new Scan(rowspec.getStartRow());
      }
      if (rowspec.hasColumns()) {
        byte[][] columns = rowspec.getColumns();
        for (byte[] column: columns) {
          byte[][] split = KeyValue.parseColumn(column);
          if (split.length > 1 && (split[1] != null && split[1].length != 0)) {
            scan.addColumn(split[0], split[1]);
          } else {
            scan.addFamily(split[0]);
          }
        }
      } else {
        for (HColumnDescriptor family: 
            table.getTableDescriptor().getFamilies()) {
          scan.addFamily(family.getName());
        }
      }
      scan.setTimeRange(rowspec.getStartTime(), rowspec.getEndTime());          
      scan.setMaxVersions(rowspec.getMaxVersions());
      if (filter != null) {
        scan.setFilter(filter);
      }
      // always disable block caching on the cluster
      scan.setCacheBlocks(false);
      scanner = table.getScanner(scan);
      cached = null;
      id = Long.toString(System.currentTimeMillis()) +
             Integer.toHexString(scanner.hashCode());
    } finally {
      pool.putTable(table);
    }
  }

  public String getID() {
    return id;
  }

  public void close() {
  }

  public boolean hasNext() {
    if (rowI != null && rowI.hasNext()) {
      return true;
    }
    if (cached != null) {
      return true;
    }
    try {
      Result result = scanner.next();
      if (result != null && !result.isEmpty()) {
        cached = result;
      }
    } catch (UnknownScannerException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    return cached != null;
  }

  public KeyValue next() {
    boolean loop;
    do {
      loop = false;
      if (rowI != null) {
        if (rowI.hasNext()) {
          return rowI.next();
        } else {
          rowI = null;
        }
      }
      if (cached != null) {
        rowI = cached.list().iterator();
        loop = true;
        cached = null;
      } else {
        Result result = null;
        try {
          result = scanner.next();
        } catch (UnknownScannerException e) {
          throw new IllegalArgumentException(e);
        } catch (IOException e) {
          LOG.error(StringUtils.stringifyException(e));
        }
        if (result != null && !result.isEmpty()) {
          rowI = result.list().iterator();
          loop = true;
        }
      }
    } while (loop);
    return null;
  }

  public void remove() {
    throw new UnsupportedOperationException("remove not supported");
  }

}
