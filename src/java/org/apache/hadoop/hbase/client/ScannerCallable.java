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

package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.RowResult;


/**
 * Retries scanner operations such as create, next, etc.
 * Used by {@link Scanner}s made by {@link HTable}.
 */
public class ScannerCallable extends ServerCallable<RowResult[]> {
  private long scannerId = -1L;
  private boolean instantiated = false;
  private boolean closed = false;
  private final byte [][] columns;
  private final long timestamp;
  private final RowFilterInterface filter;
  private int caching = 1;

  /**
   * @param connection
   * @param tableName
   * @param columns
   * @param startRow
   * @param timestamp
   * @param filter
   */
  public ScannerCallable (HConnection connection, byte [] tableName, byte [][] columns,
      byte [] startRow, long timestamp, RowFilterInterface filter) {
    super(connection, tableName, startRow);
    this.columns = columns;
    this.timestamp = timestamp;
    this.filter = filter;
  }
  
  /**
   * @param reload
   * @throws IOException
   */
  @Override
  public void instantiateServer(boolean reload) throws IOException {
    if (!instantiated || reload) {
      super.instantiateServer(reload);
      instantiated = true;
    }
  }

  /**
   * @see java.util.concurrent.Callable#call()
   */
  public RowResult[] call() throws IOException {
    if (scannerId != -1L && closed) {
      server.close(scannerId);
      scannerId = -1L;
    } else if (scannerId == -1L && !closed) {
      // open the scanner
      scannerId = openScanner();
    } else {
      RowResult [] rrs = server.next(scannerId, caching);
      return rrs.length == 0 ? null : rrs;
    }
    return null;
  }
  
  protected long openScanner() throws IOException {
    return server.openScanner(
        this.location.getRegionInfo().getRegionName(), columns, row,
        timestamp, filter);
  }
  
  protected byte [][] getColumns() {
    return columns;
  }
  
  protected long getTimestamp() {
    return timestamp;
  }
  
  protected RowFilterInterface getFilter() {
    return filter;
  }
  
  /**
   * Call this when the next invocation of call should close the scanner
   */
  public void setClose() {
    closed = true;
  }
  
  /**
   * @return the HRegionInfo for the current region
   */
  public HRegionInfo getHRegionInfo() {
    if (!instantiated) {
      return null;
    }
    return location.getRegionInfo();
  }

  /**
   * Get the number of rows that will be fetched on next
   * @return the number of rows for caching
   */
  public int getCaching() {
    return caching;
  }

  /**
   * Set the number of rows that will be fetched on next
   * @param caching the number of rows for caching
   */
  public void setCaching(int caching) {
    this.caching = caching;
  }
}