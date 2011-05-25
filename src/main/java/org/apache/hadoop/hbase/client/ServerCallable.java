/**
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

package org.apache.hadoop.hbase.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.retry.RetryPolicy;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.Callable;

/**
 * Abstract class that implements Callable, used by retryable actions.
 * @param <T> the class that the ServerCallable handles
 */
public abstract class ServerCallable<T> implements Callable<T> {
  protected final HConnection connection;
  protected final byte [] tableName;
  protected final byte [] row;
  protected HRegionLocation location;
  protected HRegionInterface server;
  protected int callTimeout;
  protected long startTime, endTime;

  /**
   * @param connection connection callable is on
   * @param tableName table name callable is on
   * @param row row we are querying
   */
  public ServerCallable(HConnection connection, byte [] tableName, byte [] row) {
    this(connection, tableName, row, HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
  }

  public ServerCallable(HConnection connection, byte [] tableName, byte [] row, int callTimeout) {
    this.connection = connection;
    this.tableName = tableName;
    this.row = row;
    this.callTimeout = callTimeout;
  }
  /**
   *
   * @param reload set this to true if connection should re-find the region
   * @throws IOException e
   */
  public void instantiateServer(boolean reload) throws IOException {
    this.location = connection.getRegionLocation(tableName, row, reload);
    this.server = connection.getHRegionConnection(location.getServerAddress());
  }

  /** @return the server name */
  public String getServerName() {
    if (location == null) {
      return null;
    }
    return location.getServerAddress().toString();
  }

  /** @return the region name */
  public byte[] getRegionName() {
    if (location == null) {
      return null;
    }
    return location.getRegionInfo().getRegionName();
  }

  /** @return the row */
  public byte [] getRow() {
    return row;
  }

  public void beforeCall() {
    HBaseRPC.setRpcTimeout(this.callTimeout);
    this.startTime = System.currentTimeMillis();
  }

  public void afterCall() {
    HBaseRPC.resetRpcTimeout();
    this.endTime = System.currentTimeMillis();
  }

  public void shouldRetry(Throwable throwable) throws IOException {
    if (this.callTimeout != HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT)
      if (throwable instanceof SocketTimeoutException
          || (this.endTime - this.startTime > this.callTimeout)) {
        throw (SocketTimeoutException) (SocketTimeoutException) new SocketTimeoutException(
            "Call to access row '" + Bytes.toString(row) + "' on table '"
                + Bytes.toString(tableName)
                + "' failed on socket timeout exception: " + throwable)
            .initCause(throwable);
      } else {
        this.callTimeout = ((int) (this.endTime - this.startTime));
      }
  }
}