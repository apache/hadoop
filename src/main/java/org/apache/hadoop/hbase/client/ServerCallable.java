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

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Abstract class that implements {@link Callable}.  Implementation stipulates
 * return type and method we actually invoke on remote Server.  Usually
 * used inside a try/catch that fields usual connection failures all wrapped
 * up in a retry loop.
 * <p>Call {@link #connect(boolean)} to connect to server hosting region
 * that contains the passed row in the passed table before invoking
 * {@link #call()}.
 * @see HConnection#getRegionServerWithoutRetries(ServerCallable)
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
   * @param connection Connection to use.
   * @param tableName Table name to which <code>row</code> belongs.
   * @param row The row we want in <code>tableName</code>.
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
   * Connect to the server hosting region with row from tablename.
   * @param reload Set this to true if connection should re-find the region
   * @throws IOException e
   */
  public void connect(final boolean reload) throws IOException {
    this.location = connection.getRegionLocation(tableName, row, reload);
    this.server = connection.getHRegionConnection(location.getHostname(),
      location.getPort());
  }

  /** @return the server name
   * @deprecated Just use {@link #toString()} instead.
   */
  public String getServerName() {
    if (location == null) return null;
    return location.getHostnamePort();
  }

  /** @return the region name
   * @deprecated Just use {@link #toString()} instead.
   */
  public byte[] getRegionName() {
    if (location == null) return null;
    return location.getRegionInfo().getRegionName();
  }

  /** @return the row
   * @deprecated Just use {@link #toString()} instead.
   */
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