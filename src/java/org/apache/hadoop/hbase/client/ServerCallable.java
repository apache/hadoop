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
import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.io.Text;

/**
 * Implements Callable, used to define the particular actions you would
 * like to take with retry logic.
 * @param <T> the class that the ServerCallable handles
 * 
 */
public abstract class ServerCallable<T> implements Callable<T> {
  protected final HConnection connection;
  protected final Text tableName;
  protected final Text row;
  protected HRegionLocation location;
  protected HRegionInterface server;

  /**
   * @param connection
   * @param tableName
   * @param row
   */
  public ServerCallable(HConnection connection, Text tableName, Text row) {
    this.connection = connection;
    this.tableName = tableName;
    this.row = row;
  }
  
  /**
   * 
   * @param reload set this to true if connection should re-find the region
   * @throws IOException
   */
  public void instantiateServer(boolean reload) throws IOException {
    this.location = connection.getRegionLocation(tableName, row, reload);
    this.server = connection.getHRegionConnection(location.getServerAddress());
  }

  /** @return the server name */
  public String getServerName() {
    return location.getServerAddress().toString();
  }
  
  /** @return the region name */
  public Text getRegionName() {
    return location.getRegionInfo().getRegionName();
  }
  
  /** @return the row */
  public Text getRow() {
    return row;
  }
}
