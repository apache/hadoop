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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;


/**
 * Used by server processes to expose HServerConnection method
 * so can call HConnectionManager#setRootRegionLocation
 */
public class ServerConnectionManager extends HConnectionManager {
  /*
   * Not instantiable
   */
  private ServerConnectionManager() {}

  /**
   * Get the connection object for the instance specified by the configuration
   * If no current connection exists, create a new connection for that instance
   * @param conf configuration
   * @return HConnection object for the instance specified by the configuration
   * @throws ZooKeeperConnectionException
   */
  public static ServerConnection getConnection(Configuration conf) throws ZooKeeperConnectionException {
    return (ServerConnection) HConnectionManager.getConnection(conf);
  }
}
