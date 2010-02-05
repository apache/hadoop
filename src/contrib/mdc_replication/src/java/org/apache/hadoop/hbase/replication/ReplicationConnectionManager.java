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
package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.ipc.ReplicationRegionInterface;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Connection manager to communicate with the other clusters.
 */
public class ReplicationConnectionManager implements HConstants {

  private final int numRetries;
  private final int maxRPCAttempts;
  private final long rpcTimeout;
  private final Map<String, ReplicationRegionInterface> servers =
      new ConcurrentHashMap<String, ReplicationRegionInterface>();
  private final
    Class<? extends ReplicationRegionInterface> serverInterfaceClass;
  private final Configuration conf;

  /**
   * Constructor that sets up RPC to other clusters
   * @param conf
   */
  public ReplicationConnectionManager(Configuration conf) {
    this.conf = conf;
    String serverClassName =
        conf.get(REGION_SERVER_CLASS, DEFAULT_REGION_SERVER_CLASS);
    this.numRetries = conf.getInt("hbase.client.retries.number", 10);
    this.maxRPCAttempts = conf.getInt("hbase.client.rpc.maxattempts", 1);
    this.rpcTimeout = conf.getLong("hbase.regionserver.lease.period", 60000);
    try {
      this.serverInterfaceClass =
        (Class<? extends ReplicationRegionInterface>)
            Class.forName(serverClassName);
    } catch (ClassNotFoundException e) {
      throw new UnsupportedOperationException(
          "Unable to find region server interface " + serverClassName, e);
    }
  }

  /**
   * Get a connection to a distant region server for replication
   * @param regionServer the address to use
   * @return the connection to the region server
   * @throws IOException
   */
  public ReplicationRegionInterface getHRegionConnection(
      HServerAddress regionServer)
      throws IOException {
    ReplicationRegionInterface server;
    synchronized (this.servers) {
      // See if we already have a connection
      server = this.servers.get(regionServer.toString());
      if (server == null) { // Get a connection
        try {
          server = (ReplicationRegionInterface) HBaseRPC.waitForProxy(
              serverInterfaceClass, HBaseRPCProtocolVersion.versionID,
              regionServer.getInetSocketAddress(), this.conf,
              this.maxRPCAttempts, this.rpcTimeout);
        } catch (RemoteException e) {
          throw RemoteExceptionHandler.decodeRemoteException(e);
        }
        this.servers.put(regionServer.toString(), server);
      }
    }
    return server;
  }
}
