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
package org.apache.hadoop.hbase.master.handler;

import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.DeadServer;
import org.apache.hadoop.hbase.master.MasterServices;

/**
 * Shutdown handler for the server hosting <code>-ROOT-</code>,
 * <code>.META.</code>, or both.
 */
public class MetaServerShutdownHandler extends ServerShutdownHandler {
  private final boolean carryingRoot;
  private final boolean carryingMeta;

  public MetaServerShutdownHandler(final Server server,
      final MasterServices services,
      final DeadServer deadServers, final ServerName serverName,
      final boolean carryingRoot, final boolean carryingMeta) {
    super(server, services, deadServers, serverName,
      EventType.M_META_SERVER_SHUTDOWN, true);
    this.carryingRoot = carryingRoot;
    this.carryingMeta = carryingMeta;
  }

  @Override
  boolean isCarryingRoot() {
    return this.carryingRoot;
  }

  @Override
  boolean isCarryingMeta() {
    return this.carryingMeta;
  }
  
  @Override
  public String toString() {
    String name = "UnknownServerName";
    if(server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid();
  }
}
