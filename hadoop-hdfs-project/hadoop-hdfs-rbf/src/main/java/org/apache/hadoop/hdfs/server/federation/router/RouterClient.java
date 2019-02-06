/**
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
package org.apache.hadoop.hdfs.server.federation.router;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocolPB.RouterAdminProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.RouterAdminProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Client to connect to the {@link Router} via the admin protocol.
 */
@Private
public class RouterClient implements Closeable {

  private final RouterAdminProtocolTranslatorPB proxy;
  private final UserGroupInformation ugi;

  private static RouterAdminProtocolTranslatorPB createRouterProxy(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi)
          throws IOException {

    RPC.setProtocolEngine(
        conf, RouterAdminProtocolPB.class, ProtobufRpcEngine.class);

    AtomicBoolean fallbackToSimpleAuth = new AtomicBoolean(false);
    final long version = RPC.getProtocolVersion(RouterAdminProtocolPB.class);
    RouterAdminProtocolPB proxy = RPC.getProtocolProxy(
        RouterAdminProtocolPB.class, version, address, ugi, conf,
        NetUtils.getDefaultSocketFactory(conf),
        RPC.getRpcTimeout(conf), null,
        fallbackToSimpleAuth).getProxy();

    return new RouterAdminProtocolTranslatorPB(proxy);
  }

  public RouterClient(InetSocketAddress address, Configuration conf)
      throws IOException {
    this.ugi = UserGroupInformation.getCurrentUser();
    this.proxy = createRouterProxy(address, conf, ugi);
  }

  public MountTableManager getMountTableManager() {
    return proxy;
  }

  public RouterStateManager getRouterStateManager() {
    return proxy;
  }

  public NameserviceManager getNameserviceManager() {
    return proxy;
  }

  @Override
  public synchronized void close() throws IOException {
    RPC.stopProxy(proxy);
  }
}