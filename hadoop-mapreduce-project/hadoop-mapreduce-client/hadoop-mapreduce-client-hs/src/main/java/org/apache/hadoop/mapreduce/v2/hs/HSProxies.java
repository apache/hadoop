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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocol;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocolPB;
import org.apache.hadoop.mapreduce.v2.hs.protocolPB.HSAdminRefreshProtocolClientSideTranslatorPB;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
public class HSProxies {

  private static final Logger LOG = LoggerFactory.getLogger(HSProxies.class);

  @SuppressWarnings("unchecked")
  public static <T> T createProxy(Configuration conf, InetSocketAddress hsaddr,
      Class<T> xface, UserGroupInformation ugi) throws IOException {

    T proxy;
    if (xface == RefreshUserMappingsProtocol.class) {
      proxy = (T) createHSProxyWithRefreshUserMappingsProtocol(hsaddr, conf,
          ugi);
    } else if (xface == GetUserMappingsProtocol.class) {
      proxy = (T) createHSProxyWithGetUserMappingsProtocol(hsaddr, conf, ugi);
    } else if (xface == HSAdminRefreshProtocol.class) {
      proxy = (T) createHSProxyWithHSAdminRefreshProtocol(hsaddr, conf, ugi);
    } else {
      String message = "Unsupported protocol found when creating the proxy "
          + "connection to History server: "
          + ((xface != null) ? xface.getClass().getName() : "null");
      LOG.error(message);
      throw new IllegalStateException(message);
    }
    return proxy;
  }

  private static RefreshUserMappingsProtocol createHSProxyWithRefreshUserMappingsProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi)
      throws IOException {
    RefreshUserMappingsProtocolPB proxy = (RefreshUserMappingsProtocolPB) createHSProxy(
        address, conf, ugi, RefreshUserMappingsProtocolPB.class, 0);
    return new RefreshUserMappingsProtocolClientSideTranslatorPB(proxy);
  }

  private static GetUserMappingsProtocol createHSProxyWithGetUserMappingsProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi)
      throws IOException {
    GetUserMappingsProtocolPB proxy = (GetUserMappingsProtocolPB) createHSProxy(
        address, conf, ugi, GetUserMappingsProtocolPB.class, 0);
    return new GetUserMappingsProtocolClientSideTranslatorPB(proxy);
  }

  private static HSAdminRefreshProtocol createHSProxyWithHSAdminRefreshProtocol(
      InetSocketAddress hsaddr, Configuration conf, UserGroupInformation ugi)
      throws IOException {
    HSAdminRefreshProtocolPB proxy = (HSAdminRefreshProtocolPB) createHSProxy(
        hsaddr, conf, ugi, HSAdminRefreshProtocolPB.class, 0);
    return new HSAdminRefreshProtocolClientSideTranslatorPB(proxy);
  }

  private static Object createHSProxy(InetSocketAddress address,
      Configuration conf, UserGroupInformation ugi, Class<?> xface,
      int rpcTimeout) throws IOException {
    RPC.setProtocolEngine(conf, xface, ProtobufRpcEngine.class);
    Object proxy = RPC.getProxy(xface, RPC.getProtocolVersion(xface), address,
        ugi, conf, NetUtils.getDefaultSocketFactory(conf), rpcTimeout);
    return proxy;
  }
}
