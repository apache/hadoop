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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NameNodeProxiesClient.ProxyAndInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocolPB.AliasMapProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.InMemoryAliasMapProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMapProtocol;
import org.apache.hadoop.hdfs.server.namenode.ha.AbstractNNFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.namenode.ha.NameNodeHAProxyFactory;
import org.apache.hadoop.hdfs.server.protocol.BalancerProtocols;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProxyCombiner;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RefreshCallQueueProtocol;
import org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolClientSideTranslatorPB;
import org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolPB;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolClientSideTranslatorPB;
import org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;

/**
 * Create proxy objects to communicate with a remote NN. All remote access to an
 * NN should be funneled through this class. Most of the time you'll want to use
 * {@link NameNodeProxies#createProxy(Configuration, URI, Class)}, which will
 * create either an HA- or non-HA-enabled client proxy as appropriate.
 */
@InterfaceAudience.Private
public class NameNodeProxies {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(NameNodeProxies.class);

  /**
   * Creates the namenode proxy with the passed protocol. This will handle
   * creation of either HA- or non-HA-enabled proxy objects, depending upon
   * if the provided URI is a configured logical URI.
   * 
   * @param conf the configuration containing the required IPC
   *        properties, client failover configurations, etc.
   * @param nameNodeUri the URI pointing either to a specific NameNode
   *        or to a logical nameservice.
   * @param xface the IPC interface which should be created
   * @return an object containing both the proxy and the associated
   *         delegation token service it corresponds to
   * @throws IOException if there is an error creating the proxy
   **/
  public static <T> ProxyAndInfo<T> createProxy(Configuration conf,
      URI nameNodeUri, Class<T> xface) throws IOException {
    return createProxy(conf, nameNodeUri, xface, null);
  }

  /**
   * Creates the namenode proxy with the passed protocol. This will handle
   * creation of either HA- or non-HA-enabled proxy objects, depending upon
   * if the provided URI is a configured logical URI.
   *
   * @param conf the configuration containing the required IPC
   *        properties, client failover configurations, etc.
   * @param nameNodeUri the URI pointing either to a specific NameNode
   *        or to a logical nameservice.
   * @param xface the IPC interface which should be created
   * @param fallbackToSimpleAuth set to true or false during calls to indicate if
   *   a secure client falls back to simple auth
   * @return an object containing both the proxy and the associated
   *         delegation token service it corresponds to
   * @throws IOException if there is an error creating the proxy
   **/
  @SuppressWarnings("unchecked")
  public static <T> ProxyAndInfo<T> createProxy(Configuration conf,
      URI nameNodeUri, Class<T> xface, AtomicBoolean fallbackToSimpleAuth)
      throws IOException {
    AbstractNNFailoverProxyProvider<T> failoverProxyProvider =
        NameNodeProxiesClient.createFailoverProxyProvider(conf, nameNodeUri,
            xface, true, fallbackToSimpleAuth, new NameNodeHAProxyFactory<T>());

    if (failoverProxyProvider == null) {
      return createNonHAProxy(conf, DFSUtilClient.getNNAddress(nameNodeUri),
          xface, UserGroupInformation.getCurrentUser(), true,
          fallbackToSimpleAuth, null);
    } else {
      return NameNodeProxiesClient.createHAProxy(conf, nameNodeUri, xface,
          failoverProxyProvider);
    }
  }

  /**
   * Creates an explicitly non-HA-enabled proxy object. Most of the time you
   * don't want to use this, and should instead use {@link NameNodeProxies#createProxy}.
   * 
   * @param conf the configuration object
   * @param nnAddr address of the remote NN to connect to
   * @param xface the IPC interface which should be created
   * @param ugi the user who is making the calls on the proxy object
   * @param withRetries certain interfaces have a non-standard retry policy
   * @return an object containing both the proxy and the associated
   *         delegation token service it corresponds to
   * @throws IOException
   */
  public static <T> ProxyAndInfo<T> createNonHAProxy(
      Configuration conf, InetSocketAddress nnAddr, Class<T> xface,
      UserGroupInformation ugi, boolean withRetries) throws IOException {
    return createNonHAProxy(conf, nnAddr, xface, ugi, withRetries, null, null);
  }

  /**
   * Creates an explicitly non-HA-enabled proxy object. Most of the time you
   * don't want to use this, and should instead use {@link NameNodeProxies#createProxy}.
   *
   * @param conf the configuration object
   * @param nnAddr address of the remote NN to connect to
   * @param xface the IPC interface which should be created
   * @param ugi the user who is making the calls on the proxy object
   * @param withRetries certain interfaces have a non-standard retry policy
   * @param fallbackToSimpleAuth - set to true or false during this method to
   *   indicate if a secure client falls back to simple auth
   * @return an object containing both the proxy and the associated
   *         delegation token service it corresponds to
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static <T> ProxyAndInfo<T> createNonHAProxy(
      Configuration conf, InetSocketAddress nnAddr, Class<T> xface,
      UserGroupInformation ugi, boolean withRetries,
      AtomicBoolean fallbackToSimpleAuth, AlignmentContext alignmentContext)
      throws IOException {
    Text dtService = SecurityUtil.buildTokenService(nnAddr);
  
    T proxy;
    if (xface == ClientProtocol.class) {
      proxy = (T) NameNodeProxiesClient.createProxyWithAlignmentContext(
          nnAddr, conf, ugi, withRetries, fallbackToSimpleAuth,
          alignmentContext);
    } else if (xface == JournalProtocol.class) {
      proxy = (T) createNNProxyWithJournalProtocol(nnAddr, conf, ugi,
          alignmentContext);
    } else if (xface == NamenodeProtocol.class) {
      proxy = (T) createNNProxyWithNamenodeProtocol(nnAddr, conf, ugi,
          withRetries, alignmentContext);
    } else if (xface == GetUserMappingsProtocol.class) {
      proxy = (T) createNNProxyWithGetUserMappingsProtocol(nnAddr, conf, ugi,
          alignmentContext);
    } else if (xface == RefreshUserMappingsProtocol.class) {
      proxy = (T) createNNProxyWithRefreshUserMappingsProtocol(nnAddr, conf,
          ugi, alignmentContext);
    } else if (xface == RefreshAuthorizationPolicyProtocol.class) {
      proxy = (T) createNNProxyWithRefreshAuthorizationPolicyProtocol(nnAddr,
          conf, ugi, alignmentContext);
    } else if (xface == RefreshCallQueueProtocol.class) {
      proxy = (T) createNNProxyWithRefreshCallQueueProtocol(nnAddr, conf, ugi,
          alignmentContext);
    } else if (xface == InMemoryAliasMapProtocol.class) {
      proxy = (T) createNNProxyWithInMemoryAliasMapProtocol(nnAddr, conf, ugi,
          alignmentContext);
    } else if (xface == BalancerProtocols.class) {
      proxy = (T) createNNProxyWithBalancerProtocol(nnAddr, conf, ugi,
          withRetries, fallbackToSimpleAuth, alignmentContext);
    } else {
      String message = "Unsupported protocol found when creating the proxy " +
          "connection to NameNode: " +
          ((xface != null) ? xface.getClass().getName() : "null");
      LOG.error(message);
      throw new IllegalStateException(message);
    }

    return new ProxyAndInfo<T>(proxy, dtService, nnAddr);
  }

  private static InMemoryAliasMapProtocol createNNProxyWithInMemoryAliasMapProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi,
      AlignmentContext alignmentContext) throws IOException {
    AliasMapProtocolPB proxy = createNameNodeProxy(
        address, conf, ugi, AliasMapProtocolPB.class, 30000, alignmentContext);
    return new InMemoryAliasMapProtocolClientSideTranslatorPB(proxy);
  }

  private static JournalProtocol createNNProxyWithJournalProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi,
      AlignmentContext alignmentContext) throws IOException {
    JournalProtocolPB proxy = createNameNodeProxy(address,
        conf, ugi, JournalProtocolPB.class, 30000, alignmentContext);
    return new JournalProtocolTranslatorPB(proxy);
  }

  private static RefreshAuthorizationPolicyProtocol
      createNNProxyWithRefreshAuthorizationPolicyProtocol(InetSocketAddress address,
      Configuration conf, UserGroupInformation ugi,
      AlignmentContext alignmentContext) throws IOException {
    RefreshAuthorizationPolicyProtocolPB proxy = createNameNodeProxy(address,
        conf, ugi, RefreshAuthorizationPolicyProtocolPB.class, 0,
        alignmentContext);
    return new RefreshAuthorizationPolicyProtocolClientSideTranslatorPB(proxy);
  }
  
  private static RefreshUserMappingsProtocol
      createNNProxyWithRefreshUserMappingsProtocol(InetSocketAddress address,
      Configuration conf, UserGroupInformation ugi,
      AlignmentContext alignmentContext) throws IOException {
    RefreshUserMappingsProtocolPB proxy = createNameNodeProxy(address, conf,
        ugi, RefreshUserMappingsProtocolPB.class, 0, alignmentContext);
    return new RefreshUserMappingsProtocolClientSideTranslatorPB(proxy);
  }

  private static RefreshCallQueueProtocol
      createNNProxyWithRefreshCallQueueProtocol(InetSocketAddress address,
      Configuration conf, UserGroupInformation ugi,
      AlignmentContext alignmentContext) throws IOException {
    RefreshCallQueueProtocolPB proxy = createNameNodeProxy(address, conf, ugi,
        RefreshCallQueueProtocolPB.class, 0, alignmentContext);
    return new RefreshCallQueueProtocolClientSideTranslatorPB(proxy);
  }

  private static GetUserMappingsProtocol createNNProxyWithGetUserMappingsProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi,
      AlignmentContext alignmentContext) throws IOException {
    GetUserMappingsProtocolPB proxy = createNameNodeProxy(address, conf, ugi,
        GetUserMappingsProtocolPB.class, 0, alignmentContext);
    return new GetUserMappingsProtocolClientSideTranslatorPB(proxy);
  }
  
  private static NamenodeProtocol createNNProxyWithNamenodeProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi,
      boolean withRetries, AlignmentContext alignmentContext)
      throws IOException {
    NamenodeProtocolPB proxy = createNameNodeProxy(
        address, conf, ugi, NamenodeProtocolPB.class, 0, alignmentContext);
    if (withRetries) { // create the proxy with retries
      RetryPolicy timeoutPolicy = RetryPolicies.exponentialBackoffRetry(5, 200,
              TimeUnit.MILLISECONDS);
      Map<String, RetryPolicy> methodNameToPolicyMap
           = new HashMap<String, RetryPolicy>();
      methodNameToPolicyMap.put("getBlocks", timeoutPolicy);
      methodNameToPolicyMap.put("getAccessKeys", timeoutPolicy);
      NamenodeProtocol translatorProxy =
          new NamenodeProtocolTranslatorPB(proxy);
      return (NamenodeProtocol) RetryProxy.create(
          NamenodeProtocol.class, translatorProxy, methodNameToPolicyMap);
    } else {
      return new NamenodeProtocolTranslatorPB(proxy);
    }
  }

  private static BalancerProtocols createNNProxyWithBalancerProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi,
      boolean withRetries, AtomicBoolean fallbackToSimpleAuth,
      AlignmentContext alignmentContext) throws IOException {
    NamenodeProtocol namenodeProtocol = createNNProxyWithNamenodeProtocol(
        address, conf, ugi, withRetries, alignmentContext);
    ClientProtocol clientProtocol =
        NameNodeProxiesClient.createProxyWithAlignmentContext(address,
            conf, ugi, withRetries, fallbackToSimpleAuth, alignmentContext);

    return ProxyCombiner.combine(BalancerProtocols.class,
        namenodeProtocol, clientProtocol);
  }

  private static <T> T createNameNodeProxy(InetSocketAddress address,
      Configuration conf, UserGroupInformation ugi, Class<T> xface,
      int rpcTimeout, AlignmentContext alignmentContext) throws IOException {
    RPC.setProtocolEngine(conf, xface, ProtobufRpcEngine.class);
    return RPC.getProtocolProxy(xface,
        RPC.getProtocolVersion(xface), address, ugi, conf,
        NetUtils.getDefaultSocketFactory(conf), rpcTimeout, null, null,
        alignmentContext).getProxy();
  }

}
