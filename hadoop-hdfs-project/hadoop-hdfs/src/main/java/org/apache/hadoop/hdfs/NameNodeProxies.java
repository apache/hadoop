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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_RETRY_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.ha.AbstractNNFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.namenode.ha.WrappedFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.DefaultFailoverProxyProvider;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.LossyRetryInvocationHandler;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolClientSideTranslatorPB;
import org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB;
import org.apache.hadoop.ipc.RefreshCallQueueProtocol;
import org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolPB;
import org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolClientSideTranslatorPB;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Create proxy objects to communicate with a remote NN. All remote access to an
 * NN should be funneled through this class. Most of the time you'll want to use
 * {@link NameNodeProxies#createProxy(Configuration, URI, Class)}, which will
 * create either an HA- or non-HA-enabled client proxy as appropriate.
 */
public class NameNodeProxies {
  
  private static final Log LOG = LogFactory.getLog(NameNodeProxies.class);

  /**
   * Wrapper for a client proxy as well as its associated service ID.
   * This is simply used as a tuple-like return type for
   * {@link NameNodeProxies#createProxy} and
   * {@link NameNodeProxies#createNonHAProxy}.
   */
  public static class ProxyAndInfo<PROXYTYPE> {
    private final PROXYTYPE proxy;
    private final Text dtService;
    private final InetSocketAddress address;
    
    public ProxyAndInfo(PROXYTYPE proxy, Text dtService,
        InetSocketAddress address) {
      this.proxy = proxy;
      this.dtService = dtService;
      this.address = address;
    }
    
    public PROXYTYPE getProxy() {
      return proxy;
    }
    
    public Text getDelegationTokenService() {
      return dtService;
    }

    public InetSocketAddress getAddress() {
      return address;
    }
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
   * @return an object containing both the proxy and the associated
   *         delegation token service it corresponds to
   * @throws IOException if there is an error creating the proxy
   **/
  @SuppressWarnings("unchecked")
  public static <T> ProxyAndInfo<T> createProxy(Configuration conf,
      URI nameNodeUri, Class<T> xface) throws IOException {
    AbstractNNFailoverProxyProvider<T> failoverProxyProvider =
        createFailoverProxyProvider(conf, nameNodeUri, xface, true);
  
    if (failoverProxyProvider == null) {
      // Non-HA case
      return createNonHAProxy(conf, NameNode.getAddress(nameNodeUri), xface,
          UserGroupInformation.getCurrentUser(), true);
    } else {
      // HA case
      Conf config = new Conf(conf);
      T proxy = (T) RetryProxy.create(xface, failoverProxyProvider,
          RetryPolicies.failoverOnNetworkException(
              RetryPolicies.TRY_ONCE_THEN_FAIL, config.maxFailoverAttempts,
              config.maxRetryAttempts, config.failoverSleepBaseMillis,
              config.failoverSleepMaxMillis));

      Text dtService;
      if (failoverProxyProvider.useLogicalURI()) {
        dtService = HAUtil.buildTokenServiceForLogicalUri(nameNodeUri);
      } else {
        dtService = SecurityUtil.buildTokenService(
            NameNode.getAddress(nameNodeUri));
      }
      return new ProxyAndInfo<T>(proxy, dtService,
          NameNode.getAddress(nameNodeUri));
    }
  }
  
  /**
   * Generate a dummy namenode proxy instance that utilizes our hacked
   * {@link LossyRetryInvocationHandler}. Proxy instance generated using this
   * method will proactively drop RPC responses. Currently this method only
   * support HA setup. null will be returned if the given configuration is not 
   * for HA.
   * 
   * @param config the configuration containing the required IPC
   *        properties, client failover configurations, etc.
   * @param nameNodeUri the URI pointing either to a specific NameNode
   *        or to a logical nameservice.
   * @param xface the IPC interface which should be created
   * @param numResponseToDrop The number of responses to drop for each RPC call
   * @return an object containing both the proxy and the associated
   *         delegation token service it corresponds to. Will return null of the
   *         given configuration does not support HA.
   * @throws IOException if there is an error creating the proxy
   */
  @SuppressWarnings("unchecked")
  public static <T> ProxyAndInfo<T> createProxyWithLossyRetryHandler(
      Configuration config, URI nameNodeUri, Class<T> xface,
      int numResponseToDrop) throws IOException {
    Preconditions.checkArgument(numResponseToDrop > 0);
    AbstractNNFailoverProxyProvider<T> failoverProxyProvider =
        createFailoverProxyProvider(config, nameNodeUri, xface, true);

    if (failoverProxyProvider != null) { // HA case
      int delay = config.getInt(
          DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY,
          DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT);
      int maxCap = config.getInt(
          DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY,
          DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT);
      int maxFailoverAttempts = config.getInt(
          DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
          DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);
      int maxRetryAttempts = config.getInt(
          DFS_CLIENT_RETRY_MAX_ATTEMPTS_KEY,
          DFS_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT);
      InvocationHandler dummyHandler = new LossyRetryInvocationHandler<T>(
              numResponseToDrop, failoverProxyProvider,
              RetryPolicies.failoverOnNetworkException(
                  RetryPolicies.TRY_ONCE_THEN_FAIL, maxFailoverAttempts, 
                  Math.max(numResponseToDrop + 1, maxRetryAttempts), delay, 
                  maxCap));
      
      T proxy = (T) Proxy.newProxyInstance(
          failoverProxyProvider.getInterface().getClassLoader(),
          new Class[] { xface }, dummyHandler);
      Text dtService;
      if (failoverProxyProvider.useLogicalURI()) {
        dtService = HAUtil.buildTokenServiceForLogicalUri(nameNodeUri);
      } else {
        dtService = SecurityUtil.buildTokenService(
            NameNode.getAddress(nameNodeUri));
      }
      return new ProxyAndInfo<T>(proxy, dtService,
          NameNode.getAddress(nameNodeUri));
    } else {
      LOG.warn("Currently creating proxy using " +
      		"LossyRetryInvocationHandler requires NN HA setup");
      return null;
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
  @SuppressWarnings("unchecked")
  public static <T> ProxyAndInfo<T> createNonHAProxy(
      Configuration conf, InetSocketAddress nnAddr, Class<T> xface,
      UserGroupInformation ugi, boolean withRetries) throws IOException {
    Text dtService = SecurityUtil.buildTokenService(nnAddr);
  
    T proxy;
    if (xface == ClientProtocol.class) {
      proxy = (T) createNNProxyWithClientProtocol(nnAddr, conf, ugi,
          withRetries);
    } else if (xface == JournalProtocol.class) {
      proxy = (T) createNNProxyWithJournalProtocol(nnAddr, conf, ugi);
    } else if (xface == NamenodeProtocol.class) {
      proxy = (T) createNNProxyWithNamenodeProtocol(nnAddr, conf, ugi,
          withRetries);
    } else if (xface == GetUserMappingsProtocol.class) {
      proxy = (T) createNNProxyWithGetUserMappingsProtocol(nnAddr, conf, ugi);
    } else if (xface == RefreshUserMappingsProtocol.class) {
      proxy = (T) createNNProxyWithRefreshUserMappingsProtocol(nnAddr, conf, ugi);
    } else if (xface == RefreshAuthorizationPolicyProtocol.class) {
      proxy = (T) createNNProxyWithRefreshAuthorizationPolicyProtocol(nnAddr,
          conf, ugi);
    } else if (xface == RefreshCallQueueProtocol.class) {
      proxy = (T) createNNProxyWithRefreshCallQueueProtocol(nnAddr, conf, ugi);
    } else {
      String message = "Unsupported protocol found when creating the proxy " +
          "connection to NameNode: " +
          ((xface != null) ? xface.getClass().getName() : "null");
      LOG.error(message);
      throw new IllegalStateException(message);
    }

    return new ProxyAndInfo<T>(proxy, dtService, nnAddr);
  }
  
  private static JournalProtocol createNNProxyWithJournalProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi)
      throws IOException {
    JournalProtocolPB proxy = (JournalProtocolPB) createNameNodeProxy(address,
        conf, ugi, JournalProtocolPB.class);
    return new JournalProtocolTranslatorPB(proxy);
  }

  private static RefreshAuthorizationPolicyProtocol
      createNNProxyWithRefreshAuthorizationPolicyProtocol(InetSocketAddress address,
          Configuration conf, UserGroupInformation ugi) throws IOException {
    RefreshAuthorizationPolicyProtocolPB proxy = (RefreshAuthorizationPolicyProtocolPB)
        createNameNodeProxy(address, conf, ugi, RefreshAuthorizationPolicyProtocolPB.class);
    return new RefreshAuthorizationPolicyProtocolClientSideTranslatorPB(proxy);
  }
  
  private static RefreshUserMappingsProtocol
      createNNProxyWithRefreshUserMappingsProtocol(InetSocketAddress address,
          Configuration conf, UserGroupInformation ugi) throws IOException {
    RefreshUserMappingsProtocolPB proxy = (RefreshUserMappingsProtocolPB)
        createNameNodeProxy(address, conf, ugi, RefreshUserMappingsProtocolPB.class);
    return new RefreshUserMappingsProtocolClientSideTranslatorPB(proxy);
  }

  private static RefreshCallQueueProtocol
      createNNProxyWithRefreshCallQueueProtocol(InetSocketAddress address,
          Configuration conf, UserGroupInformation ugi) throws IOException {
    RefreshCallQueueProtocolPB proxy = (RefreshCallQueueProtocolPB)
        createNameNodeProxy(address, conf, ugi, RefreshCallQueueProtocolPB.class);
    return new RefreshCallQueueProtocolClientSideTranslatorPB(proxy);
  }

  private static GetUserMappingsProtocol createNNProxyWithGetUserMappingsProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi)
      throws IOException {
    GetUserMappingsProtocolPB proxy = (GetUserMappingsProtocolPB)
        createNameNodeProxy(address, conf, ugi, GetUserMappingsProtocolPB.class);
    return new GetUserMappingsProtocolClientSideTranslatorPB(proxy);
  }
  
  private static NamenodeProtocol createNNProxyWithNamenodeProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi,
      boolean withRetries) throws IOException {
    NamenodeProtocolPB proxy = (NamenodeProtocolPB) createNameNodeProxy(
        address, conf, ugi, NamenodeProtocolPB.class);
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
  
  private static ClientProtocol createNNProxyWithClientProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi,
      boolean withRetries) throws IOException {
    RPC.setProtocolEngine(conf, ClientNamenodeProtocolPB.class, ProtobufRpcEngine.class);

    final RetryPolicy defaultPolicy = 
        RetryUtils.getDefaultRetryPolicy(
            conf, 
            DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_ENABLED_KEY, 
            DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_ENABLED_DEFAULT, 
            DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_SPEC_KEY,
            DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_SPEC_DEFAULT,
            SafeModeException.class);
    
    final long version = RPC.getProtocolVersion(ClientNamenodeProtocolPB.class);
    ClientNamenodeProtocolPB proxy = RPC.getProtocolProxy(
        ClientNamenodeProtocolPB.class, version, address, ugi, conf,
        NetUtils.getDefaultSocketFactory(conf),
        org.apache.hadoop.ipc.Client.getTimeout(conf), defaultPolicy)
            .getProxy();

    if (withRetries) { // create the proxy with retries

      RetryPolicy createPolicy = RetryPolicies
          .retryUpToMaximumCountWithFixedSleep(5,
              HdfsConstants.LEASE_SOFTLIMIT_PERIOD, TimeUnit.MILLISECONDS);
    
      Map<Class<? extends Exception>, RetryPolicy> remoteExceptionToPolicyMap 
                 = new HashMap<Class<? extends Exception>, RetryPolicy>();
      remoteExceptionToPolicyMap.put(AlreadyBeingCreatedException.class,
          createPolicy);

      RetryPolicy methodPolicy = RetryPolicies.retryByRemoteException(
          defaultPolicy, remoteExceptionToPolicyMap);
      Map<String, RetryPolicy> methodNameToPolicyMap 
                 = new HashMap<String, RetryPolicy>();
    
      methodNameToPolicyMap.put("create", methodPolicy);

      ClientProtocol translatorProxy =
        new ClientNamenodeProtocolTranslatorPB(proxy);
      return (ClientProtocol) RetryProxy.create(
          ClientProtocol.class,
          new DefaultFailoverProxyProvider<ClientProtocol>(
              ClientProtocol.class, translatorProxy),
          methodNameToPolicyMap,
          defaultPolicy);
    } else {
      return new ClientNamenodeProtocolTranslatorPB(proxy);
    }
  }

  private static Object createNameNodeProxy(InetSocketAddress address,
      Configuration conf, UserGroupInformation ugi, Class<?> xface)
      throws IOException {
    RPC.setProtocolEngine(conf, xface, ProtobufRpcEngine.class);
    Object proxy = RPC.getProxy(xface, RPC.getProtocolVersion(xface), address,
        ugi, conf, NetUtils.getDefaultSocketFactory(conf));
    return proxy;
  }

  /** Gets the configured Failover proxy provider's class */
  @VisibleForTesting
  public static <T> Class<FailoverProxyProvider<T>> getFailoverProxyProviderClass(
      Configuration conf, URI nameNodeUri) throws IOException {
    if (nameNodeUri == null) {
      return null;
    }
    String host = nameNodeUri.getHost();
  
    String configKey = DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "."
        + host;
    try {
      @SuppressWarnings("unchecked")
      Class<FailoverProxyProvider<T>> ret = (Class<FailoverProxyProvider<T>>) conf
          .getClass(configKey, null, FailoverProxyProvider.class);
      return ret;
    } catch (RuntimeException e) {
      if (e.getCause() instanceof ClassNotFoundException) {
        throw new IOException("Could not load failover proxy provider class "
            + conf.get(configKey) + " which is configured for authority "
            + nameNodeUri, e);
      } else {
        throw e;
      }
    }
  }

  /** Creates the Failover proxy provider instance*/
  @VisibleForTesting
  public static <T> AbstractNNFailoverProxyProvider<T> createFailoverProxyProvider(
      Configuration conf, URI nameNodeUri, Class<T> xface, boolean checkPort)
      throws IOException {
    Class<FailoverProxyProvider<T>> failoverProxyProviderClass = null;
    AbstractNNFailoverProxyProvider<T> providerNN;
    Preconditions.checkArgument(
        xface.isAssignableFrom(NamenodeProtocols.class),
        "Interface %s is not a NameNode protocol", xface);
    try {
      // Obtain the class of the proxy provider
      failoverProxyProviderClass = getFailoverProxyProviderClass(conf,
          nameNodeUri);
      if (failoverProxyProviderClass == null) {
        return null;
      }
      // Create a proxy provider instance.
      Constructor<FailoverProxyProvider<T>> ctor = failoverProxyProviderClass
          .getConstructor(Configuration.class, URI.class, Class.class);
      FailoverProxyProvider<T> provider = ctor.newInstance(conf, nameNodeUri,
          xface);

      // If the proxy provider is of an old implementation, wrap it.
      if (!(provider instanceof AbstractNNFailoverProxyProvider)) {
        providerNN = new WrappedFailoverProxyProvider<T>(provider);
      } else {
        providerNN = (AbstractNNFailoverProxyProvider<T>)provider;
      }
    } catch (Exception e) {
      String message = "Couldn't create proxy provider " + failoverProxyProviderClass;
      if (LOG.isDebugEnabled()) {
        LOG.debug(message, e);
      }
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(message, e);
      }
    }

    // Check the port in the URI, if it is logical.
    if (checkPort && providerNN.useLogicalURI()) {
      int port = nameNodeUri.getPort();
      if (port > 0 && port != NameNode.DEFAULT_PORT) {
        // Throwing here without any cleanup is fine since we have not
        // actually created the underlying proxies yet.
        throw new IOException("Port " + port + " specified in URI "
            + nameNodeUri + " but host '" + nameNodeUri.getHost()
            + "' is a logical (HA) namenode"
            + " and does not use port information.");
      }
    }
    return providerNN;
  }

}
