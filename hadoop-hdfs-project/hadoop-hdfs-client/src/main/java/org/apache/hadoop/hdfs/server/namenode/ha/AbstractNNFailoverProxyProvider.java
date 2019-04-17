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

package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.net.DomainNameResolver;
import org.apache.hadoop.net.DomainNameResolverFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractNNFailoverProxyProvider<T> implements
    FailoverProxyProvider <T> {
  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractNNFailoverProxyProvider.class);

  protected Configuration conf;
  protected Class<T> xface;
  protected HAProxyFactory<T> factory;
  protected UserGroupInformation ugi;
  protected AtomicBoolean fallbackToSimpleAuth;

  protected AbstractNNFailoverProxyProvider() {
  }

  protected AbstractNNFailoverProxyProvider(Configuration conf, URI uri,
      Class<T> xface, HAProxyFactory<T> factory) {
    this.conf = new Configuration(conf);
    this.xface = xface;
    this.factory = factory;
    try {
      this.ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    int maxRetries = this.conf.getInt(
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_KEY,
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        maxRetries);

    int maxRetriesOnSocketTimeouts = this.conf.getInt(
        HdfsClientConfigKeys
        .Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        HdfsClientConfigKeys
        .Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeysPublic
        .IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        maxRetriesOnSocketTimeouts);
  }

  /**
   * Inquire whether logical HA URI is used for the implementation. If it is
   * used, a special token handling may be needed to make sure a token acquired
   * from a node in the HA pair can be used against the other node.
   *
   * @return true if logical HA URI is used. false, if not used.
   */
  public abstract boolean useLogicalURI();

  /**
   * Set for tracking if a secure client falls back to simple auth.  This method
   * is synchronized only to stifle a Findbugs warning.
   *
   * @param fallbackToSimpleAuth - set to true or false during this method to
   *   indicate if a secure client falls back to simple auth
   */
  public synchronized void setFallbackToSimpleAuth(
      AtomicBoolean fallbackToSimpleAuth) {
    this.fallbackToSimpleAuth = fallbackToSimpleAuth;
  }

  public synchronized AtomicBoolean getFallbackToSimpleAuth() {
    return fallbackToSimpleAuth;
  }

  /**
   * ProxyInfo to a NameNode. Includes its address.
   */
  public static class NNProxyInfo<T> extends ProxyInfo<T> {
    private InetSocketAddress address;
    /**
     * The currently known state of the NameNode represented by this ProxyInfo.
     * This may be out of date if the NameNode has changed state since the last
     * time the state was checked.
     */
    private HAServiceState cachedState;

    public NNProxyInfo(InetSocketAddress address) {
      super(null, address.toString());
      this.address = address;
    }

    public InetSocketAddress getAddress() {
      return address;
    }

    public void setCachedState(HAServiceState state) {
      cachedState = state;
    }

    public HAServiceState getCachedState() {
      return cachedState;
    }
  }

  @Override
  public Class<T> getInterface() {
    return xface;
  }

  /**
   * Create a proxy if it has not been created yet.
   */
  protected NNProxyInfo<T> createProxyIfNeeded(NNProxyInfo<T> pi) {
    if (pi.proxy == null) {
      assert pi.getAddress() != null : "Proxy address is null";
      try {
        pi.proxy = factory.createProxy(conf,
            pi.getAddress(), xface, ugi, false, getFallbackToSimpleAuth());
      } catch (IOException ioe) {
        LOG.error("{} Failed to create RPC proxy to NameNode at {}",
            this.getClass().getSimpleName(), pi.address, ioe);
        throw new RuntimeException(ioe);
      }
    }
    return pi;
  }

  /**
   * Get list of configured NameNode proxy addresses.
   * Randomize the list if requested.
   */
  protected List<NNProxyInfo<T>> getProxyAddresses(URI uri, String addressKey) {
    final List<NNProxyInfo<T>> proxies = new ArrayList<NNProxyInfo<T>>();
    Map<String, Map<String, InetSocketAddress>> map =
        DFSUtilClient.getAddresses(conf, null, addressKey);
    Map<String, InetSocketAddress> addressesInNN = map.get(uri.getHost());

    if (addressesInNN == null || addressesInNN.size() == 0) {
      throw new RuntimeException("Could not find any configured addresses " +
          "for URI " + uri);
    }

    Collection<InetSocketAddress> addressesOfNns = addressesInNN.values();
    try {
      addressesOfNns = getResolvedHostsIfNecessary(addressesOfNns, uri);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    for (InetSocketAddress address : addressesOfNns) {
      proxies.add(new NNProxyInfo<T>(address));
    }
    // Randomize the list to prevent all clients pointing to the same one
    boolean randomized = getRandomOrder(conf, uri);
    if (randomized) {
      Collections.shuffle(proxies);
    }

    // The client may have a delegation token set for the logical
    // URI of the cluster. Clone this token to apply to each of the
    // underlying IPC addresses so that the IPC code can find it.
    HAUtilClient.cloneDelegationTokenForLogicalUri(ugi, uri, addressesOfNns);
    return proxies;
  }

  /**
   * If resolved is needed: for every domain name in the parameter list,
   * resolve them into the actual IP addresses.
   *
   * @param addressesOfNns The domain name list from config.
   * @param nameNodeUri The URI of namenode/nameservice.
   * @return The collection of resolved IP addresses.
   * @throws IOException If there are issues resolving the addresses.
   */
  Collection<InetSocketAddress> getResolvedHostsIfNecessary(
      Collection<InetSocketAddress> addressesOfNns, URI nameNodeUri)
          throws IOException {
    // 'host' here is usually the ID of the nameservice when address
    // resolving is needed.
    String host = nameNodeUri.getHost();
    String configKeyWithHost =
        HdfsClientConfigKeys.Failover.RESOLVE_ADDRESS_NEEDED_KEY  + "." + host;
    boolean resolveNeeded = conf.getBoolean(configKeyWithHost,
        HdfsClientConfigKeys.Failover.RESOLVE_ADDRESS_NEEDED_DEFAULT);
    if (!resolveNeeded) {
      // Early return is no resolve is necessary
      return addressesOfNns;
    }
    // decide whether to access server by IP or by host name
    String useFQDNKeyWithHost =
        HdfsClientConfigKeys.Failover.RESOLVE_ADDRESS_TO_FQDN + "." + host;
    boolean requireFQDN = conf.getBoolean(useFQDNKeyWithHost,
        HdfsClientConfigKeys.Failover.RESOLVE_ADDRESS_TO_FQDN_DEFAULT);

    Collection<InetSocketAddress> addressOfResolvedNns = new ArrayList<>();
    DomainNameResolver dnr = DomainNameResolverFactory.newInstance(
          conf, nameNodeUri, HdfsClientConfigKeys.Failover.RESOLVE_SERVICE_KEY);
    // If the address needs to be resolved, get all of the IP addresses
    // from this address and pass them into the proxy
    LOG.info("Namenode domain name will be resolved with {}",
        dnr.getClass().getName());
    for (InetSocketAddress address : addressesOfNns) {
      String[] resolvedHostNames = dnr.getAllResolvedHostnameByDomainName(
          address.getHostName(), requireFQDN);
      int port = address.getPort();
      for (String hostname : resolvedHostNames) {
        InetSocketAddress resolvedAddress = new InetSocketAddress(
            hostname, port);
        addressOfResolvedNns.add(resolvedAddress);
      }
    }

    return addressOfResolvedNns;
  }

  /**
   * Check whether random order is configured for failover proxy provider
   * for the namenode/nameservice.
   *
   * @param conf Configuration
   * @param nameNodeUri The URI of namenode/nameservice
   * @return random order configuration
   */
  public static boolean getRandomOrder(
      Configuration conf, URI nameNodeUri) {
    String host = nameNodeUri.getHost();
    String configKeyWithHost = HdfsClientConfigKeys.Failover.RANDOM_ORDER
        + "." + host;

    if (conf.get(configKeyWithHost) != null) {
      return conf.getBoolean(
          configKeyWithHost,
          HdfsClientConfigKeys.Failover.RANDOM_ORDER_DEFAULT);
    }

    return conf.getBoolean(
        HdfsClientConfigKeys.Failover.RANDOM_ORDER,
        HdfsClientConfigKeys.Failover.RANDOM_ORDER_DEFAULT);
  }
}
