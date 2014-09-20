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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.server.namenode.ha.AbstractNNFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Preconditions;

/**
 * A FailoverProxyProvider implementation which allows one to configure two URIs
 * to connect to during fail-over. The first configured address is tried first,
 * and on a fail-over event the other address is tried.
 */
public class ConfiguredFailoverProxyProvider<T> extends
    AbstractNNFailoverProxyProvider<T> {
  
  private static final Log LOG =
      LogFactory.getLog(ConfiguredFailoverProxyProvider.class);
  
  private final Configuration conf;
  private final List<AddressRpcProxyPair<T>> proxies =
      new ArrayList<AddressRpcProxyPair<T>>();
  private final UserGroupInformation ugi;
  private final Class<T> xface;
  
  private int currentProxyIndex = 0;

  public ConfiguredFailoverProxyProvider(Configuration conf, URI uri,
      Class<T> xface) {
    Preconditions.checkArgument(
        xface.isAssignableFrom(NamenodeProtocols.class),
        "Interface class %s is not a valid NameNode protocol!");
    this.xface = xface;
    
    this.conf = new Configuration(conf);
    int maxRetries = this.conf.getInt(
        DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_KEY,
        DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        maxRetries);
    
    int maxRetriesOnSocketTimeouts = this.conf.getInt(
        DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        maxRetriesOnSocketTimeouts);
    
    try {
      ugi = UserGroupInformation.getCurrentUser();
      
      Map<String, Map<String, InetSocketAddress>> map = DFSUtil.getHaNnRpcAddresses(
          conf);
      Map<String, InetSocketAddress> addressesInNN = map.get(uri.getHost());
      
      if (addressesInNN == null || addressesInNN.size() == 0) {
        throw new RuntimeException("Could not find any configured addresses " +
            "for URI " + uri);
      }
      
      Collection<InetSocketAddress> addressesOfNns = addressesInNN.values();
      for (InetSocketAddress address : addressesOfNns) {
        proxies.add(new AddressRpcProxyPair<T>(address));
      }

      // The client may have a delegation token set for the logical
      // URI of the cluster. Clone this token to apply to each of the
      // underlying IPC addresses so that the IPC code can find it.
      HAUtil.cloneDelegationTokenForLogicalUri(ugi, uri, addressesOfNns);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
    
  @Override
  public Class<T> getInterface() {
    return xface;
  }

  /**
   * Lazily initialize the RPC proxy object.
   */
  @Override
  public synchronized ProxyInfo<T> getProxy() {
    AddressRpcProxyPair<T> current = proxies.get(currentProxyIndex);
    if (current.namenode == null) {
      try {
        current.namenode = NameNodeProxies.createNonHAProxy(conf,
            current.address, xface, ugi, false, fallbackToSimpleAuth).getProxy();
      } catch (IOException e) {
        LOG.error("Failed to create RPC proxy to NameNode", e);
        throw new RuntimeException(e);
      }
    }
    return new ProxyInfo<T>(current.namenode, current.address.toString());
  }

  @Override
  public synchronized void performFailover(T currentProxy) {
    currentProxyIndex = (currentProxyIndex + 1) % proxies.size();
  }

  /**
   * A little pair object to store the address and connected RPC proxy object to
   * an NN. Note that {@link AddressRpcProxyPair#namenode} may be null.
   */
  private static class AddressRpcProxyPair<T> {
    public final InetSocketAddress address;
    public T namenode;
    
    public AddressRpcProxyPair(InetSocketAddress address) {
      this.address = address;
    }
  }

  /**
   * Close all the proxy objects which have been opened over the lifetime of
   * this proxy provider.
   */
  @Override
  public synchronized void close() throws IOException {
    for (AddressRpcProxyPair<T> proxy : proxies) {
      if (proxy.namenode != null) {
        if (proxy.namenode instanceof Closeable) {
          ((Closeable)proxy.namenode).close();
        } else {
          RPC.stopProxy(proxy.namenode);
        }
      }
    }
  }

  /**
   * Logical URI is required for this failover proxy provider.
   */
  @Override
  public boolean useLogicalURI() {
    return true;
  }
}
