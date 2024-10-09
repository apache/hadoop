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
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import java.net.InetSocketAddress;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;

/**
 * A FailoverProxyProvider implementation which allows one to configure
 * multiple URIs to connect to during fail-over. A random configured address is
 * tried first, and on a fail-over event the other addresses are tried
 * sequentially in a random order.
 */
public class ConfiguredFailoverProxyProvider<T> extends
    AbstractNNFailoverProxyProvider<T> {

  protected final List<NNProxyInfo<T>> proxies;

  private int currentProxyIndex = 0;

  public ConfiguredFailoverProxyProvider(Configuration conf, URI uri,
      Class<T> xface, HAProxyFactory<T> factory) {
    this(conf, uri, xface, factory, DFS_NAMENODE_RPC_ADDRESS_KEY);
  }

  public ConfiguredFailoverProxyProvider(Configuration conf, URI uri,
      Class<T> xface, HAProxyFactory<T> factory, String addressKey) {
    super(conf, uri, xface, factory);
    this.proxies = getProxyAddresses(uri, addressKey);
  }

  /**
   * Lazily initialize the RPC proxy object.
   */
  @Override
  public synchronized ProxyInfo<T> getProxy() {
    NNProxyInfo<T> current = proxies.get(currentProxyIndex);
    return createProxyIfNeeded(current);
  }

  @Override
  public void performFailover(T currentProxy) {
    //reset the IP address in case  the stale IP was the cause for failover
    LOG.info("Resetting cached proxy: " + currentProxyIndex);
    resetProxyAddress(proxies, currentProxyIndex);
    incrementProxyIndex();
  }

  synchronized void incrementProxyIndex() {
    currentProxyIndex = (currentProxyIndex + 1) % proxies.size();
  }

  /**
   * Close all the proxy objects which have been opened over the lifetime of
   * this proxy provider.
   */
  @Override
  public synchronized void close() throws IOException {
    for (ProxyInfo<T> proxy : proxies) {
      stopProxy(proxy.proxy);
    }
  }

  /**
   * Logical URI is required for this failover proxy provider.
   */
  @Override
  public boolean useLogicalURI() {
    return true;
  }

  /**
   * Resets the NameNode proxy address in case it's stale
   */
  protected void resetProxyAddress(List<NNProxyInfo<T>> proxies, int index) {
    try {
      stopProxy(proxies.get(index).proxy);
      InetSocketAddress oldAddress = proxies.get(index).getAddress();
      InetSocketAddress address = NetUtils.createSocketAddr(
              oldAddress.getHostName() + ":" + oldAddress.getPort());
      LOG.debug("oldAddress {}, newAddress {}", oldAddress, address);
      proxies.set(index, new NNProxyInfo<T>(address));
    } catch (Exception e) {
      throw new RuntimeException("Could not refresh NN address", e);
    }
  }

  protected void stopProxy(T proxy) {
    if (proxy != null) {
      if (proxy instanceof Closeable) {
        try {
          ((Closeable)proxy).close();
        } catch(IOException e) {
          throw new RuntimeException("Could not close proxy", e);
        }
      } else {
        RPC.stopProxy(proxy);
      }
    }
  }
}
