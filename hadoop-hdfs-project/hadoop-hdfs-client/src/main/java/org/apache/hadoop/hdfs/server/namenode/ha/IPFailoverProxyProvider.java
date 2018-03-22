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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A NNFailoverProxyProvider implementation which works on IP failover setup.
 * Only one proxy is used to connect to both servers and switching between
 * the servers is done by the environment/infrastructure, which guarantees
 * clients can consistently reach only one node at a time.
 *
 * Clients with a live connection will likely get connection reset after an
 * IP failover. This case will be handled by the
 * FailoverOnNetworkExceptionRetry retry policy. I.e. if the call is
 * not idempotent, it won't get retried.
 *
 * A connection reset while setting up a connection (i.e. before sending a
 * request) will be handled in ipc client.
 *
 * The namenode URI must contain a resolvable host name.
 */
public class IPFailoverProxyProvider<T> extends
    AbstractNNFailoverProxyProvider<T> {
  private final Configuration conf;
  private final Class<T> xface;
  private final URI nameNodeUri;
  private final HAProxyFactory<T> factory;
  private ProxyInfo<T> nnProxyInfo = null;

  public IPFailoverProxyProvider(Configuration conf, URI uri,
      Class<T> xface, HAProxyFactory<T> factory) {
    this.xface = xface;
    this.nameNodeUri = uri;
    this.factory = factory;

    this.conf = new Configuration(conf);
    int maxRetries = this.conf.getInt(
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_KEY,
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        maxRetries);

    int maxRetriesOnSocketTimeouts = this.conf.getInt(
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        maxRetriesOnSocketTimeouts);
  }

  @Override
  public Class<T> getInterface() {
    return xface;
  }

  @Override
  public synchronized ProxyInfo<T> getProxy() {
    // Create a non-ha proxy if not already created.
    if (nnProxyInfo == null) {
      try {
        // Create a proxy that is not wrapped in RetryProxy
        InetSocketAddress nnAddr = DFSUtilClient.getNNAddress(nameNodeUri);
        nnProxyInfo = new ProxyInfo<T>(factory.createProxy(conf, nnAddr, xface,
          UserGroupInformation.getCurrentUser(), false), nnAddr.toString());
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    return nnProxyInfo;
  }

  /** Nothing to do for IP failover */
  @Override
  public void performFailover(T currentProxy) {
  }

  /**
   * Close the proxy,
   */
  @Override
  public synchronized void close() throws IOException {
    if (nnProxyInfo == null) {
      return;
    }
    if (nnProxyInfo.proxy instanceof Closeable) {
      ((Closeable)nnProxyInfo.proxy).close();
    } else {
      RPC.stopProxy(nnProxyInfo.proxy);
    }
  }

  /**
   * Logical URI is not used for IP failover.
   */
  @Override
  public boolean useLogicalURI() {
    return false;
  }
}
