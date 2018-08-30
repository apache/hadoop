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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.ipc.RPC;

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
  private final NNProxyInfo<T> nnProxyInfo;

  public IPFailoverProxyProvider(Configuration conf, URI uri,
      Class<T> xface, HAProxyFactory<T> factory) {
    super(conf, uri, xface, factory);
    this.nnProxyInfo = new NNProxyInfo<T>(DFSUtilClient.getNNAddress(uri));
  }

  @Override
  public synchronized NNProxyInfo<T> getProxy() {
    // Create a non-ha proxy if not already created.
    return createProxyIfNeeded(nnProxyInfo);
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
    if (nnProxyInfo.proxy == null) {
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
