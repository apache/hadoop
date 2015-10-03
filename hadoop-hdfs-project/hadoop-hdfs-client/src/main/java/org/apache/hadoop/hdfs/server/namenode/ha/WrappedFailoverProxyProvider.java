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

import org.apache.hadoop.io.retry.FailoverProxyProvider;

/**
 * A NNFailoverProxyProvider implementation which wrapps old implementations
 * directly implementing the {@link FailoverProxyProvider} interface.
 *
 * It is assumed that the old impelmentation is using logical URI.
 */
public class WrappedFailoverProxyProvider<T> extends
    AbstractNNFailoverProxyProvider<T> {
  private final FailoverProxyProvider<T> proxyProvider;

  /**
   * Wrap the given instance of an old FailoverProxyProvider.
   */
  public WrappedFailoverProxyProvider(FailoverProxyProvider<T> provider) {
    proxyProvider = provider;
  }

  @Override
  public Class<T> getInterface() {
    return proxyProvider.getInterface();
  }

  @Override
  public synchronized ProxyInfo<T> getProxy() {
    return proxyProvider.getProxy();
  }

  @Override
  public void performFailover(T currentProxy) {
    proxyProvider.performFailover(currentProxy);
  }

  /**
   * Close the proxy,
   */
  @Override
  public synchronized void close() throws IOException {
    proxyProvider.close();
  }

  /**
   * Assume logical URI is used for old proxy provider implementations.
   */
  @Override
  public boolean useLogicalURI() {
    return true;
  }
}
