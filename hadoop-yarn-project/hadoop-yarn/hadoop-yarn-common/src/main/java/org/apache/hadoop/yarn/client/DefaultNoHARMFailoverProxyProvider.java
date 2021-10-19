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

package org.apache.hadoop.yarn.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.DefaultFailoverProxyProvider;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * An implementation of {@link RMFailoverProxyProvider} which does nothing in
 * the event of failover, and always returns the same proxy object.
 * This is the default non-HA RM Failover proxy provider. It is used to replace
 * {@link DefaultFailoverProxyProvider} which was used as Yarn default non-HA.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DefaultNoHARMFailoverProxyProvider<T>
    implements RMFailoverProxyProvider<T> {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultNoHARMFailoverProxyProvider.class);

  protected T proxy;
  protected Class<T> protocol;

  /**
   * Initialize internal data structures, invoked right after instantiation.
   *
   * @param conf     Configuration to use
   * @param proxy    The {@link RMProxy} instance to use
   * @param protocol The communication protocol to use
   */
  @Override
  public void init(Configuration conf, RMProxy<T> proxy,
                    Class<T> protocol) {
    this.protocol = protocol;
    try {
      YarnConfiguration yarnConf = new YarnConfiguration(conf);
      InetSocketAddress rmAddress =
          proxy.getRMAddress(yarnConf, protocol);
      LOG.info("Connecting to ResourceManager at {}", rmAddress);
      this.proxy = proxy.getProxy(yarnConf, protocol, rmAddress);
    } catch (IOException ioe) {
      LOG.error("Unable to create proxy to the ResourceManager ", ioe);
    }
  }

  @Override
  public Class<T> getInterface() {
    return protocol;
  }

  @Override
  public ProxyInfo<T> getProxy() {
    return new ProxyInfo<T>(proxy, null);
  }

  /**
   * PerformFailover does nothing in this class.
   * @param currentProxy
   */
  @Override
  public void performFailover(T currentProxy) {
    // Nothing to do.
  }

  /**
   * Close the current proxy.
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    RPC.stopProxy(proxy);
  }
}