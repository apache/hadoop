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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

@InterfaceAudience.Private
@InterfaceStability.Unstable
/**
 * An subclass of {@link RMFailoverProxyProvider} which try to
 * resolve the proxy DNS in the event of failover.
 * This provider don't support Federation.
 */
public class AutoRefreshHaRMFailoverProxyProvider<T>
    extends ConfiguredRMFailoverProxyProvider {
  private static final Log LOG =
      LogFactory.getLog(AutoRefreshHaRMFailoverProxyProvider.class);

  @Override
  public synchronized void performFailover(T currentProxy) {
    RPC.stopProxy(currentProxy);

    //clears out all keys that map to currentProxy
    Set<String> rmIds = new HashSet<>();
    for (Entry<K, V> entry : proxies.entrySet()) {
        if (entry.getValue().equals(currentProxy)) {
            rmIds.add(entry.getKey());
        }
    }
    for (String rmId : rmIds) {
      proxies.remove(rmId);
    }

    super.performFailover();
  }
}
