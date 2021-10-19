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

import java.util.Map.Entry;

import java.util.Set;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.RPC;

/**
 * A subclass of {@link RMFailoverProxyProvider} which tries to
 * resolve the proxy DNS in the event of failover.
 * This provider supports YARN Resourcemanager's HA mode.
 * This provider doesn't support Federation.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AutoRefreshRMFailoverProxyProvider<T>
    extends ConfiguredRMFailoverProxyProvider<T> {
  private static final Logger LOG =
      LoggerFactory.getLogger(AutoRefreshRMFailoverProxyProvider.class);

  @Override
  public synchronized void performFailover(T currentProxy) {
    RPC.stopProxy(currentProxy);

    //clears out all keys that map to currentProxy
    Set<String> rmIds = new HashSet<>();
    for (Entry<String, T> entry : proxies.entrySet()) {
      T proxy = entry.getValue();
      if (proxy.equals(currentProxy)) {
        String rmId = entry.getKey();
        rmIds.add(rmId);
      }
    }
    for (String rmId : rmIds) {
      proxies.remove(rmId);
    }

    super.performFailover(currentProxy);
  }
}
