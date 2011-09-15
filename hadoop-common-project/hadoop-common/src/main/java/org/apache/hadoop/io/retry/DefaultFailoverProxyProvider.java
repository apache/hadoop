/*
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
package org.apache.hadoop.io.retry;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.RPC;

/**
 * An implementation of {@link FailoverProxyProvider} which does nothing in the
 * event of failover, and always returns the same proxy object. 
 */
@InterfaceStability.Evolving
public class DefaultFailoverProxyProvider implements FailoverProxyProvider {
  
  private Object proxy;
  private Class<?> iface;
  
  public DefaultFailoverProxyProvider(Class<?> iface, Object proxy) {
    this.proxy = proxy;
    this.iface = iface;
  }

  @Override
  public Class<?> getInterface() {
    return iface;
  }

  @Override
  public Object getProxy() {
    return proxy;
  }

  @Override
  public void performFailover(Object currentProxy) {
    // Nothing to do.
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(proxy);
  }

}
