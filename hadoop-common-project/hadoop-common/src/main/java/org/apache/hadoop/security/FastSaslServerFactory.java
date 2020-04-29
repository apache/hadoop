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
package org.apache.hadoop.security;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Class for dealing with caching SASL server factories.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
public class FastSaslServerFactory implements SaslServerFactory {
  private final Map<String, List<SaslServerFactory>> factoryCache =
      new HashMap<String, List<SaslServerFactory>>();

  public FastSaslServerFactory(Map<String, ?> props) {
    final Enumeration<SaslServerFactory> factories =
        Sasl.getSaslServerFactories();
    while (factories.hasMoreElements()) {
      SaslServerFactory factory = factories.nextElement();
      for (String mech : factory.getMechanismNames(props)) {
        if (!factoryCache.containsKey(mech)) {
          factoryCache.put(mech, new ArrayList<SaslServerFactory>());
        }
        factoryCache.get(mech).add(factory);
      }
    }
  }

  @Override
  public SaslServer createSaslServer(String mechanism, String protocol,
      String serverName, Map<String, ?> props, CallbackHandler cbh)
      throws SaslException {
    SaslServer saslServer = null;
    List<SaslServerFactory> factories = factoryCache.get(mechanism);
    if (factories != null) {
      for (SaslServerFactory factory : factories) {
        saslServer = factory.createSaslServer(
            mechanism, protocol, serverName, props, cbh);
        if (saslServer != null) {
          break;
        }
      }
    }
    return saslServer;
  }

  @Override
  public String[] getMechanismNames(Map<String, ?> props) {
    return factoryCache.keySet().toArray(new String[0]);
  }
}