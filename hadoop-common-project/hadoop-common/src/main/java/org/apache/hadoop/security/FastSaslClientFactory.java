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
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Class for dealing with caching SASL client factories.
 */
@InterfaceAudience.LimitedPrivate({ "HDFS", "MapReduce" })
public class FastSaslClientFactory implements SaslClientFactory {
  private final Map<String, List<SaslClientFactory>> factoryCache =
      new HashMap<String, List<SaslClientFactory>>();

  public FastSaslClientFactory(Map<String, ?> props) {
    final Enumeration<SaslClientFactory> factories =
        Sasl.getSaslClientFactories();
    while (factories.hasMoreElements()) {
      SaslClientFactory factory = factories.nextElement();
      for (String mech : factory.getMechanismNames(props)) {
        if (!factoryCache.containsKey(mech)) {
          factoryCache.put(mech, new ArrayList<SaslClientFactory>());
        }
        factoryCache.get(mech).add(factory);
      }
    }
  }

  @Override
  public String[] getMechanismNames(Map<String, ?> props) {
    return factoryCache.keySet().toArray(new String[0]);
  }

  @Override
  public SaslClient createSaslClient(String[] mechanisms,
      String authorizationId, String protocol, String serverName,
      Map<String, ?> props, CallbackHandler cbh) throws SaslException {
    for (String mechanism : mechanisms) {
      List<SaslClientFactory> factories = factoryCache.get(mechanism);
      if (factories != null) {
        for (SaslClientFactory factory : factories) {
          SaslClient saslClient =
              factory.createSaslClient(new String[] {mechanism},
                  authorizationId, protocol, serverName, props, cbh);
          if (saslClient != null) {
            return saslClient;
          }
        }
      }
    }
    return null;
  }
}