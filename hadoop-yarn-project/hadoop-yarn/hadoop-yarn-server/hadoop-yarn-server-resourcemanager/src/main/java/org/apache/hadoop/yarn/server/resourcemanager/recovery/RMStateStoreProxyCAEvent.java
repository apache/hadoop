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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;

/**
 * A event used to store ProxyCA information.
 */
public class RMStateStoreProxyCAEvent extends RMStateStoreEvent {
  private X509Certificate caCert;
  private PrivateKey caPrivateKey;

  public RMStateStoreProxyCAEvent(RMStateStoreEventType type) {
    super(type);
  }

  public RMStateStoreProxyCAEvent(X509Certificate caCert,
      PrivateKey caPrivateKey, RMStateStoreEventType type) {
    this(type);
    this.caCert = caCert;
    this.caPrivateKey = caPrivateKey;
  }

  public X509Certificate getCaCert() {
    return caCert;
  }

  public PrivateKey getCaPrivateKey() {
    return caPrivateKey;
  }
}
