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

package org.apache.hadoop.yarn.server.resourcemanager.security;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.webproxy.ProxyCA;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

/**
 * Manager for {@link ProxyCA}, which contains the Certificate Authority for
 * AMs to have certificates for HTTPS communication with the RM Proxy.
 */
@Private
@InterfaceStability.Unstable
public class ProxyCAManager extends AbstractService implements Recoverable {
  private static final Logger LOG =
      LoggerFactory.getLogger(ProxyCAManager.class);

  private ProxyCA proxyCA;
  private RMContext rmContext;
  private boolean wasRecovered;

  public ProxyCAManager(ProxyCA proxyCA, RMContext rmContext) {
    super(ProxyCAManager.class.getName());
    this.proxyCA = proxyCA;
    this.rmContext = rmContext;
    wasRecovered = false;
  }

  @Override
  protected void serviceStart() throws Exception {
    if (!wasRecovered) {
      proxyCA.init();
    }
    wasRecovered = false;
    rmContext.getStateStore().storeProxyCACert(
        proxyCA.getCaCert(), proxyCA.getCaKeyPair().getPrivate());
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
  }

  public ProxyCA getProxyCA() {
    return proxyCA;
  }

  public void recover(RMState state)
      throws GeneralSecurityException, IOException {
    LOG.info("Recovering CA Certificate and Private Key");
    X509Certificate caCert = state.getProxyCAState().getCaCert();
    PrivateKey caPrivateKey = state.getProxyCAState().getCaPrivateKey();
    proxyCA.init(caCert, caPrivateKey);
    wasRecovered = true;
  }
}
