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


import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.webproxy.ProxyCA;
import org.junit.Assert;
import org.junit.Test;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestProxyCAManager {

  @Test
  public void testBasics() throws Exception {
    ProxyCA proxyCA = spy(new ProxyCA());
    RMContext rmContext = mock(RMContext.class);
    RMStateStore rmStateStore = mock(RMStateStore.class);
    when(rmContext.getStateStore()).thenReturn(rmStateStore);
    ProxyCAManager proxyCAManager = new ProxyCAManager(proxyCA, rmContext);
    proxyCAManager.init(new YarnConfiguration());
    Assert.assertEquals(proxyCA, proxyCAManager.getProxyCA());
    verify(rmContext, times(0)).getStateStore();
    verify(rmStateStore, times(0)).storeProxyCACert(any(), any());
    verify(proxyCA, times(0)).init();
    Assert.assertNull(proxyCA.getCaCert());
    Assert.assertNull(proxyCA.getCaKeyPair());

    proxyCAManager.start();
    verify(rmContext, times(1)).getStateStore();
    verify(rmStateStore, times(1)).storeProxyCACert(proxyCA.getCaCert(),
        proxyCA.getCaKeyPair().getPrivate());
    verify(proxyCA, times(1)).init();
    Assert.assertNotNull(proxyCA.getCaCert());
    Assert.assertNotNull(proxyCA.getCaKeyPair());
  }

  @Test
  public void testRecover() throws Exception {
    ProxyCA proxyCA = spy(new ProxyCA());
    RMContext rmContext = mock(RMContext.class);
    RMStateStore rmStateStore = mock(RMStateStore.class);
    when(rmContext.getStateStore()).thenReturn(rmStateStore);
    ProxyCAManager proxyCAManager = new ProxyCAManager(proxyCA, rmContext);
    proxyCAManager.init(new YarnConfiguration());
    Assert.assertEquals(proxyCA, proxyCAManager.getProxyCA());
    verify(rmContext, times(0)).getStateStore();
    verify(rmStateStore, times(0)).storeProxyCACert(any(), any());
    verify(proxyCA, times(0)).init();
    Assert.assertNull(proxyCA.getCaCert());
    Assert.assertNull(proxyCA.getCaKeyPair());

    RMStateStore.RMState rmState = mock(RMStateStore.RMState.class);
    RMStateStore.ProxyCAState proxyCAState =
        mock(RMStateStore.ProxyCAState.class);
    // We need to use a real certificate + private key because of validation
    // so just grab them from another ProxyCA
    ProxyCA otherProxyCA = new ProxyCA();
    otherProxyCA.init();
    X509Certificate certificate = otherProxyCA.getCaCert();
    when(proxyCAState.getCaCert()).thenReturn(certificate);
    PrivateKey privateKey = otherProxyCA.getCaKeyPair().getPrivate();
    when(proxyCAState.getCaPrivateKey()).thenReturn(privateKey);
    when(rmState.getProxyCAState()).thenReturn(proxyCAState);
    proxyCAManager.recover(rmState);
    verify(proxyCA, times(1)).init(certificate, privateKey);
    Assert.assertEquals(certificate, proxyCA.getCaCert());
    Assert.assertEquals(privateKey, proxyCA.getCaKeyPair().getPrivate());

    proxyCAManager.start();
    verify(rmContext, times(1)).getStateStore();
    verify(rmStateStore, times(1)).storeProxyCACert(proxyCA.getCaCert(),
        proxyCA.getCaKeyPair().getPrivate());
    verify(proxyCA, times(0)).init();
    Assert.assertEquals(certificate, proxyCA.getCaCert());
    Assert.assertEquals(privateKey, proxyCA.getCaKeyPair().getPrivate());
  }
}
