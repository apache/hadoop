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
import org.apache.hadoop.yarn.server.webproxy.ProxyCA;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestProxyCAManager {

  @Test
  public void testBasics() throws Exception {
    ProxyCA proxyCA = spy(new ProxyCA());
    RMContext rmContext = mock(RMContext.class);
    ProxyCAManager proxyCAManager = new ProxyCAManager(proxyCA, rmContext);
    proxyCAManager.init(new YarnConfiguration());
    Assert.assertEquals(proxyCA, proxyCAManager.getProxyCA());
    verify(proxyCA, times(0)).init();
    Assert.assertNull(proxyCA.getCaCert());
    Assert.assertNull(proxyCA.getCaKeyPair());

    proxyCAManager.start();
    verify(proxyCA, times(1)).init();
    Assert.assertNotNull(proxyCA.getCaCert());
    Assert.assertNotNull(proxyCA.getCaKeyPair());
  }
}
