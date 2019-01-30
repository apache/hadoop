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

package org.apache.hadoop.yarn.factories.impl.pb;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Test class for RpcClientFactoryPBImpl.
 */
public class TestRpcClientFactoryPBImpl {
  @Test
  public void testToUseCustomClassloader() throws Exception {
    Configuration configuration = mock(Configuration.class);
    RpcClientFactoryPBImpl rpcClientFactoryPB = RpcClientFactoryPBImpl.get();
    try {
      rpcClientFactoryPB.getClient(
          Class.forName("org.apache.hadoop.yarn.api.ApplicationClientProtocol"),
          -1, new InetSocketAddress(0), configuration);
    } catch (Exception e) {
      // Do nothing
    }
    verify(configuration, atLeastOnce()).getClassByName(anyString());
  }

}