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

package org.apache.hadoop.yarn.submarine.common;

import org.apache.hadoop.yarn.submarine.common.fs.MockRemoteDirectoryManager;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.io.IOException;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockClientContext extends ClientContext {
  private MockRemoteDirectoryManager remoteDirectoryMgr =
      new MockRemoteDirectoryManager();

  @Override
  public RemoteDirectoryManager getRemoteDirectoryManager() {
    return remoteDirectoryMgr;
  }

  @Override
  public synchronized YarnClient getOrCreateYarnClient() {
    YarnClient client = mock(YarnClient.class);
    try {
      when(client.getResourceTypeInfo()).thenReturn(
          ResourceUtils.getResourcesTypeInfo());
    } catch (YarnException e) {
      fail(e.getMessage());
    } catch (IOException e) {
      fail(e.getMessage());
    }
    return client;
  }
}
