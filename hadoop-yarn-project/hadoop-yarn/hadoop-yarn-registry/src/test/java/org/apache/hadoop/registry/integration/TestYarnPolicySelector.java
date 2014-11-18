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

package org.apache.hadoop.registry.integration;

import org.apache.hadoop.registry.RegistryTestHelper;
import org.apache.hadoop.registry.client.types.yarn.PersistencePolicies;
import org.apache.hadoop.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.server.integration.SelectByYarnPersistence;
import org.apache.hadoop.registry.server.services.RegistryAdminService;
import org.junit.Test;

public class TestYarnPolicySelector extends RegistryTestHelper {


  private ServiceRecord record = createRecord("1",
      PersistencePolicies.APPLICATION, "one",
      null);
  private RegistryPathStatus status = new RegistryPathStatus("/", 0, 0, 1);

  public void assertSelected(boolean outcome,
      RegistryAdminService.NodeSelector selector) {
    boolean select = selector.shouldSelect("/", status, record);
    assertEquals(selector.toString(), outcome, select);
  }

  @Test
  public void testByContainer() throws Throwable {
    assertSelected(false,
        new SelectByYarnPersistence("1",
            PersistencePolicies.CONTAINER));
  }

  @Test
  public void testByApp() throws Throwable {
    assertSelected(true,
        new SelectByYarnPersistence("1",
            PersistencePolicies.APPLICATION));
  }


  @Test
  public void testByAppName() throws Throwable {
    assertSelected(false,
        new SelectByYarnPersistence("2",
            PersistencePolicies.APPLICATION));
  }

}
