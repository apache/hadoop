/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.service;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.service.conf.YarnServiceConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

/**
 * Minicluster test that verifies registry cleanup when app lifetime is
 * exceeded.
 */
public class TestCleanupAfterKill extends ServiceTestUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestCleanupAfterKill.class);

  @BeforeEach
  public void setup() throws Exception {
    File tmpYarnDir = new File("target", "tmp");
    FileUtils.deleteQuietly(tmpYarnDir);
  }

  @AfterEach
  public void tearDown() throws IOException {
    shutdown();
  }

  @Test
  @Timeout(200000)
  void testRegistryCleanedOnLifetimeExceeded() throws Exception {
    setupInternal(NUM_NMS);
    ServiceClient client = createClient(getConf());
    Service exampleApp = createExampleApplication();
    exampleApp.setLifetime(30L);
    client.actionCreate(exampleApp);
    waitForServiceToBeStable(client, exampleApp);
    String serviceZKPath = RegistryUtils.servicePath(RegistryUtils
        .currentUser(), YarnServiceConstants.APP_TYPE, exampleApp.getName());
    assertTrue(getCuratorService().zkPathExists(serviceZKPath),
        "Registry ZK service path doesn't exist");

    // wait for app to be killed by RM
    ApplicationId exampleAppId = ApplicationId.fromString(exampleApp.getId());
    GenericTestUtils.waitFor(() -> {
      try {
        ApplicationReport ar = client.getYarnClient()
            .getApplicationReport(exampleAppId);
        return ar.getYarnApplicationState() == YarnApplicationState.KILLED;
      } catch (YarnException | IOException e) {
        throw new RuntimeException("while waiting", e);
      }
    }, 2000, 200000);
    assertFalse(getCuratorService().zkPathExists(serviceZKPath),
        "Registry ZK service path still exists after killed");

    LOG.info("Destroy the service");
    assertEquals(0, client.actionDestroy(exampleApp.getName()));
  }
}
