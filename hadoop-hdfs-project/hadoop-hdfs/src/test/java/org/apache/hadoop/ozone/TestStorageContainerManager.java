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
package org.apache.hadoop.ozone;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.scm.StorageContainerManager;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.junit.Rule;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.hadoop.io.IOUtils;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

/**
 * Test class that exercises the StorageContainerManager.
 */
public class TestStorageContainerManager {
  private static XceiverClientManager xceiverClientManager =
      new XceiverClientManager(
      new OzoneConfiguration());
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testRpcPermission() throws IOException {
    // Test with default configuration
    OzoneConfiguration defaultConf = new OzoneConfiguration();
    testRpcPermissionWithConf(defaultConf, "unknownUser", true);

    // Test with ozone.administrators defined in configuration
    OzoneConfiguration ozoneConf = new OzoneConfiguration();
    ozoneConf.setStrings(OzoneConfigKeys.OZONE_ADMINISTRATORS,
        "adminUser1, adminUser2");
    // Non-admin user will get permission denied.
    testRpcPermissionWithConf(ozoneConf, "unknownUser", true);
    // Admin user will pass the permission check.
    testRpcPermissionWithConf(ozoneConf, "adminUser2", false);
  }

  private void testRpcPermissionWithConf(
      OzoneConfiguration ozoneConf, String fakeRemoteUsername,
      boolean expectPermissionDenied) throws IOException {
    MiniOzoneCluster cluster =
        new MiniOzoneCluster.Builder(ozoneConf).numDataNodes(1)
            .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();

    try {
      String fakeUser = fakeRemoteUsername;
      StorageContainerManager mockScm = Mockito.spy(
          cluster.getStorageContainerManager());
      Mockito.when(mockScm.getPpcRemoteUsername())
          .thenReturn(fakeUser);

      try {
        mockScm.deleteContainer("container1");
        fail("Operation should fail, expecting an IOException here.");
      } catch (Exception e) {
        if (expectPermissionDenied) {
          verifyPermissionDeniedException(e, fakeUser);
        } else {
          // If passes permission check, it should fail with
          // container not exist exception.
          Assert.assertTrue(e.getMessage()
              .contains("container doesn't exist"));
        }
      }

      try {
        Pipeline pipeLine2 = mockScm.allocateContainer(
            xceiverClientManager.getType(),
            OzoneProtos.ReplicationFactor.ONE, "container2");
        if (expectPermissionDenied) {
          fail("Operation should fail, expecting an IOException here.");
        } else {
          Assert.assertEquals("container2", pipeLine2.getContainerName());
        }
      } catch (Exception e) {
        verifyPermissionDeniedException(e, fakeUser);
      }

      try {
        Pipeline pipeLine3 = mockScm.allocateContainer(
            xceiverClientManager.getType(),
            OzoneProtos.ReplicationFactor.ONE, "container3");

        if (expectPermissionDenied) {
          fail("Operation should fail, expecting an IOException here.");
        } else {
          Assert.assertEquals("container3", pipeLine3.getContainerName());
          Assert.assertEquals(1, pipeLine3.getMachines().size());
        }
      } catch (Exception e) {
        verifyPermissionDeniedException(e, fakeUser);
      }

      try {
        mockScm.getContainer("container4");
        fail("Operation should fail, expecting an IOException here.");
      } catch (Exception e) {
        if (expectPermissionDenied) {
          verifyPermissionDeniedException(e, fakeUser);
        } else {
          // If passes permission check, it should fail with
          // key not exist exception.
          Assert.assertTrue(e.getMessage()
              .contains("Specified key does not exist"));
        }
      }
    } finally {
      IOUtils.cleanup(null, cluster);
    }
  }

  private void verifyPermissionDeniedException(Exception e, String userName) {
    String expectedErrorMessage = "Access denied for user "
        + userName + ". " + "Superuser privilege is required.";
    Assert.assertTrue(e instanceof IOException);
    Assert.assertEquals(expectedErrorMessage, e.getMessage());
  }
}
