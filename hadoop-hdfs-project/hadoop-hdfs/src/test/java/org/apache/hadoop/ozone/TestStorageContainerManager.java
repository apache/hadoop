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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.ozone.scm.StorageContainerManager;
import org.apache.hadoop.scm.client.ScmClient;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.junit.Rule;
import org.junit.Assert;
// TODO : We need this when we enable these tests back.
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.rules.ExpectedException;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.scm.protocol.LocatedContainer;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

/**
 * Test class that exercises the StorageContainerManager.
 */
public class TestStorageContainerManager {
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void init() throws IOException {
    conf = new OzoneConfiguration();
  }

  @After
  public void shutdown() throws InterruptedException {
    IOUtils.cleanup(null, storageContainerLocationClient, cluster);
  }

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
    cluster = new MiniOzoneCluster.Builder(ozoneConf).numDataNodes(1)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();

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
      Pipeline pipeLine2 = mockScm.allocateContainer("container2");
      if(expectPermissionDenied) {
        fail("Operation should fail, expecting an IOException here.");
      } else {
        Assert.assertEquals("container2", pipeLine2.getContainerName());
      }
    } catch (Exception e) {
      verifyPermissionDeniedException(e, fakeUser);
    }

    try {
      Pipeline pipeLine3 = mockScm.allocateContainer("container3",
          ScmClient.ReplicationFactor.ONE);
      if(expectPermissionDenied) {
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
  }

  private void verifyPermissionDeniedException(Exception e, String userName) {
    String expectedErrorMessage = "Access denied for user "
        + userName + ". " + "Superuser privilege is required.";
    Assert.assertTrue(e instanceof IOException);
    Assert.assertEquals(expectedErrorMessage, e.getMessage());
  }

  // TODO : Disabling this test after verifying that failure is due
  // Not Implemented exception. Will turn on this test in next patch
  //@Test
  public void testLocationsForSingleKey() throws Exception {
    cluster = new MiniOzoneCluster.Builder(conf).numDataNodes(1)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    storageContainerLocationClient =
        cluster.createStorageContainerLocationClient();
    Set<LocatedContainer> containers =
        storageContainerLocationClient.getStorageContainerLocations(
            new LinkedHashSet<>(Arrays.asList("/key1")));
    assertNotNull(containers);
    assertEquals(1, containers.size());
    assertLocatedContainer(containers, "/key1", 1);
  }

  // TODO : Disabling this test after verifying that failure is due
  // Not Implemented exception. Will turn on this test in next patch
  //@Test
  public void testLocationsForMultipleKeys() throws Exception {
    cluster = new MiniOzoneCluster.Builder(conf).numDataNodes(1)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    storageContainerLocationClient =
        cluster.createStorageContainerLocationClient();
    Set<LocatedContainer> containers =
        storageContainerLocationClient.getStorageContainerLocations(
            new LinkedHashSet<>(Arrays.asList("/key1", "/key2", "/key3")));
    assertNotNull(containers);
    assertEquals(3, containers.size());
    assertLocatedContainer(containers, "/key1", 1);
    assertLocatedContainer(containers, "/key2", 1);
    assertLocatedContainer(containers, "/key3", 1);
  }
  // TODO : Disabling this test after verifying that failure is due
  // Not Implemented exception. Will turn on this test in next patch
  //@Test
  public void testNoDataNodes() throws Exception {
    cluster = new MiniOzoneCluster.Builder(conf).numDataNodes(0)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED)
        .doNotwaitTobeOutofChillMode()
        .build();
    storageContainerLocationClient =
        cluster.createStorageContainerLocationClient();
    exception.expect(IOException.class);
    exception.expectMessage("locations not found");
    storageContainerLocationClient.getStorageContainerLocations(
        new LinkedHashSet<>(Arrays.asList("/key1")));
  }

  // TODO : Disabling this test after verifying that failure is due
  // Not Implemented exception. Will turn on this test in next patch
  //@Test
  public void testMultipleDataNodes() throws Exception {
    cluster = new MiniOzoneCluster.Builder(conf).numDataNodes(3)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    storageContainerLocationClient =
        cluster.createStorageContainerLocationClient();
    Set<LocatedContainer> containers =
        storageContainerLocationClient.getStorageContainerLocations(
            new LinkedHashSet<>(Arrays.asList("/key1")));
    assertNotNull(containers);
    assertEquals(1, containers.size());
    assertLocatedContainer(containers, "/key1", 3);
  }

  private static void assertLocatedContainer(Set<LocatedContainer> containers,
      String key, int expectedNumLocations) {
    LocatedContainer container = null;
    for (LocatedContainer curContainer: containers) {
      if (key.equals(curContainer.getKey())) {
        container = curContainer;
        break;
      }
    }
    assertNotNull("Container for key " + key + " not found.", container);
    assertEquals(key, container.getKey());
    assertNotNull(container.getMatchedKeyPrefix());
    assertFalse(container.getMatchedKeyPrefix().isEmpty());
    assertNotNull(container.getContainerName());
    assertFalse(container.getContainerName().isEmpty());
    assertNotNull(container.getLocations());
    assertEquals(expectedNumLocations, container.getLocations().size());
    assertNotNull(container.getLeader());
  }
}
