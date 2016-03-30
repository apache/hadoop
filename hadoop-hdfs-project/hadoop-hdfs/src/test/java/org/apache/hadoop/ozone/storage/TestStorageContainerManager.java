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

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.protocol.LocatedContainer;
import org.apache.hadoop.ozone.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;

public class TestStorageContainerManager {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void init() throws IOException {
    conf = new OzoneConfiguration();
    conf.setBoolean(OzoneConfigKeys.DFS_OBJECTSTORE_ENABLED_KEY, true);
    conf.set(OzoneConfigKeys.DFS_STORAGE_HANDLER_TYPE_KEY, "distributed");
    conf.setBoolean(OzoneConfigKeys.DFS_OBJECTSTORE_TRACE_ENABLED_KEY, true);
  }

  @After
  public void shutdown() throws InterruptedException {
    IOUtils.cleanup(null, storageContainerLocationClient, cluster);
  }

  @Test
  public void testLocationsForSingleKey() throws IOException {
    cluster = new MiniOzoneCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitOzoneReady();
    storageContainerLocationClient =
        cluster.createStorageContainerLocationClient();
    Set<LocatedContainer> containers =
        storageContainerLocationClient.getStorageContainerLocations(
            new LinkedHashSet<>(Arrays.asList("/key1")));
    assertNotNull(containers);
    assertEquals(1, containers.size());
    assertLocatedContainer(containers, "/key1", 1);
  }

  @Test
  public void testLocationsForMultipleKeys() throws IOException {
    cluster = new MiniOzoneCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitOzoneReady();
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

  @Test
  public void testNoDataNodes() throws IOException {
    cluster = new MiniOzoneCluster.Builder(conf).numDataNodes(0).build();
    cluster.waitOzoneReady();
    storageContainerLocationClient =
        cluster.createStorageContainerLocationClient();
    exception.expect(IOException.class);
    exception.expectMessage("locations not found");
    storageContainerLocationClient.getStorageContainerLocations(
        new LinkedHashSet<>(Arrays.asList("/key1")));
  }

  @Test
  public void testMultipleDataNodes() throws IOException {
    cluster = new MiniOzoneCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitOzoneReady();
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
