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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.ozone.recon.GuiceInjectorUtilsForTestsImpl;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.api.types.ContainerMetadata;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.hadoop.ozone.recon.schema.StatsSchemaDefinition;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.inject.Injector;

import javax.sql.DataSource;

/**
 * Unit Tests for ContainerDBServiceProviderImpl.
 */
public class TestContainerDBServiceProviderImpl {

  @ClassRule
  public static TemporaryFolder tempFolder = new TemporaryFolder();
  private static ContainerDBServiceProvider containerDbServiceProvider;
  private static Injector injector;
  private static GuiceInjectorUtilsForTestsImpl guiceInjectorTest =
      new GuiceInjectorUtilsForTestsImpl();

  private String keyPrefix1 = "V3/B1/K1";
  private String keyPrefix2 = "V3/B1/K2";
  private String keyPrefix3 = "V3/B2/K1";

  private void populateKeysInContainers(long containerId1, long containerId2)
      throws Exception {

    ContainerKeyPrefix containerKeyPrefix1 = new
        ContainerKeyPrefix(containerId1, keyPrefix1, 0);
    containerDbServiceProvider.storeContainerKeyMapping(containerKeyPrefix1,
        1);

    ContainerKeyPrefix containerKeyPrefix2 = new ContainerKeyPrefix(
        containerId1, keyPrefix2, 0);
    containerDbServiceProvider.storeContainerKeyMapping(containerKeyPrefix2,
        2);

    ContainerKeyPrefix containerKeyPrefix3 = new ContainerKeyPrefix(
        containerId2, keyPrefix3, 0);

    containerDbServiceProvider.storeContainerKeyMapping(containerKeyPrefix3,
        3);
  }

  private static void initializeInjector() throws Exception {
    injector = guiceInjectorTest.getInjector(
        null, null, tempFolder);
  }

  @BeforeClass
  public static void setupOnce() throws Exception {

    initializeInjector();

    DSL.using(new DefaultConfiguration().set(
        injector.getInstance(DataSource.class)));

    containerDbServiceProvider = injector.getInstance(
        ContainerDBServiceProvider.class);

    StatsSchemaDefinition schemaDefinition = injector.getInstance(
        StatsSchemaDefinition.class);
    schemaDefinition.initializeSchema();
  }

  @Before
  public void setUp() throws Exception {
    // Reset containerDB before running each test
    containerDbServiceProvider.initNewContainerDB(null);
  }

  @Test
  public void testInitNewContainerDB() throws Exception {
    long containerId = System.currentTimeMillis();
    Map<ContainerKeyPrefix, Integer> prefixCounts = new HashMap<>();

    ContainerKeyPrefix ckp1 = new ContainerKeyPrefix(containerId,
        "V1/B1/K1", 0);
    prefixCounts.put(ckp1, 1);

    ContainerKeyPrefix ckp2 = new ContainerKeyPrefix(containerId,
        "V1/B1/K2", 0);
    prefixCounts.put(ckp2, 2);

    ContainerKeyPrefix ckp3 = new ContainerKeyPrefix(containerId,
        "V1/B2/K3", 0);
    prefixCounts.put(ckp3, 3);

    for (ContainerKeyPrefix prefix : prefixCounts.keySet()) {
      containerDbServiceProvider.storeContainerKeyMapping(
          prefix, prefixCounts.get(prefix));
    }

    assertEquals(1, containerDbServiceProvider
        .getCountForContainerKeyPrefix(ckp1).intValue());

    prefixCounts.clear();
    prefixCounts.put(ckp2, 12);
    prefixCounts.put(ckp3, 13);
    ContainerKeyPrefix ckp4 = new ContainerKeyPrefix(containerId,
        "V1/B3/K1", 0);
    prefixCounts.put(ckp4, 14);
    ContainerKeyPrefix ckp5 = new ContainerKeyPrefix(containerId,
        "V1/B3/K2", 0);
    prefixCounts.put(ckp5, 15);

    containerDbServiceProvider.initNewContainerDB(prefixCounts);
    Map<ContainerKeyPrefix, Integer> keyPrefixesForContainer =
        containerDbServiceProvider.getKeyPrefixesForContainer(containerId);

    assertEquals(4, keyPrefixesForContainer.size());
    assertEquals(12, keyPrefixesForContainer.get(ckp2).intValue());
    assertEquals(13, keyPrefixesForContainer.get(ckp3).intValue());
    assertEquals(14, keyPrefixesForContainer.get(ckp4).intValue());
    assertEquals(15, keyPrefixesForContainer.get(ckp5).intValue());

    assertEquals(0, containerDbServiceProvider
        .getCountForContainerKeyPrefix(ckp1).intValue());
  }

  @Test
  public void testStoreContainerKeyMapping() throws Exception {

    long containerId = System.currentTimeMillis();
    Map<String, Integer> prefixCounts = new HashMap<>();
    prefixCounts.put(keyPrefix1, 1);
    prefixCounts.put(keyPrefix2, 2);
    prefixCounts.put(keyPrefix3, 3);

    for (String prefix : prefixCounts.keySet()) {
      ContainerKeyPrefix containerKeyPrefix = new ContainerKeyPrefix(
          containerId, prefix, 0);
      containerDbServiceProvider.storeContainerKeyMapping(
          containerKeyPrefix, prefixCounts.get(prefix));
    }

    Assert.assertEquals(1,
        containerDbServiceProvider.getCountForContainerKeyPrefix(
            new ContainerKeyPrefix(containerId, keyPrefix1,
                0)).longValue());
    Assert.assertEquals(2,
        containerDbServiceProvider.getCountForContainerKeyPrefix(
            new ContainerKeyPrefix(containerId, keyPrefix2,
                0)).longValue());
    Assert.assertEquals(3,
        containerDbServiceProvider.getCountForContainerKeyPrefix(
            new ContainerKeyPrefix(containerId, keyPrefix3,
                0)).longValue());
  }

  @Test
  public void testStoreContainerKeyCount() throws Exception {
    long containerId = 1L;
    long nextContainerId = 2L;
    containerDbServiceProvider.storeContainerKeyCount(containerId, 2L);
    containerDbServiceProvider.storeContainerKeyCount(nextContainerId, 3L);

    assertEquals(2,
        containerDbServiceProvider.getKeyCountForContainer(containerId));
    assertEquals(3,
        containerDbServiceProvider.getKeyCountForContainer(nextContainerId));

    containerDbServiceProvider.storeContainerKeyCount(containerId, 20L);
    assertEquals(20,
        containerDbServiceProvider.getKeyCountForContainer(containerId));
  }

  @Test
  public void testGetKeyCountForContainer() throws Exception {
    long containerId = 1L;
    long nextContainerId = 2L;
    containerDbServiceProvider.storeContainerKeyCount(containerId, 2L);
    containerDbServiceProvider.storeContainerKeyCount(nextContainerId, 3L);

    assertEquals(2,
        containerDbServiceProvider.getKeyCountForContainer(containerId));
    assertEquals(3,
        containerDbServiceProvider.getKeyCountForContainer(nextContainerId));

    assertEquals(0,
        containerDbServiceProvider.getKeyCountForContainer(5L));
  }

  @Test
  public void testDoesContainerExists() throws Exception {
    long containerId = 1L;
    long nextContainerId = 2L;
    containerDbServiceProvider.storeContainerKeyCount(containerId, 2L);
    containerDbServiceProvider.storeContainerKeyCount(nextContainerId, 3L);

    assertTrue(containerDbServiceProvider.doesContainerExists(containerId));
    assertTrue(containerDbServiceProvider.doesContainerExists(nextContainerId));
    assertFalse(containerDbServiceProvider.doesContainerExists(0L));
    assertFalse(containerDbServiceProvider.doesContainerExists(3L));
  }

  @Test
  public void testGetCountForContainerKeyPrefix() throws Exception {
    long containerId = System.currentTimeMillis();

    containerDbServiceProvider.storeContainerKeyMapping(new
        ContainerKeyPrefix(containerId, keyPrefix1), 2);

    Integer count = containerDbServiceProvider.
        getCountForContainerKeyPrefix(new ContainerKeyPrefix(containerId,
            keyPrefix1));
    assertEquals(2L, count.longValue());

    count = containerDbServiceProvider.
        getCountForContainerKeyPrefix(new ContainerKeyPrefix(containerId,
            "invalid"));
    assertEquals(0L, count.longValue());
  }

  @Test
  public void testGetKeyPrefixesForContainer() throws Exception {
    long containerId = 1L;
    long nextContainerId = 2L;
    populateKeysInContainers(containerId, nextContainerId);

    ContainerKeyPrefix containerKeyPrefix1 = new
        ContainerKeyPrefix(containerId, keyPrefix1, 0);
    ContainerKeyPrefix containerKeyPrefix2 = new ContainerKeyPrefix(
        containerId, keyPrefix2, 0);
    ContainerKeyPrefix containerKeyPrefix3 = new ContainerKeyPrefix(
        nextContainerId, keyPrefix3, 0);


    Map<ContainerKeyPrefix, Integer> keyPrefixMap =
        containerDbServiceProvider.getKeyPrefixesForContainer(containerId);
    assertEquals(2, keyPrefixMap.size());

    assertEquals(1, keyPrefixMap.get(containerKeyPrefix1).longValue());
    assertEquals(2, keyPrefixMap.get(containerKeyPrefix2).longValue());

    keyPrefixMap = containerDbServiceProvider.getKeyPrefixesForContainer(
        nextContainerId);
    assertEquals(1, keyPrefixMap.size());
    assertEquals(3, keyPrefixMap.get(containerKeyPrefix3).longValue());
  }

  @Test
  public void testGetKeyPrefixesForContainerWithKeyPrefix() throws Exception {
    long containerId = 1L;
    long nextContainerId = 2L;
    populateKeysInContainers(containerId, nextContainerId);

    ContainerKeyPrefix containerKeyPrefix2 = new ContainerKeyPrefix(
        containerId, keyPrefix2, 0);

    Map<ContainerKeyPrefix, Integer> keyPrefixMap =
        containerDbServiceProvider.getKeyPrefixesForContainer(containerId,
            keyPrefix1);
    assertEquals(1, keyPrefixMap.size());
    assertEquals(2, keyPrefixMap.get(containerKeyPrefix2).longValue());

    keyPrefixMap = containerDbServiceProvider.getKeyPrefixesForContainer(
        nextContainerId, keyPrefix3);
    assertEquals(0, keyPrefixMap.size());

    // test for negative cases
    keyPrefixMap = containerDbServiceProvider.getKeyPrefixesForContainer(
        containerId, "V3/B1/invalid");
    assertEquals(0, keyPrefixMap.size());

    keyPrefixMap = containerDbServiceProvider.getKeyPrefixesForContainer(
        containerId, keyPrefix3);
    assertEquals(0, keyPrefixMap.size());

    keyPrefixMap = containerDbServiceProvider.getKeyPrefixesForContainer(
        10L, "");
    assertEquals(0, keyPrefixMap.size());
  }

  @Test
  public void testGetContainersWithPrevContainer() throws Exception {
    long containerId = 1L;
    long nextContainerId = 2L;
    populateKeysInContainers(containerId, nextContainerId);

    Map<Long, ContainerMetadata> containerMap =
        containerDbServiceProvider.getContainers(-1, 0L);
    assertEquals(2, containerMap.size());

    assertEquals(3, containerMap.get(containerId).getNumberOfKeys());
    assertEquals(3, containerMap.get(nextContainerId).getNumberOfKeys());

    // test if limit works
    containerMap = containerDbServiceProvider.getContainers(
        1, 0L);
    assertEquals(1, containerMap.size());
    assertNull(containerMap.get(nextContainerId));

    // test for prev key
    containerMap = containerDbServiceProvider.getContainers(
        -1, containerId);
    assertEquals(1, containerMap.size());
    // containerId must be skipped from containerMap result
    assertNull(containerMap.get(containerId));

    containerMap = containerDbServiceProvider.getContainers(
        -1, nextContainerId);
    assertEquals(0, containerMap.size());

    // test for negative cases
    containerMap = containerDbServiceProvider.getContainers(
        -1, 10L);
    assertEquals(0, containerMap.size());

    containerMap = containerDbServiceProvider.getContainers(
        0, containerId);
    assertEquals(0, containerMap.size());
  }

  @Test
  public void testDeleteContainerMapping() throws Exception {
    long containerId = 1L;
    long nextContainerId = 2L;
    populateKeysInContainers(containerId, nextContainerId);

    Map<ContainerKeyPrefix, Integer> keyPrefixMap =
        containerDbServiceProvider.getKeyPrefixesForContainer(containerId);
    assertEquals(2, keyPrefixMap.size());

    containerDbServiceProvider.deleteContainerMapping(new ContainerKeyPrefix(
        containerId, keyPrefix2, 0));
    keyPrefixMap =
        containerDbServiceProvider.getKeyPrefixesForContainer(containerId);
    assertEquals(1, keyPrefixMap.size());
  }

  @Test
  public void testGetCountForContainers() throws Exception {

    assertEquals(0, containerDbServiceProvider.getCountForContainers());

    containerDbServiceProvider.storeContainerCount(5L);

    assertEquals(5L, containerDbServiceProvider.getCountForContainers());
    containerDbServiceProvider.incrementContainerCountBy(1L);

    assertEquals(6L, containerDbServiceProvider.getCountForContainers());

    containerDbServiceProvider.storeContainerCount(10L);
    assertEquals(10L, containerDbServiceProvider.getCountForContainers());
  }

  @Test
  public void testStoreContainerCount() throws Exception {
    containerDbServiceProvider.storeContainerCount(3L);
    assertEquals(3L, containerDbServiceProvider.getCountForContainers());

    containerDbServiceProvider.storeContainerCount(5L);
    assertEquals(5L, containerDbServiceProvider.getCountForContainers());
  }

  @Test
  public void testIncrementContainerCountBy() throws Exception {
    assertEquals(0, containerDbServiceProvider.getCountForContainers());

    containerDbServiceProvider.incrementContainerCountBy(1L);
    assertEquals(1L, containerDbServiceProvider.getCountForContainers());

    containerDbServiceProvider.incrementContainerCountBy(3L);
    assertEquals(4L, containerDbServiceProvider.getCountForContainers());
  }
}
