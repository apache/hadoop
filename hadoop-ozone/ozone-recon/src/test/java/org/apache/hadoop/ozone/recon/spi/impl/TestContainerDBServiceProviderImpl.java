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

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.utils.db.DBStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;

/**
 * Unit Tests for ContainerDBServiceProviderImpl.
 */
public class TestContainerDBServiceProviderImpl {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private ContainerDBServiceProvider containerDbServiceProvider;
  private Injector injector;

  @Before
  public void setUp() throws IOException {
    tempFolder.create();
    injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        File dbDir = tempFolder.getRoot();
        OzoneConfiguration configuration = new OzoneConfiguration();
        configuration.set(OZONE_RECON_DB_DIR, dbDir.getAbsolutePath());
        bind(OzoneConfiguration.class).toInstance(configuration);
        bind(DBStore.class).toProvider(ReconContainerDBProvider.class).in(
            Singleton.class);
        bind(ContainerDBServiceProvider.class).to(
            ContainerDBServiceProviderImpl.class).in(Singleton.class);
      }
    });
    containerDbServiceProvider = injector.getInstance(
        ContainerDBServiceProvider.class);
  }

  @After
  public void tearDown() throws Exception {
    tempFolder.delete();
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
        .getCountForForContainerKeyPrefix(ckp1).intValue());

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
        .getCountForForContainerKeyPrefix(ckp1).intValue());
  }

  @Test
  public void testStoreContainerKeyMapping() throws Exception {

    long containerId = System.currentTimeMillis();
    Map<String, Integer> prefixCounts = new HashMap<>();
    prefixCounts.put("V1/B1/K1", 1);
    prefixCounts.put("V1/B1/K2", 2);
    prefixCounts.put("V1/B2/K3", 3);

    for (String prefix : prefixCounts.keySet()) {
      ContainerKeyPrefix containerKeyPrefix = new ContainerKeyPrefix(
          containerId, prefix, 0);
      containerDbServiceProvider.storeContainerKeyMapping(
          containerKeyPrefix, prefixCounts.get(prefix));
    }

    Assert.assertTrue(
        containerDbServiceProvider.getCountForForContainerKeyPrefix(
            new ContainerKeyPrefix(containerId, "V1/B1/K1",
                0)) == 1);
    Assert.assertTrue(
        containerDbServiceProvider.getCountForForContainerKeyPrefix(
            new ContainerKeyPrefix(containerId, "V1/B1/K2",
                0)) == 2);
    Assert.assertTrue(
        containerDbServiceProvider.getCountForForContainerKeyPrefix(
            new ContainerKeyPrefix(containerId, "V1/B2/K3",
                0)) == 3);
  }

  @Test
  public void testGetCountForForContainerKeyPrefix() throws Exception {
    long containerId = System.currentTimeMillis();

    containerDbServiceProvider.storeContainerKeyMapping(new
        ContainerKeyPrefix(containerId, "V2/B1/K1"), 2);

    Integer count = containerDbServiceProvider.
        getCountForForContainerKeyPrefix(new ContainerKeyPrefix(containerId,
            "V2/B1/K1"));
    assertTrue(count == 2);
  }

  @Test
  public void testGetKeyPrefixesForContainer() throws Exception {
    long containerId = System.currentTimeMillis();

    ContainerKeyPrefix containerKeyPrefix1 = new
        ContainerKeyPrefix(containerId, "V3/B1/K1", 0);
    containerDbServiceProvider.storeContainerKeyMapping(containerKeyPrefix1,
        1);

    ContainerKeyPrefix containerKeyPrefix2 = new ContainerKeyPrefix(
        containerId, "V3/B1/K2", 0);
    containerDbServiceProvider.storeContainerKeyMapping(containerKeyPrefix2,
        2);

    long nextContainerId = containerId + 1000L;
    ContainerKeyPrefix containerKeyPrefix3 = new ContainerKeyPrefix(
        nextContainerId, "V3/B2/K1", 0);
    containerDbServiceProvider.storeContainerKeyMapping(containerKeyPrefix3,
        3);

    Map<ContainerKeyPrefix, Integer> keyPrefixMap =
        containerDbServiceProvider.getKeyPrefixesForContainer(containerId);
    assertTrue(keyPrefixMap.size() == 2);

    assertTrue(keyPrefixMap.get(containerKeyPrefix1) == 1);
    assertTrue(keyPrefixMap.get(containerKeyPrefix2) == 2);

    keyPrefixMap = containerDbServiceProvider.getKeyPrefixesForContainer(
        nextContainerId);
    assertTrue(keyPrefixMap.size() == 1);
    assertTrue(keyPrefixMap.get(containerKeyPrefix3) == 3);
  }
}
