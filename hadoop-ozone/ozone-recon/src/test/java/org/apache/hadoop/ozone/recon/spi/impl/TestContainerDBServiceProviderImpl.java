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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.utils.MetaStoreIterator;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Unit Tests for ContainerDBServiceProviderImpl.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestContainerDBServiceProviderImpl {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private MetadataStore containerDBStore;
  private ContainerDBServiceProvider containerDbServiceProvider
      = new ContainerDBServiceProviderImpl();
  private Injector injector;

  @Before
  public void setUp() throws IOException {
    tempFolder.create();
    File dbDir = tempFolder.getRoot();
    containerDBStore = MetadataStoreBuilder.newBuilder()
        .setConf(new OzoneConfiguration())
        .setCreateIfMissing(true)
        .setDbFile(dbDir)
        .build();
    injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetadataStore.class).toInstance(containerDBStore);
        bind(ContainerDBServiceProvider.class)
            .toInstance(containerDbServiceProvider);
      }
    });
  }

  @After
  public void tearDown() throws Exception {
    tempFolder.delete();
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
          containerId, prefix);
      containerDbServiceProvider.storeContainerKeyMapping(
          containerKeyPrefix, prefixCounts.get(prefix));
    }

    int count = 0;
    MetaStoreIterator<MetadataStore.KeyValue> iterator =
        containerDBStore.iterator();
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    assertTrue(count == 3);
  }

  @Test
  public void testGetCountForForContainerKeyPrefix() throws Exception {
    long containerId = System.currentTimeMillis();

    containerDbServiceProvider.storeContainerKeyMapping(new
        ContainerKeyPrefix(containerId, "V1/B1/K1"), 2);

    Integer count = containerDbServiceProvider.
        getCountForForContainerKeyPrefix(new ContainerKeyPrefix(containerId,
            "V1/B1/K1"));
    assertTrue(count == 2);
  }

  @Test
  public void testGetKeyPrefixesForContainer() throws Exception {
    long containerId = System.currentTimeMillis();

    containerDbServiceProvider.storeContainerKeyMapping(new
        ContainerKeyPrefix(containerId, "V1/B1/K1"), 1);

    containerDbServiceProvider.storeContainerKeyMapping(new
        ContainerKeyPrefix(containerId, "V1/B1/K2"), 2);

    long nextContainerId = System.currentTimeMillis();
    containerDbServiceProvider.storeContainerKeyMapping(new
        ContainerKeyPrefix(nextContainerId, "V1/B2/K1"), 3);

    Map<String, Integer> keyPrefixMap = containerDbServiceProvider
        .getKeyPrefixesForContainer(containerId);
    assertTrue(keyPrefixMap.size() == 2);
    assertTrue(keyPrefixMap.get("V1/B1/K1") == 1);
    assertTrue(keyPrefixMap.get("V1/B1/K2") == 2);

    keyPrefixMap = containerDbServiceProvider
        .getKeyPrefixesForContainer(nextContainerId);
    assertTrue(keyPrefixMap.size() == 1);
    assertTrue(keyPrefixMap.get("V1/B2/K1") == 3);
  }
}
