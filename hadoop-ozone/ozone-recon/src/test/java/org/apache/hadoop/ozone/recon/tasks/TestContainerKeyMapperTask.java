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

package org.apache.hadoop.ozone.recon.tasks;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.AbstractOMMetadataManagerTest;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.ContainerDBServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.ReconContainerDBProvider;
import org.apache.hadoop.utils.db.DBCheckpoint;
import org.apache.hadoop.utils.db.DBStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;

/**
 * Unit test for Container Key mapper task.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*"})
@PrepareForTest(ReconUtils.class)
public class TestContainerKeyMapperTask extends AbstractOMMetadataManagerTest {

  private ContainerDBServiceProvider containerDbServiceProvider;
  private OMMetadataManager omMetadataManager;
  private ReconOMMetadataManager reconOMMetadataManager;
  private Injector injector;
  private OzoneManagerServiceProviderImpl ozoneManagerServiceProvider;

  @Before
  public void setUp() throws Exception {
    omMetadataManager = initializeNewOmMetadataManager();
    injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        try {
          bind(OzoneConfiguration.class).toInstance(
              getTestOzoneConfiguration());

          reconOMMetadataManager = getTestMetadataManager(omMetadataManager);
          bind(ReconOMMetadataManager.class).toInstance(reconOMMetadataManager);
          ozoneManagerServiceProvider = new OzoneManagerServiceProviderImpl(
              getTestOzoneConfiguration());
          bind(OzoneManagerServiceProvider.class)
              .toInstance(ozoneManagerServiceProvider);

          bind(DBStore.class).toProvider(ReconContainerDBProvider.class).
              in(Singleton.class);
          bind(ContainerDBServiceProvider.class).to(
              ContainerDBServiceProviderImpl.class).in(Singleton.class);
        } catch (IOException e) {
          Assert.fail();
        }
      }
    });
    containerDbServiceProvider = injector.getInstance(
        ContainerDBServiceProvider.class);
  }

  @Test
  public void testRun() throws Exception{

    Map<ContainerKeyPrefix, Integer> keyPrefixesForContainer =
        containerDbServiceProvider.getKeyPrefixesForContainer(1);
    assertTrue(keyPrefixesForContainer.isEmpty());

    keyPrefixesForContainer = containerDbServiceProvider
        .getKeyPrefixesForContainer(2);
    assertTrue(keyPrefixesForContainer.isEmpty());

    Pipeline pipeline = getRandomPipeline();

    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    BlockID blockID1 = new BlockID(1, 1);
    OmKeyLocationInfo omKeyLocationInfo1 = getOmKeyLocationInfo(blockID1,
        pipeline);

    BlockID blockID2 = new BlockID(2, 1);
    OmKeyLocationInfo omKeyLocationInfo2
        = getOmKeyLocationInfo(blockID2, pipeline);

    omKeyLocationInfoList.add(omKeyLocationInfo1);
    omKeyLocationInfoList.add(omKeyLocationInfo2);

    OmKeyLocationInfoGroup omKeyLocationInfoGroup = new
        OmKeyLocationInfoGroup(0, omKeyLocationInfoList);

    writeDataToOm(omMetadataManager,
        "key_one",
        "bucketOne",
        "sampleVol",
        Collections.singletonList(omKeyLocationInfoGroup));

    //Take snapshot of OM DB and copy over to Recon OM DB.
    DBCheckpoint checkpoint = omMetadataManager.getStore()
        .getCheckpoint(true);
    File tarFile = OmUtils.createTarFile(checkpoint.getCheckpointLocation());
    InputStream inputStream = new FileInputStream(tarFile);
    PowerMockito.stub(PowerMockito.method(ReconUtils.class,
        "makeHttpCall",
        CloseableHttpClient.class, String.class))
        .toReturn(inputStream);

    ContainerKeyMapperTask containerKeyMapperTask = new ContainerKeyMapperTask(
        ozoneManagerServiceProvider, containerDbServiceProvider);
    containerKeyMapperTask.run();

    keyPrefixesForContainer =
        containerDbServiceProvider.getKeyPrefixesForContainer(1);
    assertTrue(keyPrefixesForContainer.size() == 1);
    String omKey = omMetadataManager.getOzoneKey("sampleVol",
        "bucketOne", "key_one");
    ContainerKeyPrefix containerKeyPrefix = new ContainerKeyPrefix(1,
        omKey, 0);
    assertEquals(1,
        keyPrefixesForContainer.get(containerKeyPrefix).intValue());

    keyPrefixesForContainer =
        containerDbServiceProvider.getKeyPrefixesForContainer(2);
    assertTrue(keyPrefixesForContainer.size() == 1);
    containerKeyPrefix = new ContainerKeyPrefix(2, omKey,
        0);
    assertEquals(1,
        keyPrefixesForContainer.get(containerKeyPrefix).intValue());
  }

  /**
   * Get Test OzoneConfiguration instance.
   * @return OzoneConfiguration
   * @throws IOException ioEx.
   */
  private OzoneConfiguration getTestOzoneConfiguration()
      throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OZONE_RECON_OM_SNAPSHOT_DB_DIR,
        temporaryFolder.newFolder().getAbsolutePath());
    configuration.set(OZONE_RECON_DB_DIR, temporaryFolder.newFolder()
        .getAbsolutePath());
    return configuration;
  }

}