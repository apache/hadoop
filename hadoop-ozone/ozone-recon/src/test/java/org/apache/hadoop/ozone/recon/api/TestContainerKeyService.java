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

package org.apache.hadoop.ozone.recon.api;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.AbstractOMMetadataManagerTest;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.ContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.KeyMetadata;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.ContainerDBServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.ReconContainerDBProvider;
import org.apache.hadoop.ozone.recon.tasks.ContainerKeyMapperTask;
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
 * Test for container key service.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*"})
@PrepareForTest(ReconUtils.class)
public class TestContainerKeyService extends AbstractOMMetadataManagerTest {

  private ContainerDBServiceProvider containerDbServiceProvider;
  private OMMetadataManager omMetadataManager;
  private ReconOMMetadataManager reconOMMetadataManager;
  private Injector injector;
  private OzoneManagerServiceProviderImpl ozoneManagerServiceProvider;
  private ContainerKeyService containerKeyService;

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
          bind(DBStore.class).toProvider(ReconContainerDBProvider.class).
              in(Singleton.class);
          bind(ContainerDBServiceProvider.class).to(
              ContainerDBServiceProviderImpl.class).in(Singleton.class);
          ozoneManagerServiceProvider = new OzoneManagerServiceProviderImpl(
              getTestOzoneConfiguration());
          bind(OzoneManagerServiceProvider.class)
              .toInstance(ozoneManagerServiceProvider);
          containerKeyService = new ContainerKeyService();
          bind(ContainerKeyService.class).toInstance(containerKeyService);
        } catch (IOException e) {
          Assert.fail();
        }
      }
    });
    containerDbServiceProvider = injector.getInstance(
        ContainerDBServiceProvider.class);

    //Write Data to OM
    Pipeline pipeline = getRandomPipeline();

    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    BlockID blockID1 = new BlockID(1, 101);
    OmKeyLocationInfo omKeyLocationInfo1 = getOmKeyLocationInfo(blockID1,
        pipeline);
    omKeyLocationInfoList.add(omKeyLocationInfo1);

    BlockID blockID2 = new BlockID(2, 102);
    OmKeyLocationInfo omKeyLocationInfo2 = getOmKeyLocationInfo(blockID2,
        pipeline);
    omKeyLocationInfoList.add(omKeyLocationInfo2);

    OmKeyLocationInfoGroup omKeyLocationInfoGroup = new
        OmKeyLocationInfoGroup(0, omKeyLocationInfoList);

    //key = key_one, Blocks = [ {CID = 1, LID = 1}, {CID = 2, LID = 1} ]
    writeDataToOm(omMetadataManager,
        "key_one", "bucketOne", "sampleVol",
        Collections.singletonList(omKeyLocationInfoGroup));

    List<OmKeyLocationInfoGroup> infoGroups = new ArrayList<>();
    BlockID blockID3 = new BlockID(1, 103);
    OmKeyLocationInfo omKeyLocationInfo3 = getOmKeyLocationInfo(blockID3,
        pipeline);

    List<OmKeyLocationInfo> omKeyLocationInfoListNew = new ArrayList<>();
    omKeyLocationInfoListNew.add(omKeyLocationInfo3);
    infoGroups.add(new OmKeyLocationInfoGroup(0,
        omKeyLocationInfoListNew));

    BlockID blockID4 = new BlockID(1, 104);
    OmKeyLocationInfo omKeyLocationInfo4 = getOmKeyLocationInfo(blockID4,
        pipeline);

    omKeyLocationInfoListNew = new ArrayList<>();
    omKeyLocationInfoListNew.add(omKeyLocationInfo4);
    infoGroups.add(new OmKeyLocationInfoGroup(1,
        omKeyLocationInfoListNew));

    //key = key_two, Blocks = [ {CID = 1, LID = 2}, {CID = 1, LID = 3} ]
    writeDataToOm(omMetadataManager,
        "key_two", "bucketOne", "sampleVol", infoGroups);

    List<OmKeyLocationInfo> omKeyLocationInfoList2 = new ArrayList<>();
    BlockID blockID5 = new BlockID(2, 2);
    OmKeyLocationInfo omKeyLocationInfo5 = getOmKeyLocationInfo(blockID5,
        pipeline);
    omKeyLocationInfoList2.add(omKeyLocationInfo5);

    BlockID blockID6 = new BlockID(2, 3);
    OmKeyLocationInfo omKeyLocationInfo6 = getOmKeyLocationInfo(blockID6,
        pipeline);
    omKeyLocationInfoList2.add(omKeyLocationInfo6);

    OmKeyLocationInfoGroup omKeyLocationInfoGroup2 = new
        OmKeyLocationInfoGroup(0, omKeyLocationInfoList2);

    //key = key_three, Blocks = [ {CID = 2, LID = 2}, {CID = 2, LID = 3} ]
    writeDataToOm(omMetadataManager,
        "key_three", "bucketOne", "sampleVol",
        Collections.singletonList(omKeyLocationInfoGroup2));

    //Take snapshot of OM DB and copy over to Recon OM DB.
    DBCheckpoint checkpoint = omMetadataManager.getStore()
        .getCheckpoint(true);
    File tarFile = OmUtils.createTarFile(checkpoint.getCheckpointLocation());
    InputStream inputStream = new FileInputStream(tarFile);
    PowerMockito.stub(PowerMockito.method(ReconUtils.class,
        "makeHttpCall",
        CloseableHttpClient.class, String.class))
        .toReturn(inputStream);

    //Generate Recon container DB data.
    ContainerKeyMapperTask containerKeyMapperTask = new ContainerKeyMapperTask(
        ozoneManagerServiceProvider, containerDbServiceProvider);
    containerKeyMapperTask.run();
  }

  @Test
  public void testGetKeysForContainer() {

    Response response = containerKeyService.getKeysForContainer(1L);

    Collection<KeyMetadata> keyMetadataList =
        (Collection<KeyMetadata>) response.getEntity();
    assertTrue(keyMetadataList.size() == 2);

    Iterator<KeyMetadata> iterator = keyMetadataList.iterator();

    KeyMetadata keyMetadata = iterator.next();
    assertTrue(keyMetadata.getKey().equals("key_one"));
    assertTrue(keyMetadata.getVersions().size() == 1);
    assertTrue(keyMetadata.getBlockIds().size() == 1);
    Map<Long, List<KeyMetadata.ContainerBlockMetadata>> blockIds =
        keyMetadata.getBlockIds();
    assertTrue(blockIds.get(0L).iterator().next().getLocalID() == 101);

    keyMetadata = iterator.next();
    assertTrue(keyMetadata.getKey().equals("key_two"));
    assertTrue(keyMetadata.getVersions().size() == 2);
    assertTrue(keyMetadata.getVersions().contains(0L) && keyMetadata
        .getVersions().contains(1L));
    assertTrue(keyMetadata.getBlockIds().size() == 2);
    blockIds = keyMetadata.getBlockIds();
    assertTrue(blockIds.get(0L).iterator().next().getLocalID() == 103);
    assertTrue(blockIds.get(1L).iterator().next().getLocalID() == 104);

    response = containerKeyService.getKeysForContainer(3L);
    keyMetadataList = (Collection<KeyMetadata>) response.getEntity();
    assertTrue(keyMetadataList.isEmpty());
  }

  @Test
  public void testGetContainers() {

    Response response = containerKeyService.getContainers();

    List<ContainerMetadata> containers = new ArrayList<>(
        (Collection<ContainerMetadata>) response.getEntity());

    assertTrue(containers.size() == 2);

    Iterator<ContainerMetadata> iterator = containers.iterator();

    ContainerMetadata containerMetadata = iterator.next();
    assertTrue(containerMetadata.getContainerID() == 1L);
    assertTrue(containerMetadata.getNumberOfKeys() == 3L);

    containerMetadata = iterator.next();
    assertTrue(containerMetadata.getContainerID() == 2L);
    assertTrue(containerMetadata.getNumberOfKeys() == 2L);
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