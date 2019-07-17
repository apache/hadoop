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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.AbstractOMMetadataManagerTest;
import org.apache.hadoop.ozone.recon.GuiceInjectorUtilsForTestsImpl;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.ContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.ContainersResponse;
import org.apache.hadoop.ozone.recon.api.types.KeyMetadata;
import org.apache.hadoop.ozone.recon.api.types.KeysResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.ContainerKeyMapperTask;
import org.apache.hadoop.utils.db.DBCheckpoint;
import org.apache.http.impl.client.CloseableHttpClient;
import org.hadoop.ozone.recon.schema.StatsSchemaDefinition;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;

import org.junit.rules.TemporaryFolder;

/**
 * Test for container key service.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*"})
@PrepareForTest(ReconUtils.class)
public class TestContainerKeyService extends AbstractOMMetadataManagerTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private ContainerDBServiceProvider containerDbServiceProvider;
  private OMMetadataManager omMetadataManager;
  private Injector injector;
  private OzoneManagerServiceProviderImpl ozoneManagerServiceProvider;
  private ContainerKeyService containerKeyService;
  private GuiceInjectorUtilsForTestsImpl guiceInjectorTest =
      new GuiceInjectorUtilsForTestsImpl();
  private boolean isSetupDone = false;

  private void initializeInjector() throws Exception {
    omMetadataManager = initializeNewOmMetadataManager();
    OzoneConfiguration configuration =
        guiceInjectorTest.getTestOzoneConfiguration(temporaryFolder);

    ozoneManagerServiceProvider = new OzoneManagerServiceProviderImpl(
        configuration);
    ReconOMMetadataManager reconOMMetadataManager =
        getTestMetadataManager(omMetadataManager);

    Injector parentInjector = guiceInjectorTest.getInjector(
        ozoneManagerServiceProvider, reconOMMetadataManager, temporaryFolder);

    injector = parentInjector.createChildInjector(new AbstractModule() {
      @Override
      protected void configure() {
        containerKeyService = new ContainerKeyService();
        bind(ContainerKeyService.class).toInstance(containerKeyService);
      }
    });
  }

  @Before
  public void setUp() throws Exception {
    // The following setup runs only once
    if (!isSetupDone) {
      initializeInjector();

      DSL.using(new DefaultConfiguration().set(
          injector.getInstance(DataSource.class)));

      containerDbServiceProvider = injector.getInstance(
          ContainerDBServiceProvider.class);

      StatsSchemaDefinition schemaDefinition = injector.getInstance(
          StatsSchemaDefinition.class);
      schemaDefinition.initializeSchema();

      isSetupDone = true;
    }

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

    //key = key_one, Blocks = [ {CID = 1, LID = 101}, {CID = 2, LID = 102} ]
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

    //key = key_two, Blocks = [ {CID = 1, LID = 103}, {CID = 1, LID = 104} ]
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
        containerDbServiceProvider,
        ozoneManagerServiceProvider.getOMMetadataManagerInstance());
    ozoneManagerServiceProvider.updateReconOmDBWithNewSnapshot();
    containerKeyMapperTask.reprocess(ozoneManagerServiceProvider
        .getOMMetadataManagerInstance());
  }

  @Test
  public void testGetKeysForContainer() {

    Response response = containerKeyService.getKeysForContainer(1L, -1, "");

    KeysResponse responseObject = (KeysResponse) response.getEntity();
    KeysResponse.KeysResponseData data = responseObject.getKeysResponseData();
    Collection<KeyMetadata> keyMetadataList = data.getKeys();

    assertEquals(3, data.getTotalCount());
    assertEquals(2, keyMetadataList.size());

    Iterator<KeyMetadata> iterator = keyMetadataList.iterator();

    KeyMetadata keyMetadata = iterator.next();
    assertEquals("key_one", keyMetadata.getKey());
    assertEquals(1, keyMetadata.getVersions().size());
    assertEquals(1, keyMetadata.getBlockIds().size());
    Map<Long, List<KeyMetadata.ContainerBlockMetadata>> blockIds =
        keyMetadata.getBlockIds();
    assertEquals(101, blockIds.get(0L).iterator().next().getLocalID());

    keyMetadata = iterator.next();
    assertEquals("key_two", keyMetadata.getKey());
    assertEquals(2, keyMetadata.getVersions().size());
    assertTrue(keyMetadata.getVersions().contains(0L) && keyMetadata
        .getVersions().contains(1L));
    assertEquals(2, keyMetadata.getBlockIds().size());
    blockIds = keyMetadata.getBlockIds();
    assertEquals(103, blockIds.get(0L).iterator().next().getLocalID());
    assertEquals(104, blockIds.get(1L).iterator().next().getLocalID());

    response = containerKeyService.getKeysForContainer(3L, -1, "");
    responseObject = (KeysResponse) response.getEntity();
    data = responseObject.getKeysResponseData();
    keyMetadataList = data.getKeys();
    assertTrue(keyMetadataList.isEmpty());
    assertEquals(0, data.getTotalCount());

    // test if limit works as expected
    response = containerKeyService.getKeysForContainer(1L, 1, "");
    responseObject = (KeysResponse) response.getEntity();
    data = responseObject.getKeysResponseData();
    keyMetadataList = data.getKeys();
    assertEquals(1, keyMetadataList.size());
    assertEquals(3, data.getTotalCount());
  }

  @Test
  public void testGetKeysForContainerWithPrevKey() {
    // test if prev-key param works as expected
    Response response = containerKeyService.getKeysForContainer(
        1L, -1, "/sampleVol/bucketOne/key_one");

    KeysResponse responseObject =
        (KeysResponse) response.getEntity();

    KeysResponse.KeysResponseData data =
        responseObject.getKeysResponseData();
    assertEquals(3, data.getTotalCount());

    Collection<KeyMetadata> keyMetadataList = data.getKeys();
    assertEquals(1, keyMetadataList.size());

    Iterator<KeyMetadata> iterator = keyMetadataList.iterator();
    KeyMetadata keyMetadata = iterator.next();

    assertEquals("key_two", keyMetadata.getKey());
    assertEquals(2, keyMetadata.getVersions().size());
    assertEquals(2, keyMetadata.getBlockIds().size());

    response = containerKeyService.getKeysForContainer(
        1L, -1, StringUtils.EMPTY);
    responseObject = (KeysResponse) response.getEntity();
    data = responseObject.getKeysResponseData();
    keyMetadataList = data.getKeys();

    assertEquals(3, data.getTotalCount());
    assertEquals(2, keyMetadataList.size());
    iterator = keyMetadataList.iterator();
    keyMetadata = iterator.next();
    assertEquals("key_one", keyMetadata.getKey());

    // test for negative cases
    response = containerKeyService.getKeysForContainer(
        1L, -1, "/sampleVol/bucketOne/invalid_key");
    responseObject = (KeysResponse) response.getEntity();
    data = responseObject.getKeysResponseData();
    keyMetadataList = data.getKeys();
    assertEquals(3, data.getTotalCount());
    assertEquals(0, keyMetadataList.size());

    response = containerKeyService.getKeysForContainer(
        5L, -1, "");
    responseObject = (KeysResponse) response.getEntity();
    data = responseObject.getKeysResponseData();
    keyMetadataList = data.getKeys();
    assertEquals(0, keyMetadataList.size());
    assertEquals(0, data.getTotalCount());
  }

  @Test
  public void testGetContainers() {

    Response response = containerKeyService.getContainers(-1, 0L);

    ContainersResponse responseObject =
        (ContainersResponse) response.getEntity();

    ContainersResponse.ContainersResponseData data =
        responseObject.getContainersResponseData();
    assertEquals(2, data.getTotalCount());

    List<ContainerMetadata> containers = new ArrayList<>(data.getContainers());

    Iterator<ContainerMetadata> iterator = containers.iterator();

    ContainerMetadata containerMetadata = iterator.next();
    assertEquals(1L, containerMetadata.getContainerID());
    // Number of keys for CID:1 should be 3 because of two different versions
    // of key_two stored in CID:1
    assertEquals(3L, containerMetadata.getNumberOfKeys());

    containerMetadata = iterator.next();
    assertEquals(2L, containerMetadata.getContainerID());
    assertEquals(2L, containerMetadata.getNumberOfKeys());

    // test if limit works as expected
    response = containerKeyService.getContainers(1, 0L);
    responseObject = (ContainersResponse) response.getEntity();
    data = responseObject.getContainersResponseData();
    containers = new ArrayList<>(data.getContainers());
    assertEquals(1, containers.size());
    assertEquals(2, data.getTotalCount());
  }

  @Test
  public void testGetContainersWithPrevKey() {

    Response response = containerKeyService.getContainers(1, 1L);

    ContainersResponse responseObject =
        (ContainersResponse) response.getEntity();

    ContainersResponse.ContainersResponseData data =
        responseObject.getContainersResponseData();
    assertEquals(2, data.getTotalCount());

    List<ContainerMetadata> containers = new ArrayList<>(data.getContainers());

    Iterator<ContainerMetadata> iterator = containers.iterator();

    ContainerMetadata containerMetadata = iterator.next();

    assertEquals(1, containers.size());
    assertEquals(2L, containerMetadata.getContainerID());

    response = containerKeyService.getContainers(-1, 0L);
    responseObject = (ContainersResponse) response.getEntity();
    data = responseObject.getContainersResponseData();
    containers = new ArrayList<>(data.getContainers());
    assertEquals(2, containers.size());
    assertEquals(2, data.getTotalCount());
    iterator = containers.iterator();
    containerMetadata = iterator.next();
    assertEquals(1L, containerMetadata.getContainerID());

    // test for negative cases
    response = containerKeyService.getContainers(-1, 5L);
    responseObject = (ContainersResponse) response.getEntity();
    data = responseObject.getContainersResponseData();
    containers = new ArrayList<>(data.getContainers());
    assertEquals(0, containers.size());
    assertEquals(2, data.getTotalCount());

    response = containerKeyService.getContainers(-1, -1L);
    responseObject = (ContainersResponse) response.getEntity();
    data = responseObject.getContainersResponseData();
    containers = new ArrayList<>(data.getContainers());
    assertEquals(2, containers.size());
    assertEquals(2, data.getTotalCount());
  }
}