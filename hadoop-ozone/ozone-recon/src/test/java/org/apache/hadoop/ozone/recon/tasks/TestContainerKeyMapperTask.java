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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.AbstractOMMetadataManagerTest;
import org.apache.hadoop.ozone.recon.GuiceInjectorUtilsForTestsImpl;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.hadoop.ozone.recon.schema.StatsSchemaDefinition;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import com.google.inject.Injector;
import javax.sql.DataSource;

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
  private boolean setUpIsDone = false;
  private GuiceInjectorUtilsForTestsImpl guiceInjectorTest =
      new GuiceInjectorUtilsForTestsImpl();

  private Injector getInjector() {
    return injector;
  }

  @Rule
  TemporaryFolder temporaryFolder = new TemporaryFolder();

  private void initializeInjector() throws Exception {
    omMetadataManager = initializeNewOmMetadataManager();
    OzoneConfiguration configuration =
        guiceInjectorTest.getTestOzoneConfiguration(temporaryFolder);

    ozoneManagerServiceProvider = new OzoneManagerServiceProviderImpl(
        configuration);
    reconOMMetadataManager = getTestMetadataManager(omMetadataManager);

    injector = guiceInjectorTest.getInjector(
        ozoneManagerServiceProvider, reconOMMetadataManager, temporaryFolder);
  }

  @Before
  public void setUp() throws Exception {
    // The following setup is run only once
    if (!setUpIsDone) {
      initializeInjector();

      DSL.using(new DefaultConfiguration().set(
          injector.getInstance(DataSource.class)));

      containerDbServiceProvider = injector.getInstance(
          ContainerDBServiceProvider.class);

      StatsSchemaDefinition schemaDefinition = getInjector().getInstance(
          StatsSchemaDefinition.class);
      schemaDefinition.initializeSchema();

      setUpIsDone = true;
    }

    containerDbServiceProvider = injector.getInstance(
        ContainerDBServiceProvider.class);
  }

  @Test
  public void testReprocessOMDB() throws Exception{

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

    writeDataToOm(reconOMMetadataManager,
        "key_one",
        "bucketOne",
        "sampleVol",
        Collections.singletonList(omKeyLocationInfoGroup));

    ContainerKeyMapperTask containerKeyMapperTask =
        new ContainerKeyMapperTask(containerDbServiceProvider,
        ozoneManagerServiceProvider.getOMMetadataManagerInstance());
    containerKeyMapperTask.reprocess(ozoneManagerServiceProvider
        .getOMMetadataManagerInstance());

    keyPrefixesForContainer =
        containerDbServiceProvider.getKeyPrefixesForContainer(1);
    assertEquals(1, keyPrefixesForContainer.size());
    String omKey = omMetadataManager.getOzoneKey("sampleVol",
        "bucketOne", "key_one");
    ContainerKeyPrefix containerKeyPrefix = new ContainerKeyPrefix(1,
        omKey, 0);
    assertEquals(1,
        keyPrefixesForContainer.get(containerKeyPrefix).intValue());

    keyPrefixesForContainer =
        containerDbServiceProvider.getKeyPrefixesForContainer(2);
    assertEquals(1, keyPrefixesForContainer.size());
    containerKeyPrefix = new ContainerKeyPrefix(2, omKey,
        0);
    assertEquals(1,
        keyPrefixesForContainer.get(containerKeyPrefix).intValue());

    // Test if container key counts are updated
    assertEquals(1, containerDbServiceProvider.getKeyCountForContainer(1L));
    assertEquals(1, containerDbServiceProvider.getKeyCountForContainer(2L));
    assertEquals(0, containerDbServiceProvider.getKeyCountForContainer(3L));

    // Test if container count is updated
    assertEquals(2, containerDbServiceProvider.getCountForContainers());
  }

  @Test
  public void testProcessOMEvents() throws IOException {
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

    String bucket = "bucketOne";
    String volume = "sampleVol";
    String key = "key_one";
    String omKey = omMetadataManager.getOzoneKey(volume, bucket, key);
    OmKeyInfo omKeyInfo = buildOmKeyInfo(volume, bucket, key,
        omKeyLocationInfoGroup);

    OMDBUpdateEvent keyEvent1 = new OMDBUpdateEvent.
        OMUpdateEventBuilder<String, OmKeyInfo>()
        .setKey(omKey)
        .setValue(omKeyInfo)
        .setTable(omMetadataManager.getKeyTable().getName())
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
        .build();

    BlockID blockID3 = new BlockID(1, 2);
    OmKeyLocationInfo omKeyLocationInfo3 =
        getOmKeyLocationInfo(blockID3, pipeline);

    BlockID blockID4 = new BlockID(3, 1);
    OmKeyLocationInfo omKeyLocationInfo4
        = getOmKeyLocationInfo(blockID4, pipeline);

    omKeyLocationInfoList = new ArrayList<>();
    omKeyLocationInfoList.add(omKeyLocationInfo3);
    omKeyLocationInfoList.add(omKeyLocationInfo4);
    omKeyLocationInfoGroup = new OmKeyLocationInfoGroup(0,
        omKeyLocationInfoList);

    String key2 = "key_two";
    writeDataToOm(reconOMMetadataManager, key2, bucket, volume, Collections
        .singletonList(omKeyLocationInfoGroup));

    omKey = omMetadataManager.getOzoneKey(volume, bucket, key2);
    OMDBUpdateEvent keyEvent2 = new OMDBUpdateEvent.
        OMUpdateEventBuilder<String, OmKeyInfo>()
        .setKey(omKey)
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
        .setTable(omMetadataManager.getKeyTable().getName())
        .build();

    OMUpdateEventBatch omUpdateEventBatch = new OMUpdateEventBatch(new
        ArrayList<OMDBUpdateEvent>() {{
          add(keyEvent1);
          add(keyEvent2);
        }});

    ContainerKeyMapperTask containerKeyMapperTask =
        new ContainerKeyMapperTask(containerDbServiceProvider,
            ozoneManagerServiceProvider.getOMMetadataManagerInstance());
    containerKeyMapperTask.reprocess(ozoneManagerServiceProvider
        .getOMMetadataManagerInstance());

    keyPrefixesForContainer = containerDbServiceProvider
        .getKeyPrefixesForContainer(1);
    assertEquals(1, keyPrefixesForContainer.size());

    keyPrefixesForContainer = containerDbServiceProvider
        .getKeyPrefixesForContainer(2);
    assertTrue(keyPrefixesForContainer.isEmpty());

    keyPrefixesForContainer = containerDbServiceProvider
        .getKeyPrefixesForContainer(3);
    assertEquals(1, keyPrefixesForContainer.size());

    assertEquals(1, containerDbServiceProvider.getKeyCountForContainer(1L));
    assertEquals(0, containerDbServiceProvider.getKeyCountForContainer(2L));
    assertEquals(1, containerDbServiceProvider.getKeyCountForContainer(3L));

    // Process PUT & DELETE event.
    containerKeyMapperTask.process(omUpdateEventBatch);

    keyPrefixesForContainer = containerDbServiceProvider
        .getKeyPrefixesForContainer(1);
    assertEquals(1, keyPrefixesForContainer.size());

    keyPrefixesForContainer = containerDbServiceProvider
        .getKeyPrefixesForContainer(2);
    assertEquals(1, keyPrefixesForContainer.size());

    keyPrefixesForContainer = containerDbServiceProvider
        .getKeyPrefixesForContainer(3);
    assertTrue(keyPrefixesForContainer.isEmpty());

    assertEquals(1, containerDbServiceProvider.getKeyCountForContainer(1L));
    assertEquals(1, containerDbServiceProvider.getKeyCountForContainer(2L));
    assertEquals(0, containerDbServiceProvider.getKeyCountForContainer(3L));

    // Test if container count is updated
    assertEquals(3, containerDbServiceProvider.getCountForContainers());
  }

  private OmKeyInfo buildOmKeyInfo(String volume,
                                   String bucket,
                                   String key,
                                   OmKeyLocationInfoGroup
                                       omKeyLocationInfoGroup) {
    return new OmKeyInfo.Builder()
        .setBucketName(bucket)
        .setVolumeName(volume)
        .setKeyName(key)
        .setReplicationFactor(HddsProtos.ReplicationFactor.ONE)
        .setReplicationType(HddsProtos.ReplicationType.STAND_ALONE)
        .setOmKeyLocationInfos(Collections.singletonList(
            omKeyLocationInfoGroup))
        .build();
  }
}