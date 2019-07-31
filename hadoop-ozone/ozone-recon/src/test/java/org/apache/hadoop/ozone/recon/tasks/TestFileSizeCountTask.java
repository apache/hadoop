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

import com.google.inject.Injector;
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
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.utils.db.Table;
import org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.FileCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.pojos.FileCountBySize;
import org.jooq.Configuration;
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

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for Container Key mapper task.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*"})
@PrepareForTest(ReconUtils.class)
public class TestFileSizeCountTask extends AbstractOMMetadataManagerTest {
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
  private Configuration sqlConfiguration;
  private int maxBinSize = 41;
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

      UtilizationSchemaDefinition utilizationSchemaDefinition =
          getInjector().getInstance(UtilizationSchemaDefinition.class);
      utilizationSchemaDefinition.initializeSchema();

      sqlConfiguration = getInjector().getInstance(Configuration.class);
      setUpIsDone = true;
    }
  }

  @Test
  public void testFileCountBySizeReprocess() throws IOException {
    Table<String, OmKeyInfo> omKeyInfoTable = omMetadataManager.getKeyTable();
    assertTrue(omKeyInfoTable.isEmpty());

    Pipeline pipeline = getRandomPipeline();
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    BlockID blockID1 = new BlockID(1, 1);
    OmKeyLocationInfo omKeyLocationInfo1 =
        getOmKeyLocationInfo(blockID1, pipeline);

    BlockID blockID2 = new BlockID(2, 1);
    OmKeyLocationInfo omKeyLocationInfo2 =
        getOmKeyLocationInfo(blockID2, pipeline);

    omKeyLocationInfoList.add(omKeyLocationInfo1);
    omKeyLocationInfoList.add(omKeyLocationInfo2);

    OmKeyLocationInfoGroup omKeyLocationInfoGroup =
        new OmKeyLocationInfoGroup(0, omKeyLocationInfoList);

    writeDataToOm(omMetadataManager,
        "key_1",
        "bucket_1",
        "sampleVol_1",
            1048576L,
        Collections.singletonList(omKeyLocationInfoGroup));

    writeDataToOm(omMetadataManager,
            "key_2",
            "bucket_2",
            "sampleVol_2",
        1048575L,
            Collections.singletonList(omKeyLocationInfoGroup));

    writeDataToOm(omMetadataManager,
            "key_3",
            "bucket_3",
            "sampleVol_3",
            1023L,
            Collections.singletonList(omKeyLocationInfoGroup));

    writeDataToOm(omMetadataManager,
            "key_4",
            "bucket_4",
            "sampleVol_4",
            1024L,
            Collections.singletonList(omKeyLocationInfoGroup));

    writeDataToOm(omMetadataManager,
        "key_5",
        "bucket_5",
        "sampleVol_5",
        1048577L,
        Collections.singletonList(omKeyLocationInfoGroup));

    writeDataToOm(omMetadataManager,
        "key_6",
        "bucket_6",
        "sampleVol_6",
        1125899906842623L,
        Collections.singletonList(omKeyLocationInfoGroup));

    writeDataToOm(omMetadataManager,
        "key_7",
        "bucket_7",
        "sampleVol_7",
        562949953421313L,
        Collections.singletonList(omKeyLocationInfoGroup));

    writeDataToOm(omMetadataManager,
        "key_8",
        "bucket_8",
        "sampleVol_8",
        562949953421311L,
        Collections.singletonList(omKeyLocationInfoGroup));

    FileSizeCountTask fileSizeCountTask =
        new FileSizeCountTask(omMetadataManager, sqlConfiguration);
    fileSizeCountTask.reprocess(omMetadataManager);

    omKeyInfoTable = omMetadataManager.getKeyTable();
    assertFalse(omKeyInfoTable.isEmpty());

    FileCountBySizeDao fileCountBySizeDao =
        new FileCountBySizeDao(sqlConfiguration);

    List<FileCountBySize> resultSet = fileCountBySizeDao.findAll();
    assertEquals(maxBinSize, resultSet.size());

    FileCountBySize dbRecord = fileCountBySizeDao.findById(1024L); // 1KB
    assertEquals(Long.valueOf(1), dbRecord.getCount());

    dbRecord = fileCountBySizeDao.findById(2048L); // 2KB
    assertEquals(Long.valueOf(1), dbRecord.getCount());

    dbRecord = fileCountBySizeDao.findById(1048576L); // 1MB
    assertEquals(Long.valueOf(1), dbRecord.getCount());

    dbRecord = fileCountBySizeDao.findById(2097152L); // 2MB
    assertEquals(Long.valueOf(2), dbRecord.getCount());

    dbRecord = fileCountBySizeDao.findById(562949953421312L); // 512TB
    assertEquals(Long.valueOf(1), dbRecord.getCount());

    dbRecord = fileCountBySizeDao.findById(1125899906842624L); // 1PB
    assertEquals(Long.valueOf(2), dbRecord.getCount());
  }

  @Test
  public void testFileCountBySizeProcess() throws IOException {
    Table<String, OmKeyInfo> omKeyInfoTable = omMetadataManager.getKeyTable();
    assertTrue(omKeyInfoTable.isEmpty());

    Pipeline pipeline = getRandomPipeline();
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    BlockID blockID1 = new BlockID(1, 1);
    OmKeyLocationInfo omKeyLocationInfo1 =
            getOmKeyLocationInfo(blockID1, pipeline);

    BlockID blockID2 = new BlockID(2, 1);
    OmKeyLocationInfo omKeyLocationInfo2 =
            getOmKeyLocationInfo(blockID2, pipeline);

    omKeyLocationInfoList.add(omKeyLocationInfo1);
    omKeyLocationInfoList.add(omKeyLocationInfo2);

    OmKeyLocationInfoGroup omKeyLocationInfoGroup =
            new OmKeyLocationInfoGroup(0, omKeyLocationInfoList);

    String bucket = "bucketOne";
    String volume = "sampleOne";
    String key = "keyOne";
    long dataSize = 2049L;
    String omKey = omMetadataManager.getOzoneKey(volume, bucket, key);

    OmKeyInfo omKeyInfo = buildOmKeyInfo(volume, bucket, key, dataSize,
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

    String key2 = "keyTwo";
    writeDataToOm(omMetadataManager, key2, bucket, volume, 2048L,
            Collections.singletonList(omKeyLocationInfoGroup));

    omKey = omMetadataManager.getOzoneKey(volume, bucket, key2);
    omKeyInfo = buildOmKeyInfo(volume, bucket, key2, 2048L,
            omKeyLocationInfoGroup);

    OMDBUpdateEvent keyEvent2 =
        new OMDBUpdateEvent.OMUpdateEventBuilder<String, OmKeyInfo>()
            .setKey(omKey)
            .setValue(omKeyInfo)
            .setTable(omMetadataManager.getKeyTable().getName())
            .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
            .build();

    String dummyKey = omMetadataManager.getOzoneKey(volume, bucket, "dummyKey");
    omKeyInfo = buildOmKeyInfo(volume, bucket, "dummyKey", 1125899906842624L,
            omKeyLocationInfoGroup);

    OMDBUpdateEvent keyEvent3 =
        new OMDBUpdateEvent.OMUpdateEventBuilder<String, OmKeyInfo>()
            .setKey(dummyKey)
            .setValue(omKeyInfo)
            .setTable(omMetadataManager.getKeyTable().getName())
            .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
            .build();

    String key3 = omMetadataManager.getOzoneKey(volume, bucket, "dummyKey");
    omKeyInfo = buildOmKeyInfo(volume, bucket, "dummyKey", 1024L,
        omKeyLocationInfoGroup);

    OMDBUpdateEvent keyEvent4 =
        new OMDBUpdateEvent.OMUpdateEventBuilder<String, OmKeyInfo>()
        .setKey(key3)
        .setValue(omKeyInfo)
        .setTable(omMetadataManager.getKeyTable().getName())
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
        .build();

    OMUpdateEventBatch omUpdateEventBatch = new
            OMUpdateEventBatch(new ArrayList<OMDBUpdateEvent>() {{
                add(keyEvent1);
                add(keyEvent2);
                add(keyEvent4);
                add(keyEvent3);
                add(keyEvent1);
              }});

    FileSizeCountTask fileSizeCountTask =
            new FileSizeCountTask(omMetadataManager, sqlConfiguration);
    // call reprocess()
    fileSizeCountTask.reprocess(omMetadataManager);

    omKeyInfoTable = omMetadataManager.getKeyTable();
    assertFalse(omKeyInfoTable.isEmpty());

    FileCountBySizeDao fileCountBySizeDao =
        new FileCountBySizeDao(sqlConfiguration);

    FileCountBySize dbRecord = fileCountBySizeDao.findById(4096L);
    assertEquals(Long.valueOf(1), dbRecord.getCount());

    // call process()
    fileSizeCountTask.process(omUpdateEventBatch);

    dbRecord = fileCountBySizeDao.findById(4096L);

    //test halts after keyEvent 3. No count update for keyEvent1 in the end.
    assertEquals(Long.valueOf(1), dbRecord.getCount());

    dbRecord = fileCountBySizeDao.findById(2048L);
    assertEquals(Long.valueOf(1), dbRecord.getCount());
  }

  private OmKeyInfo buildOmKeyInfo(String volume,
                                   String bucket,
                                   String key,
                                   Long dataSize,
                                   OmKeyLocationInfoGroup
                                           omKeyLocationInfoGroup) {
    return new OmKeyInfo.Builder()
            .setBucketName(bucket)
            .setVolumeName(volume)
            .setKeyName(key)
            .setDataSize(dataSize)
            .setReplicationFactor(HddsProtos.ReplicationFactor.ONE)
            .setReplicationType(HddsProtos.ReplicationType.STAND_ALONE)
            .setOmKeyLocationInfos(Collections.singletonList(
                    omKeyLocationInfoGroup))
            .build();
  }
}
