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

package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.om.BucketManager;
import org.apache.hadoop.ozone.om.BucketManagerImpl;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOmMetadataManagerImpl;
import org.apache.hadoop.utils.db.DBCheckpoint;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/**
 * Utility methods for test classes.
 */
public abstract class AbstractOMMetadataManagerTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  /**
   * Create a new OM Metadata manager instance.
   * @throws IOException ioEx
   */
  protected OMMetadataManager initializeNewOmMetadataManager()
      throws IOException {
    File omDbDir = temporaryFolder.newFolder();
    OzoneConfiguration omConfiguration = new OzoneConfiguration();
    omConfiguration.set(OZONE_OM_DB_DIRS,
        omDbDir.getAbsolutePath());
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(
        omConfiguration);

    String volumeKey = omMetadataManager.getVolumeKey("sampleVol");
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setVolume("sampleVol")
            .setAdminName("TestUser")
            .setOwnerName("TestUser")
            .build();
    omMetadataManager.getVolumeTable().put(volumeKey, args);

    BucketManager bucketManager = new BucketManagerImpl(omMetadataManager);
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .build();
    bucketManager.createBucket(bucketInfo);

    return omMetadataManager;
  }

  /**
   * Get an instance of Recon OM Metadata manager.
   * @return ReconOMMetadataManager
   * @throws IOException when creating the RocksDB instance.
   */
  protected ReconOMMetadataManager getTestMetadataManager(
      OMMetadataManager omMetadataManager)
      throws IOException {

    DBCheckpoint checkpoint = omMetadataManager.getStore()
        .getCheckpoint(true);
    assertNotNull(checkpoint.getCheckpointLocation());

    File reconOmDbDir = temporaryFolder.newFolder();
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OZONE_RECON_OM_SNAPSHOT_DB_DIR, reconOmDbDir
        .getAbsolutePath());

    ReconOMMetadataManager reconOMMetaMgr =
        new ReconOmMetadataManagerImpl(configuration);
    reconOMMetaMgr.start(configuration);

    reconOMMetaMgr.updateOmDB(
        checkpoint.getCheckpointLocation().toFile());
    return reconOMMetaMgr;
  }

  /**
   * Write a key to OM instance.
   * @throws IOException while writing.
   */
  public void writeDataToOm(OMMetadataManager omMetadataManager,
                                   String key) throws IOException {

    String omKey = omMetadataManager.getOzoneKey("sampleVol",
        "bucketOne", key);

    omMetadataManager.getKeyTable().put(omKey,
        new OmKeyInfo.Builder()
            .setBucketName("bucketOne")
            .setVolumeName("sampleVol")
            .setKeyName(key)
            .setReplicationFactor(HddsProtos.ReplicationFactor.ONE)
            .setReplicationType(HddsProtos.ReplicationType.STAND_ALONE)
            .build());
  }

  /**
   * Write a key to OM instance.
   * @throws IOException while writing.
   */
  protected void writeDataToOm(OMMetadataManager omMetadataManager,
                               String key,
                               String bucket,
                               String volume,
                               List<OmKeyLocationInfoGroup>
                                   omKeyLocationInfoGroupList)
      throws IOException {

    String omKey = omMetadataManager.getOzoneKey(volume,
        bucket, key);

    omMetadataManager.getKeyTable().put(omKey,
        new OmKeyInfo.Builder()
            .setBucketName(bucket)
            .setVolumeName(volume)
            .setKeyName(key)
            .setReplicationFactor(HddsProtos.ReplicationFactor.ONE)
            .setReplicationType(HddsProtos.ReplicationType.STAND_ALONE)
            .setOmKeyLocationInfos(omKeyLocationInfoGroupList)
            .build());
  }

  /**
   * Return random pipeline.
   * @return pipeline
   */
  protected Pipeline getRandomPipeline() {
    return Pipeline.newBuilder()
        .setFactor(HddsProtos.ReplicationFactor.ONE)
        .setId(PipelineID.randomId())
        .setNodes(Collections.EMPTY_LIST)
        .setState(Pipeline.PipelineState.OPEN)
        .setType(HddsProtos.ReplicationType.STAND_ALONE)
        .build();
  }

  /**
   * Get new OmKeyLocationInfo for given BlockID and Pipeline.
   * @param blockID blockId
   * @param pipeline pipeline
   * @return new instance of OmKeyLocationInfo
   */
  protected OmKeyLocationInfo getOmKeyLocationInfo(BlockID blockID,
                                                   Pipeline pipeline) {
    return new OmKeyLocationInfo.Builder()
        .setBlockID(blockID)
        .setPipeline(pipeline)
        .build();
  }
}
