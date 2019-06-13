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


package org.apache.hadoop.ozone.om.request.key;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ScmClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.util.Time;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Tests OMAllocateBlockRequest class.
 */
public class TestOMAllocateBlockRequest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;
  private AuditLogger auditLogger;

  private ScmClient scmClient;
  private OzoneBlockTokenSecretManager ozoneBlockTokenSecretManager;
  private ScmBlockLocationProtocol scmBlockLocationProtocol;

  private final long containerID = 1000L;
  private final long localID = 100L;

  private String volumeName;
  private String bucketName;
  private String keyName;
  private HddsProtos.ReplicationType replicationType;
  private HddsProtos.ReplicationFactor replicationFactor;
  private long clientID;



  @Before
  public void setup() throws Exception {
    ozoneManager = Mockito.mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    auditLogger = Mockito.mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    Mockito.doNothing().when(auditLogger).logWrite(any(AuditMessage.class));

    scmClient = Mockito.mock(ScmClient.class);
    ozoneBlockTokenSecretManager =
        Mockito.mock(OzoneBlockTokenSecretManager.class);
    scmBlockLocationProtocol = Mockito.mock(ScmBlockLocationProtocol.class);
    when(ozoneManager.getScmClient()).thenReturn(scmClient);
    when(ozoneManager.getBlockTokenSecretManager())
        .thenReturn(ozoneBlockTokenSecretManager);
    when(ozoneManager.getScmBlockSize()).thenReturn(1000L);
    when(ozoneManager.getPreallocateBlocksMax()).thenReturn(2);
    when(ozoneManager.isGrpcBlockTokenEnabled()).thenReturn(false);
    when(ozoneManager.getOMNodeId()).thenReturn(UUID.randomUUID().toString());
    when(scmClient.getBlockClient()).thenReturn(scmBlockLocationProtocol);

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setType(HddsProtos.ReplicationType.STAND_ALONE)
        .setFactor(HddsProtos.ReplicationFactor.ONE)
        .setNodes(new ArrayList<>())
        .build();

    AllocatedBlock allocatedBlock =
        new AllocatedBlock.Builder()
            .setContainerBlockID(new ContainerBlockID(containerID, localID))
        .setPipeline(pipeline).build();

    List<AllocatedBlock> allocatedBlocks = new ArrayList<>();

    allocatedBlocks.add(allocatedBlock);

    when(scmBlockLocationProtocol.allocateBlock(anyLong(), anyInt(),
        any(HddsProtos.ReplicationType.class),
        any(HddsProtos.ReplicationFactor.class),
        anyString(), any(ExcludeList.class))).thenReturn(allocatedBlocks);


    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    keyName = UUID.randomUUID().toString();
    replicationFactor = HddsProtos.ReplicationFactor.ONE;
    replicationType = HddsProtos.ReplicationType.RATIS;
    clientID = Time.now();

  }


  @Test
  public void testPreExecute() throws Exception {

    doPreExecute(createAllocateBlockRequest());

  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createAllocateBlockRequest());

    OMAllocateBlockRequest omAllocateBlockRequest =
        new OMAllocateBlockRequest(modifiedOmRequest);


    // Add volume, bucket, key entries to DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    TestOMRequestUtils.addKeyToOpenTable(volumeName, bucketName, keyName,
        clientID, replicationType, replicationFactor, omMetadataManager);

    // Check before calling validateAndUpdateCache. As adding DB entry has
    // not added any blocks, so size should be zero.

    OmKeyInfo omKeyInfo =
        omMetadataManager.getOpenKeyTable().get(omMetadataManager.getOpenKey(
            volumeName, bucketName, keyName, clientID));

    List<OmKeyLocationInfo> omKeyLocationInfo =
        omKeyInfo.getLatestVersionLocations().getLocationList();

    Assert.assertTrue(omKeyLocationInfo.size() == 0);

    OMClientResponse omAllocateBlockResponse =
        omAllocateBlockRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omAllocateBlockResponse.getOMResponse().getStatus());

    // Check open table whether new block is added or not.

    omKeyInfo =
        omMetadataManager.getOpenKeyTable().get(omMetadataManager.getOpenKey(
            volumeName, bucketName, keyName, clientID));

    omKeyLocationInfo =
        omKeyInfo.getLatestVersionLocations().getLocationList();

    Assert.assertTrue(omKeyLocationInfo.size() == 1);

    // Check modification time
    Assert.assertEquals(modifiedOmRequest.getAllocateBlockRequest()
        .getKeyArgs().getModificationTime(), omKeyInfo.getModificationTime());
    Assert.assertNotEquals(omKeyInfo.getCreationTime(),
        omKeyInfo.getModificationTime());

    // Check data of the block
    OzoneManagerProtocolProtos.KeyLocation keyLocation =
        modifiedOmRequest.getAllocateBlockRequest().getKeyLocation();

    Assert.assertEquals(keyLocation.getBlockID().getContainerBlockID()
        .getContainerID(), omKeyLocationInfo.get(0).getContainerID());

    Assert.assertEquals(keyLocation.getBlockID().getContainerBlockID()
            .getLocalID(), omKeyLocationInfo.get(0).getLocalID());

  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createAllocateBlockRequest());

    OMAllocateBlockRequest omAllocateBlockRequest =
        new OMAllocateBlockRequest(modifiedOmRequest);


    OMClientResponse omAllocateBlockResponse =
        omAllocateBlockRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assert.assertTrue(omAllocateBlockResponse.getOMResponse().getStatus()
        == OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND);

  }

  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createAllocateBlockRequest());

    OMAllocateBlockRequest omAllocateBlockRequest =
        new OMAllocateBlockRequest(modifiedOmRequest);


    // Added only volume to DB.
    TestOMRequestUtils.addVolumeToDB(volumeName, "ozone", omMetadataManager);

    OMClientResponse omAllocateBlockResponse =
        omAllocateBlockRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assert.assertTrue(omAllocateBlockResponse.getOMResponse().getStatus()
        == OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND);

  }

  @Test
  public void testValidateAndUpdateCacheWithKeyNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createAllocateBlockRequest());

    OMAllocateBlockRequest omAllocateBlockRequest =
        new OMAllocateBlockRequest(modifiedOmRequest);

    // Add volume, bucket entries to DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);


    OMClientResponse omAllocateBlockResponse =
        omAllocateBlockRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assert.assertTrue(omAllocateBlockResponse.getOMResponse().getStatus()
        == OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND);

  }

  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOMRequest
   * @return OMRequest - modified request returned from preExecute.
   * @throws Exception
   */
  private OMRequest doPreExecute(OMRequest originalOMRequest) throws Exception {

    OMAllocateBlockRequest omAllocateBlockRequest =
        new OMAllocateBlockRequest(originalOMRequest);

    OMRequest modifiedOmRequest =
        omAllocateBlockRequest.preExecute(ozoneManager);


    Assert.assertEquals(originalOMRequest.getCmdType(),
        modifiedOmRequest.getCmdType());
    Assert.assertEquals(originalOMRequest.getClientId(),
        modifiedOmRequest.getClientId());

    Assert.assertTrue(modifiedOmRequest.hasAllocateBlockRequest());
    AllocateBlockRequest allocateBlockRequest =
        modifiedOmRequest.getAllocateBlockRequest();
    // Time should be set
    Assert.assertTrue(allocateBlockRequest.getKeyArgs()
        .getModificationTime() > 0);

    // KeyLocation should be set.
    Assert.assertTrue(allocateBlockRequest.hasKeyLocation());
    Assert.assertEquals(containerID,
        allocateBlockRequest.getKeyLocation().getBlockID()
            .getContainerBlockID().getContainerID());
    Assert.assertEquals(localID,
        allocateBlockRequest.getKeyLocation().getBlockID()
            .getContainerBlockID().getLocalID());
    Assert.assertTrue(allocateBlockRequest.getKeyLocation().hasPipeline());

    Assert.assertEquals(allocateBlockRequest.getClientID(),
        allocateBlockRequest.getClientID());

    return modifiedOmRequest;
  }


  private OMRequest createAllocateBlockRequest() {

    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName)
        .setFactor(replicationFactor).setType(replicationType)
        .build();

    AllocateBlockRequest allocateBlockRequest =
        AllocateBlockRequest.newBuilder().setClientID(clientID)
            .setKeyArgs(keyArgs).build();

    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.AllocateBlock)
        .setClientId(UUID.randomUUID().toString())
        .setAllocateBlockRequest(allocateBlockRequest).build();

  }
}
