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

import org.apache.hadoop.hdds.client.BlockID;
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
    .CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Tests OMCreateKeyRequest class.
 */
public class TestOMKeyCreateRequest {

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
  private final long scmBlockSize = 1000L;

  private String volumeName;
  private String bucketName;
  private String keyName;
  private HddsProtos.ReplicationType replicationType;
  private HddsProtos.ReplicationFactor replicationFactor;
  private long dataSize;


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
    when(ozoneManager.getScmBlockSize()).thenReturn(scmBlockSize);
    when(ozoneManager.getPreallocateBlocksMax()).thenReturn(2);
    when(ozoneManager.isGrpcBlockTokenEnabled()).thenReturn(false);
    when(ozoneManager.getOMNodeId()).thenReturn(UUID.randomUUID().toString());
    when(scmClient.getBlockClient()).thenReturn(scmBlockLocationProtocol);

    List<OmKeyLocationInfo> omKeyLocationInfos = new ArrayList<>();

    OmKeyLocationInfo omKeyLocationInfo =
        new OmKeyLocationInfo.Builder().setOffset(0).setLength(0)
            .setBlockID(new BlockID(containerID, localID))
            .setPipeline(Mockito.mock(Pipeline.class)).build();

    omKeyLocationInfos.add(omKeyLocationInfo);

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
    dataSize = 1000L;

  }


  @Test
  public void testPreExecuteWithNormalKey() throws Exception {

    doPreExecute(createKeyRequest(false, 0));

  }

  @Test
  public void testPreExecuteWithMultipartKey() throws Exception {

    doPreExecute(createKeyRequest(true, 1));

  }


  @Test
  public void testValidateAndUpdateCache() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createKeyRequest(false, 0));

    OMKeyCreateRequest omKeyCreateRequest =
        new OMKeyCreateRequest(modifiedOmRequest);

    // Add volume and bucket entries to DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    long id = modifiedOmRequest.getCreateKeyRequest().getID();

    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, id);

    // Before calling
    OmKeyInfo omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omKeyCreateResponse.getOMResponse().getStatus());

    // Check open table whether key is added or not.

    omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

    Assert.assertNotNull(omKeyInfo);

    List<OmKeyLocationInfo> omKeyLocationInfoList =
        omKeyInfo.getLatestVersionLocations().getLocationList();
    Assert.assertTrue(omKeyLocationInfoList.size() == 1);

    OmKeyLocationInfo omKeyLocationInfo = omKeyLocationInfoList.get(0);

    // Check modification time
    Assert.assertEquals(modifiedOmRequest.getCreateKeyRequest()
        .getKeyArgs().getModificationTime(), omKeyInfo.getModificationTime());

    Assert.assertEquals(omKeyInfo.getModificationTime(),
        omKeyInfo.getCreationTime());


    // Check data of the block
    OzoneManagerProtocolProtos.KeyLocation keyLocation =
        modifiedOmRequest.getCreateKeyRequest().getKeyArgs().getKeyLocations(0);

    Assert.assertEquals(keyLocation.getBlockID().getContainerBlockID()
        .getContainerID(), omKeyLocationInfo.getContainerID());
    Assert.assertEquals(keyLocation.getBlockID().getContainerBlockID()
        .getLocalID(), omKeyLocationInfo.getLocalID());

  }

  @Test
  public void testValidateAndUpdateCacheWithNoSuchMultipartUploadError()
      throws Exception {


    int partNumber = 1;
    OMRequest modifiedOmRequest =
        doPreExecute(createKeyRequest(true, partNumber));

    OMKeyCreateRequest omKeyCreateRequest =
        new OMKeyCreateRequest(modifiedOmRequest);

    // Add volume and bucket entries to DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    long id = modifiedOmRequest.getCreateKeyRequest().getID();

    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, id);

    // Before calling
    OmKeyInfo omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assert.assertEquals(
        OzoneManagerProtocolProtos.Status.NO_SUCH_MULTIPART_UPLOAD_ERROR,
        omKeyCreateResponse.getOMResponse().getStatus());

    // As we got error, no entry should be created in openKeyTable.

    omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

    Assert.assertNull(omKeyInfo);
  }



  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createKeyRequest(false, 0));

    OMKeyCreateRequest omKeyCreateRequest =
        new OMKeyCreateRequest(modifiedOmRequest);


    long id = modifiedOmRequest.getCreateKeyRequest().getID();

    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, id);


    // Before calling
    OmKeyInfo omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omKeyCreateResponse.getOMResponse().getStatus());


    // As We got an error, openKey Table should not have entry.
    omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

    Assert.assertNull(omKeyInfo);

  }


  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {


    OMRequest modifiedOmRequest =
        doPreExecute(createKeyRequest(
            false, 0));

    OMKeyCreateRequest omKeyCreateRequest =
        new OMKeyCreateRequest(modifiedOmRequest);


    long id = modifiedOmRequest.getCreateKeyRequest().getID();

    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, id);

    TestOMRequestUtils.addVolumeToDB(volumeName, "ozone", omMetadataManager);

    // Before calling
    OmKeyInfo omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omKeyCreateResponse.getOMResponse().getStatus());


    // As We got an error, openKey Table should not have entry.
    omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

    Assert.assertNull(omKeyInfo);

  }



  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOMRequest
   * @return OMRequest - modified request returned from preExecute.
   * @throws Exception
   */
  private OMRequest doPreExecute(OMRequest originalOMRequest) throws Exception {

    OMKeyCreateRequest omKeyCreateRequest =
        new OMKeyCreateRequest(originalOMRequest);

    OMRequest modifiedOmRequest =
        omKeyCreateRequest.preExecute(ozoneManager);

    Assert.assertEquals(originalOMRequest.getCmdType(),
        modifiedOmRequest.getCmdType());
    Assert.assertEquals(originalOMRequest.getClientId(),
        modifiedOmRequest.getClientId());

    Assert.assertTrue(modifiedOmRequest.hasCreateKeyRequest());

    CreateKeyRequest createKeyRequest =
        modifiedOmRequest.getCreateKeyRequest();

    KeyArgs keyArgs = createKeyRequest.getKeyArgs();
    // Time should be set
    Assert.assertTrue(keyArgs.getModificationTime() > 0);


    // ID should be set.
    Assert.assertTrue(createKeyRequest.hasID());
    Assert.assertTrue(createKeyRequest.getID() > 0);


    if (!originalOMRequest.getCreateKeyRequest().getKeyArgs()
        .getIsMultipartKey()) {

      // As our data size is 100, and scmBlockSize is default to 1000, so we
      // shall have only one block.
      List< OzoneManagerProtocolProtos.KeyLocation> keyLocations =
          keyArgs.getKeyLocationsList();
      // KeyLocation should be set.
      Assert.assertTrue(keyLocations.size() == 1);
      Assert.assertEquals(containerID,
          keyLocations.get(0).getBlockID().getContainerBlockID()
              .getContainerID());
      Assert.assertEquals(localID,
          keyLocations.get(0).getBlockID().getContainerBlockID()
              .getLocalID());
      Assert.assertTrue(keyLocations.get(0).hasPipeline());

      Assert.assertEquals(0, keyLocations.get(0).getOffset());

      Assert.assertEquals(scmBlockSize, keyLocations.get(0).getLength());
    } else {
      // We don't create blocks for multipart key in createKey preExecute.
      Assert.assertTrue(keyArgs.getKeyLocationsList().size() == 0);
    }

    return modifiedOmRequest;

  }

  /**
   * Create OMRequest which encapsulates CreateKeyRequest.
   * @param isMultipartKey
   * @param partNumber
   * @return OMRequest.
   */

  @SuppressWarnings("parameterNumber")
  private OMRequest createKeyRequest(boolean isMultipartKey, int partNumber) {

    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName).setIsMultipartKey(isMultipartKey)
        .setFactor(replicationFactor).setType(replicationType);

    if (isMultipartKey) {
      keyArgs.setDataSize(dataSize).setMultipartNumber(partNumber);
    }

    OzoneManagerProtocolProtos.CreateKeyRequest createKeyRequest =
        CreateKeyRequest.newBuilder().setKeyArgs(keyArgs).build();

    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateKey)
        .setClientId(UUID.randomUUID().toString())
        .setCreateKeyRequest(createKeyRequest).build();

  }


}
