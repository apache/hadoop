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

package org.apache.hadoop.ozone.om.request.file;

import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.request.key.TestOMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.FILE_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.NOT_A_FILE;

/**
 * Tests OMFileCreateRequest.
 */
public class TestOMFileCreateRequest extends TestOMKeyRequest {


  @Test
  public void testPreExecute() throws Exception{
    OMRequest omRequest = createFileRequest(volumeName, bucketName, keyName,
        HddsProtos.ReplicationFactor.ONE, HddsProtos.ReplicationType.RATIS,
        false, false);

    OMFileCreateRequest omFileCreateRequest =
        new OMFileCreateRequest(omRequest);

    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);
    Assert.assertNotEquals(omRequest, modifiedOmRequest);


    // Check clientID and modification time is set or not.
    Assert.assertTrue(modifiedOmRequest.hasCreateFileRequest());
    Assert.assertTrue(
        modifiedOmRequest.getCreateFileRequest().getClientID() > 0);

    KeyArgs keyArgs = modifiedOmRequest.getCreateFileRequest().getKeyArgs();
    Assert.assertNotNull(keyArgs);
    Assert.assertTrue(keyArgs.getModificationTime() > 0);

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
  }

  @Test
  public void testPreExecuteWithBlankKey() throws Exception{
    OMRequest omRequest = createFileRequest(volumeName, bucketName, "",
        HddsProtos.ReplicationFactor.ONE, HddsProtos.ReplicationType.RATIS,
        false, false);

    OMFileCreateRequest omFileCreateRequest = new OMFileCreateRequest(
        omRequest);

    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);
    Assert.assertNotEquals(omRequest, modifiedOmRequest);


    // When KeyName is root, nothing will be set.
    Assert.assertTrue(modifiedOmRequest.hasCreateFileRequest());
    Assert.assertFalse(
        modifiedOmRequest.getCreateFileRequest().getClientID() > 0);

    KeyArgs keyArgs = modifiedOmRequest.getCreateFileRequest().getKeyArgs();
    Assert.assertNotNull(keyArgs);
    Assert.assertTrue(keyArgs.getModificationTime() == 0);
    Assert.assertTrue(keyArgs.getKeyLocationsList().size() == 0);
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    OMRequest omRequest = createFileRequest(volumeName, bucketName, keyName,
        HddsProtos.ReplicationFactor.ONE, HddsProtos.ReplicationType.RATIS,
        false, true);

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    OMFileCreateRequest omFileCreateRequest = new OMFileCreateRequest(
        omRequest);

    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);


    long id = modifiedOmRequest.getCreateFileRequest().getClientID();

    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, id);

    // Before calling
    OmKeyInfo omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);
    Assert.assertNull(omKeyInfo);

    omFileCreateRequest = new OMFileCreateRequest(modifiedOmRequest);

    OMClientResponse omFileCreateResponse =
        omFileCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omFileCreateResponse.getOMResponse().getStatus());

    // Check open table whether key is added or not.

    omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);
    Assert.assertNotNull(omKeyInfo);

    List< OmKeyLocationInfo > omKeyLocationInfoList =
        omKeyInfo.getLatestVersionLocations().getLocationList();
    Assert.assertTrue(omKeyLocationInfoList.size() == 1);

    OmKeyLocationInfo omKeyLocationInfo = omKeyLocationInfoList.get(0);

    // Check modification time
    Assert.assertEquals(modifiedOmRequest.getCreateFileRequest()
        .getKeyArgs().getModificationTime(), omKeyInfo.getModificationTime());

    Assert.assertEquals(omKeyInfo.getModificationTime(),
        omKeyInfo.getCreationTime());


    // Check data of the block
    OzoneManagerProtocolProtos.KeyLocation keyLocation =
        modifiedOmRequest.getCreateFileRequest().getKeyArgs()
            .getKeyLocations(0);

    Assert.assertEquals(keyLocation.getBlockID().getContainerBlockID()
        .getContainerID(), omKeyLocationInfo.getContainerID());
    Assert.assertEquals(keyLocation.getBlockID().getContainerBlockID()
        .getLocalID(), omKeyLocationInfo.getLocalID());

  }


  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {
    OMRequest omRequest = createFileRequest(volumeName, bucketName, keyName,
        HddsProtos.ReplicationFactor.ONE, HddsProtos.ReplicationType.RATIS,
        false, true);

    TestOMRequestUtils.addVolumeToDB(volumeName, omMetadataManager);
    OMFileCreateRequest omFileCreateRequest = new OMFileCreateRequest(
        omRequest);

    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);

    omFileCreateRequest = new OMFileCreateRequest(modifiedOmRequest);


    OMClientResponse omFileCreateResponse =
        omFileCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);
    Assert.assertEquals(BUCKET_NOT_FOUND,
        omFileCreateResponse.getOMResponse().getStatus());

  }

  @Test
  public void testValidateAndUpdateCacheWithNonRecursive() throws Exception {
    testNonRecursivePath(UUID.randomUUID().toString(), false, false, false);
    testNonRecursivePath("a/b", false, false, true);

    // Create some child keys for the path
    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName,
        "a/b/c/d", 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    testNonRecursivePath("a/b/c", false, false, false);

    // Delete child key and add a path "a/b/ to key table
    omMetadataManager.getKeyTable().delete(omMetadataManager.getOzoneKey(
        volumeName, bucketName, "a/b/c/d"));


    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName,
        "a/b/", 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    testNonRecursivePath("a/b/e", false, false, false);

  }

  @Test
  public void testValidateAndUpdateCacheWithRecursive() throws Exception {
    // Should be able to create file even if parent directories does not
    // exist and key already exist, as this is with overwrite enabled.
    testNonRecursivePath(UUID.randomUUID().toString(), false, false, false);
    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName,
        "c/d/e/f", 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    testNonRecursivePath("c/d/e/f", true, true, false);
    // Create some child keys for the path
    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName,
        "a/b/c/d", 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    testNonRecursivePath("a/b/c", false, true, false);


  }

  @Test
  public void testValidateAndUpdateCacheWithRecursiveAndOverWrite()
      throws Exception {

    String key = "c/d/e/f";
    // Should be able to create file even if parent directories does not exist
    testNonRecursivePath(key, false, true, false);

    // Add the key to key table
    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName,
        key, 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);

    // Even if key exists, should be able to create file as overwrite is set
    // to true
    testNonRecursivePath(key, true, true, false);
    testNonRecursivePath(key, false, true, true);
  }

  @Test
  public void testValidateAndUpdateCacheWithNonRecursiveAndOverWrite()
      throws Exception {

    String key = "c/d/e/f";
    // Need to add the path which starts with "c/d/e" to keyTable as this is
    // non-recursive parent should exist.
    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName,
        "c/d/e/h", 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    testNonRecursivePath(key, false, false, false);

    // Add the key to key table
    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName,
        key, 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);

    // Even if key exists, should be able to create file as overwrite is set
    // to true
    testNonRecursivePath(key, true, false, false);
    testNonRecursivePath(key, false, false, true);
  }


  private void testNonRecursivePath(String key,
      boolean overWrite, boolean recursive, boolean fail) throws Exception {
    OMRequest omRequest = createFileRequest(volumeName, bucketName, key,
        HddsProtos.ReplicationFactor.ONE, HddsProtos.ReplicationType.RATIS,
        overWrite, recursive);

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    OMFileCreateRequest omFileCreateRequest = new OMFileCreateRequest(
        omRequest);

    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);

    omFileCreateRequest = new OMFileCreateRequest(modifiedOmRequest);

    OMClientResponse omFileCreateResponse =
        omFileCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    if (fail) {
      Assert.assertTrue(omFileCreateResponse.getOMResponse()
          .getStatus() == NOT_A_FILE || omFileCreateResponse.getOMResponse()
          .getStatus() == FILE_ALREADY_EXISTS);
    } else {
      long id = modifiedOmRequest.getCreateFileRequest().getClientID();

      String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
          key, id);
      OmKeyInfo omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);
      Assert.assertNotNull(omKeyInfo);

      List< OmKeyLocationInfo > omKeyLocationInfoList =
          omKeyInfo.getLatestVersionLocations().getLocationList();
      Assert.assertTrue(omKeyLocationInfoList.size() == 1);

      OmKeyLocationInfo omKeyLocationInfo = omKeyLocationInfoList.get(0);

      // Check modification time
      Assert.assertEquals(modifiedOmRequest.getCreateFileRequest()
          .getKeyArgs().getModificationTime(), omKeyInfo.getModificationTime());


      // Check data of the block
      OzoneManagerProtocolProtos.KeyLocation keyLocation =
          modifiedOmRequest.getCreateFileRequest().getKeyArgs()
              .getKeyLocations(0);

      Assert.assertEquals(keyLocation.getBlockID().getContainerBlockID()
          .getContainerID(), omKeyLocationInfo.getContainerID());
      Assert.assertEquals(keyLocation.getBlockID().getContainerBlockID()
          .getLocalID(), omKeyLocationInfo.getLocalID());
    }
  }


  /**
   * Create OMRequest which encapsulates OMFileCreateRequest.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param replicationFactor
   * @param replicationType
   * @return OMRequest
   */
  private OMRequest createFileRequest(
      String volumeName, String bucketName, String keyName,
      HddsProtos.ReplicationFactor replicationFactor,
      HddsProtos.ReplicationType replicationType, boolean overWrite,
      boolean recursive) {

    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
            .setVolumeName(volumeName).setBucketName(bucketName)
            .setKeyName(keyName).setFactor(replicationFactor)
            .setType(replicationType).setDataSize(dataSize);

    CreateFileRequest createFileRequest = CreateFileRequest.newBuilder()
        .setKeyArgs(keyArgs)
        .setIsOverwrite(overWrite)
        .setIsRecursive(recursive).build();

    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateKey)
        .setClientId(UUID.randomUUID().toString())
        .setCreateFileRequest(createFileRequest).build();

  }
}
