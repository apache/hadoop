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

import java.util.UUID;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateDirectoryRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Test OM directory create request.
 */
public class TestOMDirectoryCreateRequest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;
  private AuditLogger auditLogger;
  // Just setting ozoneManagerDoubleBuffer which does nothing.
  private OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper =
      ((response, transactionIndex) -> {
        return null;
      });

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
  }

  @After
  public void stop() {
    omMetrics.unRegister();
    Mockito.framework().clearInlineMocks();
  }

  @Test
  public void testPreExecute() throws Exception {

    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = "a/b/c";

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest);

    OMRequest modifiedOmRequest =
        omDirectoryCreateRequest.preExecute(ozoneManager);

    // As in preExecute, we modify original request.
    Assert.assertNotEquals(omRequest, modifiedOmRequest);

  }


  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = RandomStringUtils.randomAlphabetic(5);
    for (int i =0; i< 3; i++) {
      keyName += "/" + RandomStringUtils.randomAlphabetic(5);
    }

    // Add volume and bucket entries to DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest);

    OMRequest modifiedOmRequest =
        omDirectoryCreateRequest.preExecute(ozoneManager);

    omDirectoryCreateRequest = new OMDirectoryCreateRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        omDirectoryCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse.getOMResponse().getStatus()
        == OzoneManagerProtocolProtos.Status.OK);
    Assert.assertTrue(omMetadataManager.getKeyTable().get(
        omMetadataManager.getOzoneDirKey(
            volumeName, bucketName, keyName)) != null);

  }


  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = RandomStringUtils.randomAlphabetic(5);
    for (int i =0; i< 3; i++) {
      keyName += "/" + RandomStringUtils.randomAlphabetic(5);
    }

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest);

    OMRequest modifiedOmRequest =
        omDirectoryCreateRequest.preExecute(ozoneManager);

    omDirectoryCreateRequest = new OMDirectoryCreateRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        omDirectoryCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse.getOMResponse().getStatus()
        == OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND);

    // Key should not exist in DB
    Assert.assertTrue(omMetadataManager.getKeyTable().get(
        omMetadataManager.getOzoneDirKey(
            volumeName, bucketName, keyName)) == null);

  }

  @Test
  public void testValidateAndUpdateCacheWithSubDirectoryInPath()
      throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = RandomStringUtils.randomAlphabetic(5);
    for (int i =0; i< 3; i++) {
      keyName += "/" + RandomStringUtils.randomAlphabetic(5);
    }

    // Add volume and bucket entries to DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName,
        keyName.substring(0, 12), 1L, HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest);

    OMRequest modifiedOmRequest =
        omDirectoryCreateRequest.preExecute(ozoneManager);

    omDirectoryCreateRequest = new OMDirectoryCreateRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        omDirectoryCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse.getOMResponse().getStatus()
        == OzoneManagerProtocolProtos.Status.OK);

    // Key should exist in DB and cache.
    Assert.assertTrue(omMetadataManager.getKeyTable().get(
        omMetadataManager.getOzoneDirKey(
            volumeName, bucketName, keyName)) != null);
    Assert.assertTrue(omMetadataManager.getKeyTable().getCacheValue(
        new CacheKey<>(omMetadataManager.getOzoneDirKey(
            volumeName, bucketName, keyName))) != null);

  }

  @Test
  public void testValidateAndUpdateCacheWithDirectoryAlreadyExists()
      throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = RandomStringUtils.randomAlphabetic(5);
    for (int i =0; i< 3; i++) {
      keyName += "/" + RandomStringUtils.randomAlphabetic(5);
    }

    // Add volume and bucket entries to DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName,
        OzoneFSUtils.addTrailingSlashIfNeeded(keyName), 1L,
        HddsProtos.ReplicationType.RATIS, HddsProtos.ReplicationFactor.ONE,
        omMetadataManager);
    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest);

    OMRequest modifiedOmRequest =
        omDirectoryCreateRequest.preExecute(ozoneManager);

    omDirectoryCreateRequest = new OMDirectoryCreateRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        omDirectoryCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse.getOMResponse().getStatus()
        == OzoneManagerProtocolProtos.Status.OK);

    // Key should exist in DB
    Assert.assertTrue(omMetadataManager.getKeyTable().get(
        omMetadataManager.getOzoneDirKey(
            volumeName, bucketName, keyName)) != null);

    // As it already exists, it should not be in cache.
    Assert.assertTrue(omMetadataManager.getKeyTable().getCacheValue(
        new CacheKey<>(omMetadataManager.getOzoneDirKey(
            volumeName, bucketName, keyName))) == null);

  }

  @Test
  public void testValidateAndUpdateCacheWithFilesInPath() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = RandomStringUtils.randomAlphabetic(5);
    for (int i =0; i< 3; i++) {
      keyName += "/" + RandomStringUtils.randomAlphabetic(5);
    }

    // Add volume and bucket entries to DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    // Add a key with first two levels.
    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName,
        keyName.substring(0, 11), 1L, HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest);

    OMRequest modifiedOmRequest =
        omDirectoryCreateRequest.preExecute(ozoneManager);

    omDirectoryCreateRequest = new OMDirectoryCreateRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        omDirectoryCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse.getOMResponse().getStatus()
        == OzoneManagerProtocolProtos.Status.FILE_ALREADY_EXISTS);

    // Key should not exist in DB
    Assert.assertTrue(omMetadataManager.getKeyTable().get(
        omMetadataManager.getOzoneDirKey(
            volumeName, bucketName, keyName)) == null);

  }

  /**
   * Create OMRequest which encapsulates CreateDirectory request.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @return OMRequest
   */
  private OMRequest createDirectoryRequest(String volumeName, String bucketName,
      String keyName) {
    return OMRequest.newBuilder().setCreateDirectoryRequest(
        CreateDirectoryRequest.newBuilder().setKeyArgs(
            KeyArgs.newBuilder().setVolumeName(volumeName)
                .setBucketName(bucketName).setKeyName(keyName)))
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateDirectory)
        .setClientId(UUID.randomUUID().toString()).build();
  }

}
