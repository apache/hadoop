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

package org.apache.hadoop.ozone.om.response.file;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.utils.db.BatchOperation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.event.Level;

import java.util.UUID;

/**
 * Tests OMDirectoryCreateResponse.
 */
public class TestOMDirectoryCreateResponse {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OMMetadataManager omMetadataManager;
  private BatchOperation batchOperation;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
  }

  @Test
  public void testAddToDBBatch() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OmKeyInfo omKeyInfo = TestOMRequestUtils.createOmKeyInfo(volumeName,
        bucketName, OzoneFSUtils.addTrailingSlashIfNeeded(keyName),
        HddsProtos.ReplicationType.RATIS, HddsProtos.ReplicationFactor.ONE);

    OMResponse omResponse = OMResponse.newBuilder().setCreateDirectoryResponse(
        OzoneManagerProtocolProtos.CreateDirectoryResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateDirectory)
            .build();

    OMDirectoryCreateResponse omDirectoryCreateResponse =
        new OMDirectoryCreateResponse(omKeyInfo, omResponse);

    omDirectoryCreateResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertNotNull(omMetadataManager.getKeyTable().get(
        omMetadataManager.getOzoneDirKey(volumeName, bucketName, keyName)));
  }

  @Test
  public void testAddToDBBatchWithNullOmkeyInfo() throws Exception {

    GenericTestUtils.setLogLevel(OMDirectoryCreateResponse.LOG, Level.DEBUG);
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(OMDirectoryCreateResponse.LOG);


    String volumeName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OMResponse omResponse = OMResponse.newBuilder().setCreateDirectoryResponse(
        OzoneManagerProtocolProtos.CreateDirectoryResponse.getDefaultInstance())
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateDirectory)
        .build();

    OMDirectoryCreateResponse omDirectoryCreateResponse =
        new OMDirectoryCreateResponse(null, omResponse);

    omDirectoryCreateResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertNull(omMetadataManager.getKeyTable().get(
        omMetadataManager.getOzoneDirKey(volumeName, bucketName, keyName)));

    Assert.assertTrue(logCapturer.getOutput().contains("Response Status is " +
        "OK, dirKeyInfo is null"));
  }
}
