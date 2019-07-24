/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.request.s3.multipart;

import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Base test class for S3 Multipart upload request.
 */
@SuppressWarnings("visibilitymodifier")
public class TestS3MultipartRequest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  protected OzoneManager ozoneManager;
  protected OMMetrics omMetrics;
  protected OMMetadataManager omMetadataManager;
  protected AuditLogger auditLogger;


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

  /**
   * Perform preExecute of Initiate Multipart upload request for given
   * volume, bucket and key name.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @return OMRequest - returned from preExecute.
   */
  protected OMRequest doPreExecuteInitiateMPU(
      String volumeName, String bucketName, String keyName) {
    OMRequest omRequest =
        TestOMRequestUtils.createInitiateMPURequest(volumeName, bucketName,
            keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        new S3InitiateMultipartUploadRequest(omRequest);

    OMRequest modifiedRequest =
        s3InitiateMultipartUploadRequest.preExecute(ozoneManager);

    Assert.assertNotEquals(omRequest, modifiedRequest);
    Assert.assertTrue(modifiedRequest.hasInitiateMultiPartUploadRequest());
    Assert.assertNotNull(modifiedRequest.getInitiateMultiPartUploadRequest()
        .getKeyArgs().getMultipartUploadID());
    Assert.assertTrue(modifiedRequest.getInitiateMultiPartUploadRequest()
        .getKeyArgs().getModificationTime() > 0);

    return modifiedRequest;
  }

  /**
   * Perform preExecute of Commit Multipart Upload request for given volume,
   * bucket and keyName.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param clientID
   * @param multipartUploadID
   * @param partNumber
   * @return OMRequest - returned from preExecute.
   */
  protected OMRequest doPreExecuteCommitMPU(
      String volumeName, String bucketName, String keyName,
      long clientID, String multipartUploadID, int partNumber) {

    // Just set dummy size
    long dataSize = 100L;
    OMRequest omRequest =
        TestOMRequestUtils.createCommitPartMPURequest(volumeName, bucketName,
            keyName, clientID, dataSize, multipartUploadID, partNumber);
    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        new S3MultipartUploadCommitPartRequest(omRequest);


    OMRequest modifiedRequest =
        s3MultipartUploadCommitPartRequest.preExecute(ozoneManager);

    // UserInfo and modification time is set.
    Assert.assertNotEquals(omRequest, modifiedRequest);

    return modifiedRequest;
  }


}
