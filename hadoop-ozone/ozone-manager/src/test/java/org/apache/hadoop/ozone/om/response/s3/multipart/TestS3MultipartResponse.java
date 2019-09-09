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

package org.apache.hadoop.ozone.om.response.s3.multipart;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartUploadAbortResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .PartKeyInfo;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.utils.db.BatchOperation;

/**
 * Base test class for S3 MPU response.
 */

@SuppressWarnings("VisibilityModifier")
public class TestS3MultipartResponse {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  protected OMMetadataManager omMetadataManager;
  protected BatchOperation batchOperation;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
  }


  public S3InitiateMultipartUploadResponse createS3InitiateMPUResponse(
      String volumeName, String bucketName, String keyName,
      String multipartUploadID) {
    OmMultipartKeyInfo multipartKeyInfo = new OmMultipartKeyInfo(
        multipartUploadID, new HashMap<>());

    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setReplicationType(HddsProtos.ReplicationType.RATIS)
        .setReplicationFactor(HddsProtos.ReplicationFactor.ONE)
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())))
        .build();

    OMResponse omResponse = OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.InitiateMultiPartUpload)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setSuccess(true).setInitiateMultiPartUploadResponse(
            OzoneManagerProtocolProtos.MultipartInfoInitiateResponse
                .newBuilder().setVolumeName(volumeName)
                .setBucketName(bucketName)
                .setKeyName(keyName)
                .setMultipartUploadID(multipartUploadID)).build();

    return new S3InitiateMultipartUploadResponse(multipartKeyInfo, omKeyInfo,
            omResponse);
  }

  public S3MultipartUploadAbortResponse createS3AbortMPUResponse(
      String multipartKey, long timeStamp,
      OmMultipartKeyInfo omMultipartKeyInfo) {
    OMResponse omResponse = OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.AbortMultiPartUpload)
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setSuccess(true)
        .setAbortMultiPartUploadResponse(
            MultipartUploadAbortResponse.newBuilder().build()).build();

    return new S3MultipartUploadAbortResponse(multipartKey, timeStamp,
            omMultipartKeyInfo,
            omResponse);
  }


  public void addPart(int partNumber, PartKeyInfo partKeyInfo,
      OmMultipartKeyInfo omMultipartKeyInfo) {
    omMultipartKeyInfo.addPartKeyInfo(partNumber, partKeyInfo);
  }

  public PartKeyInfo createPartKeyInfo(
      String volumeName, String bucketName, String keyName, int partNumber) {
    return PartKeyInfo.newBuilder()
        .setPartNumber(partNumber)
        .setPartName(omMetadataManager.getMultipartKey(volumeName,
            bucketName, keyName, UUID.randomUUID().toString()))
        .setPartKeyInfo(KeyInfo.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setDataSize(100L) // Just set dummy size for testing
            .setCreationTime(Time.now())
            .setModificationTime(Time.now())
            .setType(HddsProtos.ReplicationType.RATIS)
            .setFactor(HddsProtos.ReplicationFactor.ONE).build()).build();
  }
}
