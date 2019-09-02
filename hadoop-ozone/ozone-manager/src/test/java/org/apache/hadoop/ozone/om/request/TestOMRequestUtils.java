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

package org.apache.hadoop.ozone.om.request;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.s3.bucket.S3BucketCreateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartUploadAbortRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartCommitUploadPartRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartInfoInitiateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetVolumePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .AddAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RemoveAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetAclRequest;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType;
import org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType;

import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.util.Time;

/**
 * Helper class to test OMClientRequest classes.
 */
public final class TestOMRequestUtils {

  private TestOMRequestUtils() {
    //Do nothing
  }

  /**
   * Add's volume and bucket creation entries to OM DB.
   * @param volumeName
   * @param bucketName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addVolumeAndBucketToDB(String volumeName,
      String bucketName, OMMetadataManager omMetadataManager) throws Exception {

    addVolumeToDB(volumeName, omMetadataManager);

    OmBucketInfo omBucketInfo =
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName).setCreationTime(Time.now()).build();

    omMetadataManager.getBucketTable().put(
        omMetadataManager.getBucketKey(volumeName, bucketName), omBucketInfo);
  }

  /**
   * Add key entry to KeyTable. if openKeyTable flag is true, add's entries
   * to openKeyTable, else add's it to keyTable.
   * @param openKeyTable
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param clientID
   * @param replicationType
   * @param replicationFactor
   * @param omMetadataManager
   * @throws Exception
   */
  @SuppressWarnings("parameterNumber")
  public static void addKeyToTable(boolean openKeyTable, String volumeName,
      String bucketName,
      String keyName, long clientID,
      HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor,
      OMMetadataManager omMetadataManager) throws Exception {


    OmKeyInfo.Builder builder = new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(1000L)
        .setReplicationType(replicationType)
        .setReplicationFactor(replicationFactor);

    if (openKeyTable) {
      omMetadataManager.getOpenKeyTable().put(
          omMetadataManager.getOpenKey(volumeName, bucketName, keyName,
              clientID), builder.build());
    } else {
      omMetadataManager.getKeyTable().put(omMetadataManager.getOzoneKey(
          volumeName, bucketName, keyName), builder.build());
    }

  }

  /**
   * Create OmKeyInfo.
   */

  public static OmKeyInfo createOmKeyInfo(String volumeName, String bucketName,
      String keyName, HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor) {
    return new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(1000L)
        .setReplicationType(replicationType)
        .setReplicationFactor(replicationFactor).build();
  }


  /**
   * Add volume creation entry to OM DB.
   * @param volumeName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addVolumeToDB(String volumeName,
      OMMetadataManager omMetadataManager) throws Exception {
    addVolumeToDB(volumeName, UUID.randomUUID().toString(), omMetadataManager);
  }

  public static void addS3BucketToDB(String volumeName, String s3BucketName,
      OMMetadataManager omMetadataManager) throws Exception {
    omMetadataManager.getS3Table().put(s3BucketName,
        S3BucketCreateRequest.formatS3MappingName(volumeName, s3BucketName));
  }

  /**
   * Add volume creation entry to OM DB.
   * @param volumeName
   * @param ownerName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addVolumeToDB(String volumeName, String ownerName,
      OMMetadataManager omMetadataManager) throws Exception {
    OmVolumeArgs omVolumeArgs =
        OmVolumeArgs.newBuilder().setCreationTime(Time.now())
            .setVolume(volumeName).setAdminName(ownerName)
            .setOwnerName(ownerName).build();
    omMetadataManager.getVolumeTable().put(
        omMetadataManager.getVolumeKey(volumeName), omVolumeArgs);
  }


  public static OzoneManagerProtocolProtos.OMRequest createBucketRequest(
      String bucketName, String volumeName, boolean isVersionEnabled,
      OzoneManagerProtocolProtos.StorageTypeProto storageTypeProto) {
    OzoneManagerProtocolProtos.BucketInfo bucketInfo =
        OzoneManagerProtocolProtos.BucketInfo.newBuilder()
            .setBucketName(bucketName)
            .setVolumeName(volumeName)
            .setIsVersionEnabled(isVersionEnabled)
            .setStorageType(storageTypeProto)
            .addAllMetadata(getMetadataList()).build();
    OzoneManagerProtocolProtos.CreateBucketRequest.Builder req =
        OzoneManagerProtocolProtos.CreateBucketRequest.newBuilder();
    req.setBucketInfo(bucketInfo);
    return OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setCreateBucketRequest(req)
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateBucket)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  public static OzoneManagerProtocolProtos.OMRequest createS3BucketRequest(
      String userName, String s3BucketName) {
    OzoneManagerProtocolProtos.S3CreateBucketRequest request =
        OzoneManagerProtocolProtos.S3CreateBucketRequest.newBuilder()
            .setUserName(userName)
            .setS3Bucketname(s3BucketName).build();

    return OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setCreateS3BucketRequest(request)
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateS3Bucket)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  public static OzoneManagerProtocolProtos.OMRequest deleteS3BucketRequest(
      String s3BucketName) {
    OzoneManagerProtocolProtos.S3DeleteBucketRequest request =
        OzoneManagerProtocolProtos.S3DeleteBucketRequest.newBuilder()
            .setS3BucketName(s3BucketName).build();
    return OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setDeleteS3BucketRequest(request)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteS3Bucket)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  public static List< HddsProtos.KeyValue> getMetadataList() {
    List<HddsProtos.KeyValue> metadataList = new ArrayList<>();
    metadataList.add(HddsProtos.KeyValue.newBuilder().setKey("key1").setValue(
        "value1").build());
    metadataList.add(HddsProtos.KeyValue.newBuilder().setKey("key2").setValue(
        "value2").build());
    return metadataList;
  }


  /**
   * Add user to user table.
   * @param volumeName
   * @param ownerName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addUserToDB(String volumeName, String ownerName,
      OMMetadataManager omMetadataManager) throws Exception {
    OzoneManagerProtocolProtos.VolumeList volumeList =
        OzoneManagerProtocolProtos.VolumeList.newBuilder()
            .addVolumeNames(volumeName).build();
    omMetadataManager.getUserTable().put(
        omMetadataManager.getUserKey(ownerName), volumeList);
  }

  /**
   * Create OMRequest for set volume property request with owner set.
   * @param volumeName
   * @param newOwner
   * @return OMRequest
   */
  public static OMRequest createSetVolumePropertyRequest(String volumeName,
      String newOwner) {
    SetVolumePropertyRequest setVolumePropertyRequest =
        SetVolumePropertyRequest.newBuilder().setVolumeName(volumeName)
            .setOwnerName(newOwner).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.SetVolumeProperty)
        .setSetVolumePropertyRequest(setVolumePropertyRequest).build();
  }


  /**
   * Create OMRequest for set volume property request with quota set.
   * @param volumeName
   * @param quota
   * @return OMRequest
   */
  public static OMRequest createSetVolumePropertyRequest(String volumeName,
      long quota) {
    SetVolumePropertyRequest setVolumePropertyRequest =
        SetVolumePropertyRequest.newBuilder().setVolumeName(volumeName)
            .setQuotaInBytes(quota).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.SetVolumeProperty)
        .setSetVolumePropertyRequest(setVolumePropertyRequest).build();
  }

  public static OMRequest createVolumeAddAclRequest(String volumeName,
      OzoneAcl acl) {
    AddAclRequest.Builder addAclRequestBuilder = AddAclRequest.newBuilder();
    addAclRequestBuilder.setObj(OzoneObj.toProtobuf(new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setResType(ResourceType.VOLUME)
        .setStoreType(StoreType.OZONE)
        .build()));
    if (acl != null) {
      addAclRequestBuilder.setAcl(OzoneAcl.toProtobuf(acl));
    }
    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.AddAcl)
        .setAddAclRequest(addAclRequestBuilder.build()).build();
  }

  public static OMRequest createVolumeRemoveAclRequest(String volumeName,
      OzoneAcl acl) {
    RemoveAclRequest.Builder removeAclRequestBuilder =
        RemoveAclRequest.newBuilder();
    removeAclRequestBuilder.setObj(OzoneObj.toProtobuf(
        new OzoneObjInfo.Builder()
            .setVolumeName(volumeName)
            .setResType(ResourceType.VOLUME)
            .setStoreType(StoreType.OZONE)
            .build()));
    if (acl != null) {
      removeAclRequestBuilder.setAcl(OzoneAcl.toProtobuf(acl));
    }
    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.RemoveAcl)
        .setRemoveAclRequest(removeAclRequestBuilder.build()).build();
  }

  public static OMRequest createVolumeSetAclRequest(String volumeName,
      List<OzoneAcl> acls) {
    SetAclRequest.Builder setAclRequestBuilder = SetAclRequest.newBuilder();
    setAclRequestBuilder.setObj(OzoneObj.toProtobuf(new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setResType(ResourceType.VOLUME)
        .setStoreType(StoreType.OZONE)
        .build()));
    if (acls != null) {
      acls.forEach(
          acl -> setAclRequestBuilder.addAcl(OzoneAcl.toProtobuf(acl)));
    }

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.SetAcl)
        .setSetAclRequest(setAclRequestBuilder.build()).build();
  }

  /**
   * Deletes key from Key table and adds it to DeletedKeys table.
   * @return the deletedKey name
   */
  public static String deleteKey(String ozoneKey,
      OMMetadataManager omMetadataManager) throws IOException {
    // Retrieve the keyInfo
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);

    // Delete key from KeyTable and put in DeletedKeyTable
    omMetadataManager.getKeyTable().delete(ozoneKey);
    String deletedKeyName = OmUtils.getDeletedKeyName(ozoneKey, Time.now());
    omMetadataManager.getDeletedTable().put(deletedKeyName, omKeyInfo);

    return deletedKeyName;
  }

  /**
   * Create OMRequest which encapsulates InitiateMultipartUpload request.
   * @param volumeName
   * @param bucketName
   * @param keyName
   */
  public static OMRequest createInitiateMPURequest(String volumeName,
      String bucketName, String keyName) {
    MultipartInfoInitiateRequest
        multipartInfoInitiateRequest =
        MultipartInfoInitiateRequest.newBuilder().setKeyArgs(
            KeyArgs.newBuilder().setVolumeName(volumeName).setKeyName(keyName)
                .setBucketName(bucketName)).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.InitiateMultiPartUpload)
        .setInitiateMultiPartUploadRequest(multipartInfoInitiateRequest)
        .build();
  }

  /**
   * Create OMRequest which encapsulates InitiateMultipartUpload request.
   * @param volumeName
   * @param bucketName
   * @param keyName
   */
  public static OMRequest createCommitPartMPURequest(String volumeName,
      String bucketName, String keyName, long clientID, long size,
      String multipartUploadID, int partNumber) {

    // Just set dummy size.
    KeyArgs.Builder keyArgs =
        KeyArgs.newBuilder().setVolumeName(volumeName).setKeyName(keyName)
            .setBucketName(bucketName)
            .setDataSize(size)
            .setMultipartNumber(partNumber)
            .setMultipartUploadID(multipartUploadID)
            .addAllKeyLocations(new ArrayList<>());
    // Just adding dummy list. As this is for UT only.

    MultipartCommitUploadPartRequest multipartCommitUploadPartRequest =
        MultipartCommitUploadPartRequest.newBuilder()
            .setKeyArgs(keyArgs).setClientID(clientID).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.CommitMultiPartUpload)
        .setCommitMultiPartUploadRequest(multipartCommitUploadPartRequest)
        .build();
  }

  public static OMRequest createAbortMPURequest(String volumeName,
      String bucketName, String keyName, String multipartUploadID) {
    KeyArgs.Builder keyArgs =
        KeyArgs.newBuilder().setVolumeName(volumeName)
            .setKeyName(keyName)
            .setBucketName(bucketName)
            .setMultipartUploadID(multipartUploadID);

    MultipartUploadAbortRequest multipartUploadAbortRequest =
        MultipartUploadAbortRequest.newBuilder().setKeyArgs(keyArgs).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.AbortMultiPartUpload)
        .setAbortMultiPartUploadRequest(multipartUploadAbortRequest).build();
  }

  public static OMRequest createCompleteMPURequest(String volumeName,
      String bucketName, String keyName, String multipartUploadID,
      List<OzoneManagerProtocolProtos.Part> partList) {
    KeyArgs.Builder keyArgs =
        KeyArgs.newBuilder().setVolumeName(volumeName)
            .setKeyName(keyName)
            .setBucketName(bucketName)
            .setMultipartUploadID(multipartUploadID);

    MultipartUploadCompleteRequest multipartUploadCompleteRequest =
        MultipartUploadCompleteRequest.newBuilder().setKeyArgs(keyArgs)
            .addAllPartsList(partList).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.CompleteMultiPartUpload)
        .setCompleteMultiPartUploadRequest(multipartUploadCompleteRequest)
        .build();

  }
}
