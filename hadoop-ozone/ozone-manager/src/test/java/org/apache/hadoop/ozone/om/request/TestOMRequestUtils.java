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


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetVolumePropertyRequest;
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

}
