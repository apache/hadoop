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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.web.storage;

import java.util.List;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.KeyData;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.ozone.OzoneConsts.Versioning;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.KeyInfo;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.response.VolumeOwner;
import org.apache.hadoop.util.StringUtils;

/**
 * This class contains methods that define the translation between the Ozone
 * domain model and the storage container domain model.
 */
final class OzoneContainerTranslation {

  private static final String ACLS = "ACLS";
  private static final String BUCKET = "BUCKET";
  private static final String BUCKET_NAME = "BUCKET_NAME";
  private static final String CREATED_BY = "CREATED_BY";
  private static final String CREATED_ON = "CREATED_ON";
  private static final String KEY = "KEY";
  private static final String OWNER = "OWNER";
  private static final String QUOTA = "QUOTA";
  private static final String STORAGE_TYPE = "STORAGE_TYPE";
  private static final String TYPE = "TYPE";
  private static final String VERSIONING = "VERSIONING";
  private static final String VOLUME = "VOLUME";
  private static final String VOLUME_NAME = "VOLUME_NAME";

  /**
   * Creates key data intended for reading a container key.
   *
   * @param containerName container name
   * @param containerKey container key
   * @return KeyData intended for reading the container key
   */
  public static KeyData containerKeyDataForRead(String containerName,
      String containerKey) {
    return KeyData
        .newBuilder()
        .setContainerName(containerName)
        .setName(containerKey)
        .build();
  }

  /**
   * Translates a bucket to its container representation.
   *
   * @param containerName container name
   * @param containerKey container key
   * @param bucket the bucket to translate
   * @return KeyData representation of bucket
   */
  public static KeyData fromBucketToContainerKeyData(
      String containerName, String containerKey, BucketInfo bucket) {
    KeyData.Builder containerKeyData = KeyData
        .newBuilder()
        .setContainerName(containerName)
        .setName(containerKey)
        .addMetadata(newKeyValue(TYPE, BUCKET))
        .addMetadata(newKeyValue(VOLUME_NAME, bucket.getVolumeName()))
        .addMetadata(newKeyValue(BUCKET_NAME, bucket.getBucketName()));

    if (bucket.getAcls() != null) {
      containerKeyData.addMetadata(newKeyValue(ACLS,
          StringUtils.join(',', bucket.getAcls())));
    }

    if (bucket.getVersioning() != null &&
        bucket.getVersioning() != Versioning.NOT_DEFINED) {
      containerKeyData.addMetadata(newKeyValue(VERSIONING,
          bucket.getVersioning().name()));
    }

    if (bucket.getStorageType() != StorageType.RAM_DISK) {
      containerKeyData.addMetadata(newKeyValue(STORAGE_TYPE,
          bucket.getStorageType().name()));
    }

    return containerKeyData.build();
  }

  /**
   * Translates a bucket from its container representation.
   *
   * @param metadata container metadata representing the bucket
   * @return bucket translated from container representation
   */
  public static BucketInfo fromContainerKeyValueListToBucket(
      List<KeyValue> metadata) {
    BucketInfo bucket = new BucketInfo();
    for (KeyValue keyValue : metadata) {
      switch (keyValue.getKey()) {
      case VOLUME_NAME:
        bucket.setVolumeName(keyValue.getValue());
        break;
      case BUCKET_NAME:
        bucket.setBucketName(keyValue.getValue());
        break;
      case VERSIONING:
        bucket.setVersioning(
            Enum.valueOf(Versioning.class, keyValue.getValue()));
        break;
      case STORAGE_TYPE:
        bucket.setStorageType(
            Enum.valueOf(StorageType.class, keyValue.getValue()));
        break;
      default:
        break;
      }
    }
    return bucket;
  }

  /**
   * Translates a volume from its container representation.
   *
   * @param metadata container metadata representing the volume
   * @return volume translated from container representation
   */
  public static VolumeInfo fromContainerKeyValueListToVolume(
      List<KeyValue> metadata) {
    VolumeInfo volume = new VolumeInfo();
    for (KeyValue keyValue : metadata) {
      switch (keyValue.getKey()) {
      case VOLUME_NAME:
        volume.setVolumeName(keyValue.getValue());
        break;
      case CREATED_BY:
        volume.setCreatedBy(keyValue.getValue());
        break;
      case CREATED_ON:
        volume.setCreatedOn(keyValue.getValue());
        break;
      case OWNER:
        volume.setOwner(new VolumeOwner(keyValue.getValue()));
        break;
      case QUOTA:
        volume.setQuota(OzoneQuota.parseQuota(keyValue.getValue()));
        break;
      default:
        break;
      }
    }
    return volume;
  }

  /**
   * Translates a key to its container representation.
   *
   * @param containerName container name
   * @param containerKey container key
   * @param keyInfo key information received from call
   * @return KeyData intended for reading the container key
   */
  public static KeyData fromKeyToContainerKeyData(String containerName,
      String containerKey, KeyInfo key) {
    return KeyData
        .newBuilder()
        .setContainerName(containerName)
        .setName(containerKey)
        .addMetadata(newKeyValue(TYPE, KEY))
        .build();
  }

  /**
   * Translates a key to its container representation.  The return value is a
   * builder that can be manipulated further before building the result.
   *
   * @param containerName container name
   * @param containerKey container key
   * @param keyInfo key information received from call
   * @return KeyData builder
   */
  public static KeyData.Builder fromKeyToContainerKeyDataBuilder(
      String containerName, String containerKey, KeyInfo key) {
    return KeyData
        .newBuilder()
        .setContainerName(containerName)
        .setName(containerKey)
        .addMetadata(newKeyValue(TYPE, KEY));
  }

  /**
   * Translates a volume to its container representation.
   *
   * @param containerName container name
   * @param containerKey container key
   * @param volume the volume to translate
   * @return KeyData representation of volume
   */
  public static KeyData fromVolumeToContainerKeyData(
      String containerName, String containerKey, VolumeInfo volume) {
    KeyData.Builder containerKeyData = KeyData
        .newBuilder()
        .setContainerName(containerName)
        .setName(containerKey)
        .addMetadata(newKeyValue(TYPE, VOLUME))
        .addMetadata(newKeyValue(VOLUME_NAME, volume.getVolumeName()))
        .addMetadata(newKeyValue(CREATED_ON, volume.getCreatedOn()));

    if (volume.getQuota() != null && volume.getQuota().sizeInBytes() != -1L) {
      containerKeyData.addMetadata(newKeyValue(QUOTA,
          OzoneQuota.formatQuota(volume.getQuota())));
    }

    if (volume.getOwner() != null && volume.getOwner().getName() != null &&
        !volume.getOwner().getName().isEmpty()) {
      containerKeyData.addMetadata(newKeyValue(OWNER,
          volume.getOwner().getName()));
    }

    if (volume.getCreatedBy() != null && !volume.getCreatedBy().isEmpty()) {
      containerKeyData.addMetadata(
          newKeyValue(CREATED_BY, volume.getCreatedBy()));
    }

    return containerKeyData.build();
  }

  /**
   * Translates a key-value pair to its container representation.
   *
   * @param key the key
   * @param value the value
   * @return container representation of key-value pair
   */
  private static KeyValue newKeyValue(String key, Object value) {
    return KeyValue.newBuilder().setKey(key).setValue(value.toString()).build();
  }

  /**
   * There is no need to instantiate this class.
   */
  private OzoneContainerTranslation() {
  }
}
