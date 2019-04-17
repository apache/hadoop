/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;
import org.iq80.leveldb.DBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OM bucket manager.
 */
public class BucketManagerImpl implements BucketManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(BucketManagerImpl.class);

  /**
   * OMMetadataManager is used for accessing OM MetadataDB and ReadWriteLock.
   */
  private final OMMetadataManager metadataManager;
  private final KeyProviderCryptoExtension kmsProvider;

  private final boolean isRatisEnabled;

  /**
   * Constructs BucketManager.
   *
   * @param metadataManager
   */
  public BucketManagerImpl(OMMetadataManager metadataManager) {
    this(metadataManager, null, false);
  }

  public BucketManagerImpl(OMMetadataManager metadataManager,
                           KeyProviderCryptoExtension kmsProvider) {
    this(metadataManager, kmsProvider, false);
  }

  public BucketManagerImpl(OMMetadataManager metadataManager,
      KeyProviderCryptoExtension kmsProvider, boolean isRatisEnabled) {
    this.metadataManager = metadataManager;
    this.kmsProvider = kmsProvider;
    this.isRatisEnabled = isRatisEnabled;
  }

  KeyProviderCryptoExtension getKMSProvider() {
    return kmsProvider;
  }

  /**
   * MetadataDB is maintained in MetadataManager and shared between
   * BucketManager and VolumeManager. (and also by BlockManager)
   *
   * BucketManager uses MetadataDB to store bucket level information.
   *
   * Keys used in BucketManager for storing data into MetadataDB
   * for BucketInfo:
   * {volume/bucket} -> bucketInfo
   *
   * Work flow of create bucket:
   *
   * -> Check if the Volume exists in metadataDB, if not throw
   * VolumeNotFoundException.
   * -> Else check if the Bucket exists in metadataDB, if so throw
   * BucketExistException
   * -> Else update MetadataDB with VolumeInfo.
   */

  /**
   * Creates a bucket.
   *
   * @param bucketInfo - OmBucketInfo.
   */
  @Override
  public OmBucketInfo createBucket(OmBucketInfo bucketInfo) throws IOException {
    Preconditions.checkNotNull(bucketInfo);
    String volumeName = bucketInfo.getVolumeName();
    String bucketName = bucketInfo.getBucketName();
    metadataManager.getLock().acquireVolumeLock(volumeName);
    metadataManager.getLock().acquireBucketLock(volumeName, bucketName);
    try {
      String volumeKey = metadataManager.getVolumeKey(volumeName);
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);

      //Check if the volume exists
      if (metadataManager.getVolumeTable().get(volumeKey) == null) {
        LOG.debug("volume: {} not found ", volumeName);
        throw new OMException("Volume doesn't exist",
            OMException.ResultCodes.VOLUME_NOT_FOUND);
      }
      //Check if bucket already exists
      if (metadataManager.getBucketTable().get(bucketKey) != null) {
        LOG.debug("bucket: {} already exists ", bucketName);
        throw new OMException("Bucket already exist",
            OMException.ResultCodes.BUCKET_ALREADY_EXISTS);
      }
      BucketEncryptionKeyInfo bek = bucketInfo.getEncryptionKeyInfo();
      BucketEncryptionKeyInfo.Builder bekb = null;
      if (bek != null) {
        if (kmsProvider == null) {
          throw new OMException("Invalid KMS provider, check configuration " +
              CommonConfigurationKeys.HADOOP_SECURITY_KEY_PROVIDER_PATH,
              OMException.ResultCodes.INVALID_KMS_PROVIDER);
        }
        if (bek.getKeyName() == null) {
          throw new OMException("Bucket encryption key needed.", OMException
              .ResultCodes.BUCKET_ENCRYPTION_KEY_NOT_FOUND);
        }
        // Talk to KMS to retrieve the bucket encryption key info.
        KeyProvider.Metadata metadata = getKMSProvider().getMetadata(
            bek.getKeyName());
        if (metadata == null) {
          throw new OMException("Bucket encryption key " + bek.getKeyName()
              + " doesn't exist.",
              OMException.ResultCodes.BUCKET_ENCRYPTION_KEY_NOT_FOUND);
        }
        // If the provider supports pool for EDEKs, this will fill in the pool
        kmsProvider.warmUpEncryptedKeys(bek.getKeyName());
        bekb = new BucketEncryptionKeyInfo.Builder()
            .setKeyName(bek.getKeyName())
            .setVersion(CryptoProtocolVersion.ENCRYPTION_ZONES)
            .setSuite(CipherSuite.convert(metadata.getCipher()));
      }
      OmBucketInfo.Builder omBucketInfoBuilder = OmBucketInfo.newBuilder()
          .setVolumeName(bucketInfo.getVolumeName())
          .setBucketName(bucketInfo.getBucketName())
          .setAcls(bucketInfo.getAcls())
          .setStorageType(bucketInfo.getStorageType())
          .setIsVersionEnabled(bucketInfo.getIsVersionEnabled())
          .setCreationTime(Time.now())
          .addAllMetadata(bucketInfo.getMetadata());

      if (bekb != null) {
        omBucketInfoBuilder.setBucketEncryptionKey(bekb.build());
      }

      OmBucketInfo omBucketInfo = omBucketInfoBuilder.build();
      if (!isRatisEnabled) {
        commitCreateBucketInfoToDB(omBucketInfo);
      }
      LOG.debug("created bucket: {} in volume: {}", bucketName, volumeName);
      return omBucketInfo;
    } catch (IOException | DBException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Bucket creation failed for bucket:{} in volume:{}",
            bucketName, volumeName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseBucketLock(volumeName, bucketName);
      metadataManager.getLock().releaseVolumeLock(volumeName);
    }
  }


  public void applyCreateBucket(OmBucketInfo omBucketInfo) throws IOException {
    Preconditions.checkNotNull(omBucketInfo);
    try {
      commitCreateBucketInfoToDB(omBucketInfo);
    } catch (IOException ex) {
      LOG.error("Apply CreateBucket Failed for bucket: {}, volume: {}",
          omBucketInfo.getBucketName(), omBucketInfo.getVolumeName(), ex);
      throw ex;
    }
  }

  private void commitCreateBucketInfoToDB(OmBucketInfo omBucketInfo)
      throws IOException {
    String dbBucketKey =
        metadataManager.getBucketKey(omBucketInfo.getVolumeName(),
            omBucketInfo.getBucketName());
    metadataManager.getBucketTable().put(dbBucketKey,
        omBucketInfo);
  }

  /**
   * Returns Bucket Information.
   *
   * @param volumeName - Name of the Volume.
   * @param bucketName - Name of the Bucket.
   */
  @Override
  public OmBucketInfo getBucketInfo(String volumeName, String bucketName)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    metadataManager.getLock().acquireBucketLock(volumeName, bucketName);
    try {
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      OmBucketInfo value = metadataManager.getBucketTable().get(bucketKey);
      if (value == null) {
        LOG.debug("bucket: {} not found in volume: {}.", bucketName,
            volumeName);
        throw new OMException("Bucket not found",
            OMException.ResultCodes.BUCKET_NOT_FOUND);
      }
      return value;
    } catch (IOException | DBException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Exception while getting bucket info for bucket: {}",
            bucketName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseBucketLock(volumeName, bucketName);
    }
  }

  /**
   * Sets bucket property from args.
   *
   * @param args - BucketArgs.
   * @throws IOException - On Failure.
   */
  @Override
  public OmBucketInfo setBucketProperty(OmBucketArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    metadataManager.getLock().acquireBucketLock(volumeName, bucketName);
    try {
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      OmBucketInfo oldBucketInfo =
          metadataManager.getBucketTable().get(bucketKey);
      //Check if bucket exist
      if (oldBucketInfo == null) {
        LOG.debug("bucket: {} not found ", bucketName);
        throw new OMException("Bucket doesn't exist",
            OMException.ResultCodes.BUCKET_NOT_FOUND);
      }
      OmBucketInfo.Builder bucketInfoBuilder = OmBucketInfo.newBuilder();
      bucketInfoBuilder.setVolumeName(oldBucketInfo.getVolumeName())
          .setBucketName(oldBucketInfo.getBucketName());
      bucketInfoBuilder.addAllMetadata(args.getMetadata());

      //Check ACLs to update
      if (args.getAddAcls() != null || args.getRemoveAcls() != null) {
        bucketInfoBuilder.setAcls(getUpdatedAclList(oldBucketInfo.getAcls(),
            args.getRemoveAcls(), args.getAddAcls()));
        LOG.debug("Updating ACLs for bucket: {} in volume: {}",
            bucketName, volumeName);
      } else {
        bucketInfoBuilder.setAcls(oldBucketInfo.getAcls());
      }

      //Check StorageType to update
      StorageType storageType = args.getStorageType();
      if (storageType != null) {
        bucketInfoBuilder.setStorageType(storageType);
        LOG.debug("Updating bucket storage type for bucket: {} in volume: {}",
            bucketName, volumeName);
      } else {
        bucketInfoBuilder.setStorageType(oldBucketInfo.getStorageType());
      }

      //Check Versioning to update
      Boolean versioning = args.getIsVersionEnabled();
      if (versioning != null) {
        bucketInfoBuilder.setIsVersionEnabled(versioning);
        LOG.debug("Updating bucket versioning for bucket: {} in volume: {}",
            bucketName, volumeName);
      } else {
        bucketInfoBuilder
            .setIsVersionEnabled(oldBucketInfo.getIsVersionEnabled());
      }
      bucketInfoBuilder.setCreationTime(oldBucketInfo.getCreationTime());

      OmBucketInfo omBucketInfo = bucketInfoBuilder.build();

      if (!isRatisEnabled) {
        commitSetBucketPropertyInfoToDB(omBucketInfo);
      }
      return omBucketInfo;
    } catch (IOException | DBException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Setting bucket property failed for bucket:{} in volume:{}",
            bucketName, volumeName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseBucketLock(volumeName, bucketName);
    }
  }

  public void applySetBucketProperty(OmBucketInfo omBucketInfo)
      throws IOException {
    try {
      commitSetBucketPropertyInfoToDB(omBucketInfo);
    } catch (IOException ex) {
      LOG.error("Apply SetBucket property failed for bucket:{} in " +
              "volume:{}", omBucketInfo.getBucketName(),
          omBucketInfo.getVolumeName(), ex);
      throw ex;
    }
  }

  private void commitSetBucketPropertyInfoToDB(OmBucketInfo omBucketInfo)
      throws IOException {
    commitCreateBucketInfoToDB(omBucketInfo);
  }

  /**
   * Updates the existing ACL list with remove and add ACLs that are passed.
   * Remove is done before Add.
   *
   * @param existingAcls - old ACL list.
   * @param removeAcls - ACLs to be removed.
   * @param addAcls - ACLs to be added.
   * @return updated ACL list.
   */
  private List<OzoneAcl> getUpdatedAclList(List<OzoneAcl> existingAcls,
      List<OzoneAcl> removeAcls, List<OzoneAcl> addAcls) {
    if (removeAcls != null && !removeAcls.isEmpty()) {
      existingAcls.removeAll(removeAcls);
    }
    if (addAcls != null && !addAcls.isEmpty()) {
      addAcls.stream().filter(acl -> !existingAcls.contains(acl)).forEach(
          existingAcls::add);
    }
    return existingAcls;
  }

  /**
   * Deletes an existing empty bucket from volume.
   *
   * @param volumeName - Name of the volume.
   * @param bucketName - Name of the bucket.
   * @throws IOException - on Failure.
   */
  @Override
  public void deleteBucket(String volumeName, String bucketName)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    metadataManager.getLock().acquireBucketLock(volumeName, bucketName);
    try {
      //Check if bucket exists
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      if (metadataManager.getBucketTable().get(bucketKey) == null) {
        LOG.debug("bucket: {} not found ", bucketName);
        throw new OMException("Bucket doesn't exist",
            OMException.ResultCodes.BUCKET_NOT_FOUND);
      }
      //Check if bucket is empty
      if (!metadataManager.isBucketEmpty(volumeName, bucketName)) {
        LOG.debug("bucket: {} is not empty ", bucketName);
        throw new OMException("Bucket is not empty",
            OMException.ResultCodes.BUCKET_NOT_EMPTY);
      }

      if (!isRatisEnabled) {
        commitDeleteBucketInfoToOMDB(bucketKey);
      }
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Delete bucket failed for bucket:{} in volume:{}", bucketName,
            volumeName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseBucketLock(volumeName, bucketName);
    }
  }

  public void applyDeleteBucket(String volumeName, String bucketName)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    try {
      commitDeleteBucketInfoToOMDB(metadataManager.getBucketKey(volumeName,
          bucketName));
    } catch (IOException ex) {
      LOG.error("Apply DeleteBucket Failed for bucket: {}, volume: {}",
          bucketName, volumeName, ex);
      throw ex;
    }
  }

  private void commitDeleteBucketInfoToOMDB(String dbBucketKey)
      throws IOException {
    metadataManager.getBucketTable().delete(dbBucketKey);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<OmBucketInfo> listBuckets(String volumeName,
      String startBucket, String bucketPrefix, int maxNumOfBuckets)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    return metadataManager.listBuckets(
        volumeName, startBucket, bucketPrefix, maxNumOfBuckets);

  }
}
