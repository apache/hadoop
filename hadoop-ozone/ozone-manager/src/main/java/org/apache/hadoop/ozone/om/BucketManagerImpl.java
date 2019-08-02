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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

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
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;
import org.iq80.leveldb.DBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneAcl.ZERO_BITSET;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclScope.*;

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
  public void createBucket(OmBucketInfo bucketInfo) throws IOException {
    Preconditions.checkNotNull(bucketInfo);
    String volumeName = bucketInfo.getVolumeName();
    String bucketName = bucketInfo.getBucketName();
    boolean acquiredBucketLock = false;
    metadataManager.getLock().acquireLock(VOLUME_LOCK, volumeName);
    try {
      acquiredBucketLock = metadataManager.getLock().acquireLock(BUCKET_LOCK,
          volumeName, bucketName);
      String volumeKey = metadataManager.getVolumeKey(volumeName);
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      OmVolumeArgs volumeArgs = metadataManager.getVolumeTable().get(volumeKey);

      //Check if the volume exists
      if (volumeArgs == null) {
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
      List<OzoneAcl> acls = new ArrayList<>();
      acls.addAll(bucketInfo.getAcls());
      volumeArgs.getAclMap().getDefaultAclList().forEach(
          a -> acls.add(OzoneAcl.fromProtobufWithAccessType(a)));

      OmBucketInfo.Builder omBucketInfoBuilder = OmBucketInfo.newBuilder()
          .setVolumeName(bucketInfo.getVolumeName())
          .setBucketName(bucketInfo.getBucketName())
          .setAcls(acls)
          .setStorageType(bucketInfo.getStorageType())
          .setIsVersionEnabled(bucketInfo.getIsVersionEnabled())
          .setCreationTime(Time.now())
          .addAllMetadata(bucketInfo.getMetadata());

      if (bekb != null) {
        omBucketInfoBuilder.setBucketEncryptionKey(bekb.build());
      }

      OmBucketInfo omBucketInfo = omBucketInfoBuilder.build();
      commitBucketInfoToDB(omBucketInfo);
      LOG.debug("created bucket: {} in volume: {}", bucketName, volumeName);
    } catch (IOException | DBException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Bucket creation failed for bucket:{} in volume:{}",
            bucketName, volumeName, ex);
      }
      throw ex;
    } finally {
      if (acquiredBucketLock) {
        metadataManager.getLock().releaseLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
      metadataManager.getLock().releaseLock(VOLUME_LOCK, volumeName);
    }
  }

  private void commitBucketInfoToDB(OmBucketInfo omBucketInfo)
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
    metadataManager.getLock().acquireLock(BUCKET_LOCK, volumeName, bucketName);
    try {
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      OmBucketInfo value = metadataManager.getBucketTable().get(bucketKey);
      if (value == null) {
        LOG.debug("bucket: {} not found in volume: {}.", bucketName,
            volumeName);
        throw new OMException("Bucket not found",
            BUCKET_NOT_FOUND);
      }
      return value;
    } catch (IOException | DBException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Exception while getting bucket info for bucket: {}",
            bucketName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseLock(BUCKET_LOCK, volumeName,
          bucketName);
    }
  }

  /**
   * Sets bucket property from args.
   *
   * @param args - BucketArgs.
   * @throws IOException - On Failure.
   */
  @Override
  public void setBucketProperty(OmBucketArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    metadataManager.getLock().acquireLock(BUCKET_LOCK, volumeName, bucketName);
    try {
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      OmBucketInfo oldBucketInfo =
          metadataManager.getBucketTable().get(bucketKey);
      //Check if bucket exist
      if (oldBucketInfo == null) {
        LOG.debug("bucket: {} not found ", bucketName);
        throw new OMException("Bucket doesn't exist",
            BUCKET_NOT_FOUND);
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
      commitBucketInfoToDB(omBucketInfo);
    } catch (IOException | DBException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Setting bucket property failed for bucket:{} in volume:{}",
            bucketName, volumeName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseLock(BUCKET_LOCK, volumeName,
          bucketName);
    }
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
    metadataManager.getLock().acquireLock(BUCKET_LOCK, volumeName, bucketName);
    try {
      //Check if bucket exists
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      if (metadataManager.getBucketTable().get(bucketKey) == null) {
        LOG.debug("bucket: {} not found ", bucketName);
        throw new OMException("Bucket doesn't exist",
            BUCKET_NOT_FOUND);
      }
      //Check if bucket is empty
      if (!metadataManager.isBucketEmpty(volumeName, bucketName)) {
        LOG.debug("bucket: {} is not empty ", bucketName);
        throw new OMException("Bucket is not empty",
            OMException.ResultCodes.BUCKET_NOT_EMPTY);
      }
      commitDeleteBucketInfoToOMDB(bucketKey);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Delete bucket failed for bucket:{} in volume:{}", bucketName,
            volumeName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseLock(BUCKET_LOCK, volumeName,
          bucketName);
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

  /**
   * Add acl for Ozone object. Return true if acl is added successfully else
   * false.
   *
   * @param obj Ozone object for which acl should be added.
   * @param acl ozone acl top be added.
   * @throws IOException if there is error.
   */
  @Override
  public boolean addAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    Objects.requireNonNull(obj);
    Objects.requireNonNull(acl);
    if (!obj.getResourceType().equals(OzoneObj.ResourceType.BUCKET)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "BucketManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    metadataManager.getLock().acquireLock(BUCKET_LOCK, volume, bucket);
    try {
      String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
      OmBucketInfo bucketInfo =
          metadataManager.getBucketTable().get(dbBucketKey);
      if (bucketInfo == null) {
        LOG.debug("Bucket:{}/{} does not exist", volume, bucket);
        throw new OMException("Bucket " + bucket + " is not found",
            BUCKET_NOT_FOUND);
      }

      // Case 1: When we are adding more rights to existing user/group.
      boolean addToExistingAcl = false;
      for(OzoneAcl a: bucketInfo.getAcls()) {
        if(a.getName().equals(acl.getName()) &&
            a.getType().equals(acl.getType())) {
          BitSet bits = (BitSet) acl.getAclBitSet().clone();
          bits.or(a.getAclBitSet());

          if (bits.equals(a.getAclBitSet())) {
            return false;
          }
          a.getAclBitSet().or(acl.getAclBitSet());
          addToExistingAcl = true;
          break;
        }
      }

      // Case 2: When a completely new acl is added.
      if(!addToExistingAcl) {
        List<OzoneAcl> newAcls = bucketInfo.getAcls();
        if(newAcls == null) {
          newAcls = new ArrayList<>();
        }
        newAcls.add(acl);
        bucketInfo = OmBucketInfo.newBuilder()
            .setVolumeName(bucketInfo.getVolumeName())
            .setBucketName(bucketInfo.getBucketName())
            .setStorageType(bucketInfo.getStorageType())
            .setIsVersionEnabled(bucketInfo.getIsVersionEnabled())
            .setCreationTime(bucketInfo.getCreationTime())
            .setBucketEncryptionKey(bucketInfo.getEncryptionKeyInfo())
            .addAllMetadata(bucketInfo.getMetadata())
            .setAcls(newAcls)
            .build();
      }

      metadataManager.getBucketTable().put(dbBucketKey, bucketInfo);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Add acl operation failed for bucket:{}/{} acl:{}",
            volume, bucket, acl, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseLock(BUCKET_LOCK, volume, bucket);
    }

    return true;
  }

  /**
   * Remove acl for Ozone object. Return true if acl is removed successfully
   * else false.
   *
   * @param obj Ozone object.
   * @param acl Ozone acl to be removed.
   * @throws IOException if there is error.
   */
  @Override
  public boolean removeAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    Objects.requireNonNull(obj);
    Objects.requireNonNull(acl);
    if (!obj.getResourceType().equals(OzoneObj.ResourceType.BUCKET)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "BucketManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    metadataManager.getLock().acquireLock(BUCKET_LOCK, volume, bucket);
    try {
      String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
      OmBucketInfo bucketInfo =
          metadataManager.getBucketTable().get(dbBucketKey);
      if (bucketInfo == null) {
        LOG.debug("Bucket:{}/{} does not exist", volume, bucket);
        throw new OMException("Bucket " + bucket + " is not found",
            BUCKET_NOT_FOUND);
      }

      boolean removed = false;
      // When we are removing subset of rights from existing acl.
      for(OzoneAcl a: bucketInfo.getAcls()) {
        if(a.getName().equals(acl.getName()) &&
            a.getType().equals(acl.getType())) {
          BitSet bits = (BitSet) acl.getAclBitSet().clone();
          bits.and(a.getAclBitSet());

          if (bits.equals(ZERO_BITSET)) {
            return false;
          }

          a.getAclBitSet().xor(bits);

          if(a.getAclBitSet().equals(ZERO_BITSET)) {
            bucketInfo.getAcls().remove(a);
          }
          removed = true;
          break;
        }
      }

      if (removed) {
        metadataManager.getBucketTable().put(dbBucketKey, bucketInfo);
      }
      return removed;
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Remove acl operation failed for bucket:{}/{} acl:{}",
            volume, bucket, acl, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseLock(BUCKET_LOCK, volume, bucket);
    }
  }

  /**
   * Acls to be set for given Ozone object. This operations reset ACL for given
   * object to list of ACLs provided in argument.
   *
   * @param obj Ozone object.
   * @param acls List of acls.
   * @throws IOException if there is error.
   */
  @Override
  public boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException {
    Objects.requireNonNull(obj);
    Objects.requireNonNull(acls);
    if (!obj.getResourceType().equals(OzoneObj.ResourceType.BUCKET)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "BucketManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    metadataManager.getLock().acquireLock(BUCKET_LOCK, volume, bucket);
    try {
      String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
      OmBucketInfo bucketInfo =
          metadataManager.getBucketTable().get(dbBucketKey);
      if (bucketInfo == null) {
        LOG.debug("Bucket:{}/{} does not exist", volume, bucket);
        throw new OMException("Bucket " + bucket + " is not found",
            BUCKET_NOT_FOUND);
      }
      OmBucketInfo updatedBucket = OmBucketInfo.newBuilder()
          .setVolumeName(bucketInfo.getVolumeName())
          .setBucketName(bucketInfo.getBucketName())
          .setStorageType(bucketInfo.getStorageType())
          .setIsVersionEnabled(bucketInfo.getIsVersionEnabled())
          .setCreationTime(bucketInfo.getCreationTime())
          .setBucketEncryptionKey(bucketInfo.getEncryptionKeyInfo())
          .addAllMetadata(bucketInfo.getMetadata())
          .setAcls(acls)
          .build();

      metadataManager.getBucketTable().put(dbBucketKey, updatedBucket);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Set acl operation failed for bucket:{}/{} acl:{}",
            volume, bucket, StringUtils.join(",", acls), ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseLock(BUCKET_LOCK, volume, bucket);
    }

    return true;
  }

  /**
   * Returns list of ACLs for given Ozone object.
   *
   * @param obj Ozone object.
   * @throws IOException if there is error.
   */
  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    Objects.requireNonNull(obj);

    if (!obj.getResourceType().equals(OzoneObj.ResourceType.BUCKET)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "BucketManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    metadataManager.getLock().acquireLock(BUCKET_LOCK, volume, bucket);
    try {
      String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
      OmBucketInfo bucketInfo =
          metadataManager.getBucketTable().get(dbBucketKey);
      if (bucketInfo == null) {
        LOG.debug("Bucket:{}/{} does not exist", volume, bucket);
        throw new OMException("Bucket " + bucket + " is not found",
            BUCKET_NOT_FOUND);
      }
      return bucketInfo.getAcls();
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Get acl operation failed for bucket:{}/{} acl:{}",
            volume, bucket, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseLock(BUCKET_LOCK, volume, bucket);
    }
  }

  /**
   * Check access for given ozoneObject.
   *
   * @param ozObject object for which access needs to be checked.
   * @param context Context object encapsulating all user related information.
   * @return true if user has access else false.
   */
  @Override
  public boolean checkAccess(OzoneObj ozObject, RequestContext context)
      throws OMException {
    Objects.requireNonNull(ozObject);
    Objects.requireNonNull(context);

    String volume = ozObject.getVolumeName();
    String bucket = ozObject.getBucketName();
    metadataManager.getLock().acquireLock(BUCKET_LOCK, volume, bucket);
    try {
      String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
      OmBucketInfo bucketInfo =
          metadataManager.getBucketTable().get(dbBucketKey);
      if (bucketInfo == null) {
        LOG.debug("Bucket:{}/{} does not exist", volume, bucket);
        throw new OMException("Bucket " + bucket + " is not found",
            BUCKET_NOT_FOUND);
      }
      boolean hasAccess = OzoneUtils.checkAclRights(bucketInfo.getAcls(),
          context);
      LOG.debug("user:{} has access rights for bucket:{} :{} ",
          context.getClientUgi(), ozObject.getBucketName(), hasAccess);
      return hasAccess;
    } catch (IOException ex) {
      if(ex instanceof OMException) {
        throw (OMException) ex;
      }
      LOG.error("CheckAccess operation failed for bucket:{}/{} acl:{}",
          volume, bucket, ex);
      throw new OMException("Check access operation failed for " +
          "bucket:" + bucket, ex, INTERNAL_ERROR);
    } finally {
      metadataManager.getLock().releaseLock(BUCKET_LOCK, volume, bucket); 
    }
  }
}
