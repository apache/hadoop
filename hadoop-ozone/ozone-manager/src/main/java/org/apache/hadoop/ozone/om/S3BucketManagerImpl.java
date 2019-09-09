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
package org.apache.hadoop.ozone.om;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_ALREADY_EXISTS;

import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.apache.hadoop.ozone.OzoneConsts.OM_S3_VOLUME_PREFIX;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.S3_BUCKET_LOCK;

/**
 * S3 Bucket Manager, this class maintains a mapping between S3 Bucket and Ozone
 * Volume/bucket.
 */
public class S3BucketManagerImpl implements S3BucketManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(S3BucketManagerImpl.class);

  private static final String S3_ADMIN_NAME = "OzoneS3Manager";
  private final OzoneConfiguration configuration;
  private final OMMetadataManager omMetadataManager;
  private final VolumeManager volumeManager;
  private final BucketManager bucketManager;

  /**
   * Construct an S3 Bucket Manager Object.
   *
   * @param configuration - Ozone Configuration.
   * @param omMetadataManager - Ozone Metadata Manager.
   */
  public S3BucketManagerImpl(
      OzoneConfiguration configuration,
      OMMetadataManager omMetadataManager,
      VolumeManager volumeManager,
      BucketManager bucketManager) {
    this.configuration = configuration;
    this.omMetadataManager = omMetadataManager;
    this.volumeManager = volumeManager;
    this.bucketManager = bucketManager;
  }

  @Override
  public void createS3Bucket(String userName, String bucketName)
      throws IOException {
    Preconditions.checkArgument(Strings.isNotBlank(bucketName), "Bucket" +
        " name cannot be null or empty.");
    Preconditions.checkArgument(Strings.isNotBlank(userName), "User name " +
        "cannot be null or empty.");

    Preconditions.checkArgument(bucketName.length() >=3 &&
        bucketName.length() < 64, "Length of the S3 Bucket is not correct.");


    // TODO: Decide if we want to enforce S3 Bucket Creation Rules in this
    // code path?
    // https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html

    // Generate an Ozone volume name. For the time being, we are going to use
    // s3userName as the Ozone volume name. Since S3 advices 100 buckets max
    // for a user and we have no limit to the number of Ozone buckets under a
    // volume we will stick to very simple model.
    //
    // s3Bucket -> ozoneVolume/OzoneBucket name
    // s3BucketName ->s3userName/s3Bucketname
    //
    // You might wonder if all names map to this pattern, why we need to
    // store the S3 bucketName in a table at all. This is to support
    // anonymous access to bucket where the user name is absent.
    String ozoneVolumeName = formatOzoneVolumeName(userName);

    omMetadataManager.getLock().acquireLock(S3_BUCKET_LOCK, bucketName);
    try {
      String bucket = omMetadataManager.getS3Table().get(bucketName);

      if (bucket != null) {
        LOG.debug("Bucket already exists. {}", bucketName);
        throw new OMException(
            "Unable to create S3 bucket. " + bucketName + " already exists.",
            OMException.ResultCodes.S3_BUCKET_ALREADY_EXISTS);
      }
      String ozoneBucketName = bucketName;
      createOzoneBucket(ozoneVolumeName, ozoneBucketName);
      String finalName = String.format("%s/%s", ozoneVolumeName,
          ozoneBucketName);

      omMetadataManager.getS3Table().put(bucketName, finalName);
    } finally {
      omMetadataManager.getLock().releaseLock(S3_BUCKET_LOCK, bucketName);
    }

  }

  @Override
  public void deleteS3Bucket(String bucketName) throws IOException {
    Preconditions.checkArgument(
        Strings.isNotBlank(bucketName), "Bucket name cannot be null or empty");

    omMetadataManager.getLock().acquireLock(S3_BUCKET_LOCK, bucketName);
    try {
      String map = omMetadataManager.getS3Table().get(bucketName);

      if (map == null) {
        throw new OMException("No such S3 bucket. " + bucketName,
            OMException.ResultCodes.S3_BUCKET_NOT_FOUND);
      }

      bucketManager.deleteBucket(getOzoneVolumeName(bucketName), bucketName);
      omMetadataManager.getS3Table().delete(bucketName);
    } catch(IOException ex) {
      throw ex;
    } finally {
      omMetadataManager.getLock().releaseLock(S3_BUCKET_LOCK, bucketName);
    }

  }

  @Override
  public String formatOzoneVolumeName(String userName) {
    return String.format(OM_S3_VOLUME_PREFIX + "%s", userName);
  }

  @Override
  public boolean createOzoneVolumeIfNeeded(String userName)
      throws IOException {
    // We don't have to time of check. time of use problem here because
    // this call is invoked while holding the s3Bucket lock.
    boolean newVolumeCreate = true;
    String ozoneVolumeName = formatOzoneVolumeName(userName);
    try {
      OmVolumeArgs.Builder builder =
          OmVolumeArgs.newBuilder()
              .setAdminName(S3_ADMIN_NAME)
              .setOwnerName(userName)
              .setVolume(ozoneVolumeName)
              .setQuotaInBytes(OzoneConsts.MAX_QUOTA_IN_BYTES);
      for (OzoneAcl acl : getDefaultAcls(userName)) {
        builder.addOzoneAcls(OzoneAcl.toProtobuf(acl));
      }

      OmVolumeArgs args = builder.build();

      volumeManager.createVolume(args);

    } catch (OMException exp) {
      newVolumeCreate = false;
      if (exp.getResult().compareTo(VOLUME_ALREADY_EXISTS) == 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Volume already exists. {}", exp.getMessage());
        }
      } else {
        throw exp;
      }
    }

    return newVolumeCreate;
  }

  /**
   * Get default acls. 
   * */
  private List<OzoneAcl> getDefaultAcls(String userName) {
    UserGroupInformation ugi = ProtobufRpcEngine.Server.getRemoteUser();
    return OzoneAcl.parseAcls("user:" + (ugi == null ? userName :
        ugi.getUserName()) + ":a,user:" + S3_ADMIN_NAME + ":a");
  }

  private void createOzoneBucket(String volumeName, String bucketName)
      throws IOException {
    OmBucketInfo.Builder builder = OmBucketInfo.newBuilder();
    OmBucketInfo bucketInfo =
        builder
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setIsVersionEnabled(Boolean.FALSE)
            .setStorageType(StorageType.DEFAULT)
            .setAcls(getDefaultAcls(null))
            .build();
    bucketManager.createBucket(bucketInfo);
  }

  @Override
  public String getOzoneBucketMapping(String s3BucketName) throws IOException {
    Preconditions.checkArgument(
        Strings.isNotBlank(s3BucketName),
        "Bucket name cannot be null or empty.");
    Preconditions.checkArgument(s3BucketName.length() >=3 &&
        s3BucketName.length() < 64,
        "Length of the S3 Bucket is not correct.");
    omMetadataManager.getLock().acquireLock(S3_BUCKET_LOCK, s3BucketName);
    try {
      String mapping = omMetadataManager.getS3Table().get(s3BucketName);
      if (mapping != null) {
        return mapping;
      }
      throw new OMException("No such S3 bucket.",
          OMException.ResultCodes.S3_BUCKET_NOT_FOUND);
    } finally {
      omMetadataManager.getLock().releaseLock(S3_BUCKET_LOCK, s3BucketName);
    }
  }

  @Override
  public String getOzoneVolumeName(String s3BucketName) throws IOException {
    String mapping = getOzoneBucketMapping(s3BucketName);
    return mapping.split("/")[0];
  }

  @Override
  public String getOzoneBucketName(String s3BucketName) throws IOException {
    String mapping = getOzoneBucketMapping(s3BucketName);
    return mapping.split("/")[1];
  }

  @Override
  public String getOzoneVolumeNameForUser(String userName) throws IOException {
    Objects.requireNonNull(userName, "UserName cannot be null");
    return formatOzoneVolumeName(userName);
  }

}
