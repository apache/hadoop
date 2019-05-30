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

package org.apache.hadoop.ozone.om.request.bucket;

import java.io.IOException;

import com.google.common.base.Optional;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketCreateResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .BucketEncryptionInfoProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .BucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CryptoProtocolVersionProto.ENCRYPTION_ZONES;

/**
 * Handles CreateBucket Request.
 */
public class OMBucketCreateRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMBucketCreateRequest.class);

  public OMBucketCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }
  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    // Get original request.
    CreateBucketRequest createBucketRequest =
        getOmRequest().getCreateBucketRequest();
    BucketInfo bucketInfo = createBucketRequest.getBucketInfo();

    // Get KMS provider.
    KeyProviderCryptoExtension kmsProvider =
        ozoneManager.getKmsProvider();

    // Create new Bucket request with new bucket info.
    CreateBucketRequest.Builder newCreateBucketRequest =
        createBucketRequest.toBuilder();

    BucketInfo.Builder newBucketInfo = bucketInfo.toBuilder();

    // Set creation time.
    newBucketInfo.setCreationTime(Time.now());

    if (bucketInfo.hasBeinfo()) {
      newBucketInfo.setBeinfo(getBeinfo(kmsProvider, bucketInfo));
    }

    newCreateBucketRequest.setBucketInfo(newBucketInfo.build());
    return getOmRequest().toBuilder().setCreateBucketRequest(
        newCreateBucketRequest.build()).build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex) {
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumBucketCreates();

    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();

    BucketInfo bucketInfo = getBucketInfoFromRequest();

    String volumeName = bucketInfo.getVolumeName();
    String bucketName = bucketInfo.getBucketName();

    OMResponse.Builder omResponse = OMResponse.newBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.CreateBucket).setStatus(
        OzoneManagerProtocolProtos.Status.OK);
    OmBucketInfo omBucketInfo = null;


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

      omBucketInfo = OmBucketInfo.getFromProtobuf(bucketInfo);
      LOG.debug("created bucket: {} in volume: {}", bucketName, volumeName);
      omMetrics.incNumBuckets();

      // Update table cache.
      metadataManager.getBucketTable().addCacheEntry(new CacheKey<>(bucketKey),
          new CacheValue<>(Optional.of(omBucketInfo), transactionLogIndex));

      // TODO: check acls.
    } catch (IOException ex) {
      omMetrics.incNumBucketCreateFails();
      LOG.error("Bucket creation failed for bucket:{} in volume:{}",
          bucketName, volumeName, ex);
      omResponse.setStatus(
          OzoneManagerRatisUtils.exceptionToResponseStatus(ex));
      omResponse.setMessage(ex.getMessage());
      omResponse.setSuccess(false);
    } finally {
      metadataManager.getLock().releaseBucketLock(volumeName, bucketName);
      metadataManager.getLock().releaseVolumeLock(volumeName);
    }
    omResponse.setCreateBucketResponse(
        CreateBucketResponse.newBuilder().build());
    return new OMBucketCreateResponse(omBucketInfo, omResponse.build());
  }


  private BucketInfo getBucketInfoFromRequest() {
    CreateBucketRequest createBucketRequest =
        getOmRequest().getCreateBucketRequest();
    return createBucketRequest.getBucketInfo();
  }

  private BucketEncryptionInfoProto getBeinfo(
      KeyProviderCryptoExtension kmsProvider, BucketInfo bucketInfo)
      throws IOException {
    BucketEncryptionInfoProto bek = bucketInfo.getBeinfo();
    BucketEncryptionInfoProto.Builder bekb = null;
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
    KeyProvider.Metadata metadata = kmsProvider.getMetadata(
        bek.getKeyName());
    if (metadata == null) {
      throw new OMException("Bucket encryption key " + bek.getKeyName()
          + " doesn't exist.",
          OMException.ResultCodes.BUCKET_ENCRYPTION_KEY_NOT_FOUND);
    }
    // If the provider supports pool for EDEKs, this will fill in the pool
    kmsProvider.warmUpEncryptedKeys(bek.getKeyName());
    bekb = BucketEncryptionInfoProto.newBuilder()
        .setKeyName(bek.getKeyName())
        .setCryptoProtocolVersion(ENCRYPTION_ZONES)
        .setSuite(OMPBHelper.convert(
            CipherSuite.convert(metadata.getCipher())));
    return bekb.build();
  }
}
