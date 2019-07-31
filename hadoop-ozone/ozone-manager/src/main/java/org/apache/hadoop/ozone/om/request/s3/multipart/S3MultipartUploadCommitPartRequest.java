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

package org.apache.hadoop.ozone.om.request.s3.multipart;

import com.google.common.base.Optional;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart
    .S3MultipartUploadCommitPartResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartCommitUploadPartRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartCommitUploadPartResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handle Multipart upload commit upload part file.
 */
public class S3MultipartUploadCommitPartRequest extends OMKeyRequest {


  private static final Logger LOG =
      LoggerFactory.getLogger(S3MultipartUploadCommitPartRequest.class);

  public S3MultipartUploadCommitPartRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) {
    MultipartCommitUploadPartRequest multipartCommitUploadPartRequest =
        getOmRequest().getCommitMultiPartUploadRequest();

    return getOmRequest().toBuilder().setCommitMultiPartUploadRequest(
        multipartCommitUploadPartRequest.toBuilder()
            .setKeyArgs(multipartCommitUploadPartRequest.getKeyArgs()
                .toBuilder().setModificationTime(Time.now())))
        .setUserInfo(getUserInfo()).build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    MultipartCommitUploadPartRequest multipartCommitUploadPartRequest =
        getOmRequest().getCommitMultiPartUploadRequest();

    OzoneManagerProtocolProtos.KeyArgs keyArgs =
        multipartCommitUploadPartRequest.getKeyArgs();

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    ozoneManager.getMetrics().incNumCommitMultipartUploadParts();

    boolean acquiredLock = false;

    IOException exception = null;
    String partName = null;
    OMResponse.Builder omResponse = OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CommitMultiPartUpload)
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setSuccess(true);
    OMClientResponse omClientResponse = null;
    OzoneManagerProtocolProtos.PartKeyInfo oldPartKeyInfo = null;
    String openKey = null;
    OmKeyInfo omKeyInfo = null;
    String multipartKey = null;
    OmMultipartKeyInfo multipartKeyInfo = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.KEY,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE,
            volumeName, bucketName, keyName);
      }

      acquiredLock =
          omMetadataManager.getLock().acquireLock(BUCKET_LOCK, volumeName,
              bucketName);

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      String uploadID = keyArgs.getMultipartUploadID();
      multipartKey = omMetadataManager.getMultipartKey(volumeName,
          bucketName, keyName, uploadID);

      multipartKeyInfo =
          omMetadataManager.getMultipartInfoTable().get(multipartKey);

      long clientID = multipartCommitUploadPartRequest.getClientID();

      openKey = omMetadataManager.getOpenKey(
          volumeName, bucketName, keyName, clientID);

      omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

      if (omKeyInfo == null) {
        throw new OMException("Failed to commit Multipart Upload key, as " +
            openKey + "entry is not found in the openKey table", KEY_NOT_FOUND);
      }

      // set the data size and location info list
      omKeyInfo.setDataSize(keyArgs.getDataSize());
      omKeyInfo.updateLocationInfoList(keyArgs.getKeyLocationsList().stream()
          .map(OmKeyLocationInfo::getFromProtobuf)
          .collect(Collectors.toList()));
      // Set Modification time
      omKeyInfo.setModificationTime(keyArgs.getModificationTime());

      partName = omMetadataManager.getOzoneKey(volumeName, bucketName,
          keyName) + clientID;

      if (multipartKeyInfo == null) {
        // This can occur when user started uploading part by the time commit
        // of that part happens, in between the user might have requested
        // abort multipart upload. If we just throw exception, then the data
        // will not be garbage collected, so move this part to delete table
        // and throw error
        // Move this part to delete table.
        throw new OMException("No such Multipart upload is with specified " +
            "uploadId " + uploadID,
            OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
      } else {
        int partNumber = keyArgs.getMultipartNumber();
        oldPartKeyInfo = multipartKeyInfo.getPartKeyInfo(partNumber);

        // Build this multipart upload part info.
        OzoneManagerProtocolProtos.PartKeyInfo.Builder partKeyInfo =
            OzoneManagerProtocolProtos.PartKeyInfo.newBuilder();
        partKeyInfo.setPartName(partName);
        partKeyInfo.setPartNumber(partNumber);
        partKeyInfo.setPartKeyInfo(omKeyInfo.getProtobuf());

        // Add this part information in to multipartKeyInfo.
        multipartKeyInfo.addPartKeyInfo(partNumber, partKeyInfo.build());

        // Add to cache.

        // Delete from open key table and add it to multipart info table.
        // No need to add cache entries to delete table, as no
        // read/write requests that info for validation.
        omMetadataManager.getMultipartInfoTable().addCacheEntry(
            new CacheKey<>(multipartKey),
            new CacheValue<>(Optional.of(multipartKeyInfo),
                transactionLogIndex));

        omMetadataManager.getOpenKeyTable().addCacheEntry(
            new CacheKey<>(openKey),
            new CacheValue<>(Optional.absent(), transactionLogIndex));
      }

      omResponse.setCommitMultiPartUploadResponse(
          MultipartCommitUploadPartResponse.newBuilder().setPartName(partName));
      omClientResponse = new S3MultipartUploadCommitPartResponse(multipartKey,
        openKey, keyArgs.getModificationTime(), omKeyInfo, multipartKeyInfo,
          oldPartKeyInfo, omResponse.build());

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new S3MultipartUploadCommitPartResponse(multipartKey,
          openKey, keyArgs.getModificationTime(), omKeyInfo, multipartKeyInfo,
          oldPartKeyInfo, createErrorOMResponse(omResponse, exception));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
      if (acquiredLock) {
        omMetadataManager.getLock().releaseLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    // audit log
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.COMMIT_MULTIPART_UPLOAD_PARTKEY, buildKeyArgsAuditMap(keyArgs),
        exception, getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.debug("MultipartUpload Commit is successfully for Key:{} in " +
          "Volume/Bucket {}/{}", keyName, volumeName, bucketName);

    } else {
      LOG.error("MultipartUpload Commit is failed for Key:{} in " +
          "Volume/Bucket {}/{}", keyName, volumeName, bucketName, exception);
      ozoneManager.getMetrics().incNumCommitMultipartUploadPartFails();
    }
    return omClientResponse;
  }
}

