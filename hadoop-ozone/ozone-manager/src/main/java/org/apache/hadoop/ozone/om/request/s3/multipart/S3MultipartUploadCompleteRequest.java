package org.apache.hadoop.ozone.om.request.s3.multipart;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Optional;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3MultipartUploadCompleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartUploadCompleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .PartKeyInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.OzoneConsts.OM_MULTIPART_MIN_SIZE;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handle Multipart upload complete request.
 */
public class S3MultipartUploadCompleteRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3MultipartUploadCompleteRequest.class);

  public S3MultipartUploadCompleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    MultipartUploadCompleteRequest multipartUploadCompleteRequest =
        getOmRequest().getCompleteMultiPartUploadRequest();

    KeyArgs keyArgs = multipartUploadCompleteRequest.getKeyArgs();

    return getOmRequest().toBuilder()
        .setCompleteMultiPartUploadRequest(multipartUploadCompleteRequest
            .toBuilder().setKeyArgs(keyArgs.toBuilder()
                .setModificationTime(Time.now())))
        .setUserInfo(getUserInfo()).build();

  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    MultipartUploadCompleteRequest multipartUploadCompleteRequest =
        getOmRequest().getCompleteMultiPartUploadRequest();

    KeyArgs keyArgs = multipartUploadCompleteRequest.getKeyArgs();

    List<OzoneManagerProtocolProtos.Part> partsList =
        multipartUploadCompleteRequest.getPartsListList();

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();
    String uploadID = keyArgs.getMultipartUploadID();

    ozoneManager.getMetrics().incNumCompleteMultipartUploads();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    boolean acquiredLock = false;
    OMResponse.Builder omResponse = OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CommitMultiPartUpload)
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setSuccess(true);
    OMClientResponse omClientResponse = null;
    IOException exception = null;
    OmMultipartUploadList multipartUploadList = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.KEY,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE,
            volumeName, bucketName, keyName);
      }

      TreeMap<Integer, String> partsMap = new TreeMap<>();
      for (OzoneManagerProtocolProtos.Part part : partsList) {
        partsMap.put(part.getPartNumber(), part.getPartName());
      }

      multipartUploadList = new OmMultipartUploadList(partsMap);

      acquiredLock = omMetadataManager.getLock().acquireLock(BUCKET_LOCK,
          volumeName, bucketName);

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      String multipartKey = omMetadataManager.getMultipartKey(volumeName,
          bucketName, keyName, uploadID);
      String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
          keyName);
      OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);

      OmMultipartKeyInfo multipartKeyInfo = omMetadataManager
          .getMultipartInfoTable().get(multipartKey);

      if (multipartKeyInfo == null) {
        throw new OMException("Complete Multipart Upload Failed: volume: " +
            volumeName + "bucket: " + bucketName + "key: " + keyName,
            OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
      }
      TreeMap<Integer, PartKeyInfo> partKeyInfoMap =
          multipartKeyInfo.getPartKeyInfoMap();

      TreeMap<Integer, String> multipartMap = multipartUploadList
          .getMultipartMap();

      // Last key in the map should be having key value as size, as map's
      // are sorted. Last entry in both maps should have partNumber as size
      // of the map. As we have part entries 1, 2, 3, 4 and then we get
      // complete multipart upload request so the map last entry should have 4,
      // if it is having value greater or less than map size, then there is
      // some thing wrong throw error.

      Map.Entry<Integer, String> multipartMapLastEntry = multipartMap
          .lastEntry();
      Map.Entry<Integer, PartKeyInfo> partKeyInfoLastEntry =
          partKeyInfoMap.lastEntry();
      if (partKeyInfoMap.size() != multipartMap.size()) {
        throw new OMException("Complete Multipart Upload Failed: volume: " +
            volumeName + "bucket: " + bucketName + "key: " + keyName,
            OMException.ResultCodes.MISMATCH_MULTIPART_LIST);
      }

      // Last entry part Number should be the size of the map, otherwise this
      // means we have missing some parts but we got a complete request.
      if (multipartMapLastEntry.getKey() != partKeyInfoMap.size() ||
          partKeyInfoLastEntry.getKey() != partKeyInfoMap.size()) {
        throw new OMException("Complete Multipart Upload Failed: volume: " +
            volumeName + "bucket: " + bucketName + "key: " + keyName,
            OMException.ResultCodes.MISSING_UPLOAD_PARTS);
      }
      HddsProtos.ReplicationType type = partKeyInfoLastEntry.getValue()
          .getPartKeyInfo().getType();
      HddsProtos.ReplicationFactor factor = partKeyInfoLastEntry.getValue()
          .getPartKeyInfo().getFactor();
      List< OmKeyLocationInfo > locations = new ArrayList<>();
      long size = 0;
      int partsCount =1;
      int partsMapSize = partKeyInfoMap.size();
      for(Map.Entry<Integer, PartKeyInfo > partKeyInfoEntry : partKeyInfoMap
          .entrySet()) {
        int partNumber = partKeyInfoEntry.getKey();
        PartKeyInfo partKeyInfo = partKeyInfoEntry.getValue();
        // Check we have all parts to complete multipart upload and also
        // check partNames provided match with actual part names
        String providedPartName = multipartMap.get(partNumber);
        String actualPartName = partKeyInfo.getPartName();
        if (partNumber == partsCount) {
          if (!actualPartName.equals(providedPartName)) {
            throw new OMException("Complete Multipart Upload Failed: volume: " +
                volumeName + "bucket: " + bucketName + "key: " + keyName,
                OMException.ResultCodes.MISMATCH_MULTIPART_LIST);
          }
          OmKeyInfo currentPartKeyInfo = OmKeyInfo
              .getFromProtobuf(partKeyInfo.getPartKeyInfo());
          // Check if any part size is less than 5mb, last part can be less
          // than 5 mb.
          if (partsCount != partsMapSize &&
              currentPartKeyInfo.getDataSize() < OM_MULTIPART_MIN_SIZE) {
            LOG.error("MultipartUpload: " + ozoneKey + "Part number: " +
                partKeyInfo.getPartNumber() + "size " + currentPartKeyInfo
                .getDataSize() + " is less than minimum part size " +
                OzoneConsts.OM_MULTIPART_MIN_SIZE);
            throw new OMException("Complete Multipart Upload Failed: Entity " +
                "too small: volume: " + volumeName + "bucket: " + bucketName
                + "key: " + keyName, OMException.ResultCodes.ENTITY_TOO_SMALL);
          }
          // As all part keys will have only one version.
          OmKeyLocationInfoGroup currentKeyInfoGroup = currentPartKeyInfo
              .getKeyLocationVersions().get(0);
          locations.addAll(currentKeyInfoGroup.getLocationList());
          size += currentPartKeyInfo.getDataSize();
        } else {
          throw new OMException("Complete Multipart Upload Failed: volume: " +
              volumeName + "bucket: " + bucketName + "key: " + keyName,
              OMException.ResultCodes.MISSING_UPLOAD_PARTS);
        }
        partsCount++;
      }
      if (omKeyInfo == null) {
        // This is a newly added key, it does not have any versions.
        OmKeyLocationInfoGroup keyLocationInfoGroup = new
            OmKeyLocationInfoGroup(0, locations);
        // A newly created key, this is the first version.
        omKeyInfo = new OmKeyInfo.Builder().setVolumeName(volumeName)
            .setBucketName(bucketName).setKeyName(keyName)
            .setReplicationFactor(factor).setReplicationType(type)
            .setCreationTime(keyArgs.getModificationTime())
            .setModificationTime(keyArgs.getModificationTime())
            .setDataSize(size)
            .setOmKeyLocationInfos(
                Collections.singletonList(keyLocationInfoGroup))
            .setAcls(keyArgs.getAclsList()).build();
      } else {
        // Already a version exists, so we should add it as a new version.
        // But now as versioning is not supported, just following the commit
        // key approach. When versioning support comes, then we can uncomment
        // below code keyInfo.addNewVersion(locations);
        omKeyInfo.updateLocationInfoList(locations);
        omKeyInfo.setModificationTime(keyArgs.getModificationTime());
      }

      updateCache(omMetadataManager, ozoneKey, multipartKey, omKeyInfo,
          transactionLogIndex);

      omResponse.setCompleteMultiPartUploadResponse(
          MultipartUploadCompleteResponse.newBuilder()
              .setVolume(volumeName)
              .setBucket(bucketName)
              .setKey(keyName)
              .setHash(DigestUtils.sha256Hex(keyName)));

      omClientResponse = new S3MultipartUploadCompleteResponse(multipartKey,
          omKeyInfo, omResponse.build());

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new S3MultipartUploadCompleteResponse(null, null,
          createErrorOMResponse(omResponse, exception));
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

    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
    if (multipartUploadList != null) {
      auditMap.put(OzoneConsts.MULTIPART_LIST, multipartUploadList
          .getMultipartMap().toString());
    }

    // audit log
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.COMPLETE_MULTIPART_UPLOAD, auditMap, exception,
        getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.debug("MultipartUpload Complete request is successfull for Key: {} " +
          "in Volume/Bucket {}/{}", keyName, volumeName, bucketName);
    } else {
      LOG.error("MultipartUpload Complete request failed for Key: {} " +
          "in Volume/Bucket {}/{}", keyName, volumeName, bucketName, exception);
      ozoneManager.getMetrics().incNumCompleteMultipartUploadFails();
    }

    return omClientResponse;
  }

  private void updateCache(OMMetadataManager omMetadataManager,
      String ozoneKey, String multipartKey, OmKeyInfo omKeyInfo,
      long transactionLogIndex) {
    // Update cache.
    // 1. Add key entry to key table.
    // 2. Delete multipartKey entry from openKeyTable and multipartInfo table.
    omMetadataManager.getKeyTable().addCacheEntry(
        new CacheKey<>(ozoneKey),
        new CacheValue<>(Optional.of(omKeyInfo), transactionLogIndex));

    omMetadataManager.getOpenKeyTable().addCacheEntry(
        new CacheKey<>(multipartKey),
        new CacheValue<>(Optional.absent(), transactionLogIndex));
    omMetadataManager.getMultipartInfoTable().addCacheEntry(
        new CacheKey<>(multipartKey),
        new CacheValue<>(Optional.absent(), transactionLogIndex));
  }
}

