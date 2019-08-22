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

package org.apache.hadoop.ozone.om.request.key;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMAllocateBlockResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .AllocateBlockResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;


import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes
    .KEY_NOT_FOUND;

/**
 * Handles allocate block request.
 */
public class OMAllocateBlockRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMAllocateBlockRequest.class);

  public OMAllocateBlockRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    AllocateBlockRequest allocateBlockRequest =
        getOmRequest().getAllocateBlockRequest();

    Preconditions.checkNotNull(allocateBlockRequest);

    KeyArgs keyArgs = allocateBlockRequest.getKeyArgs();

    ExcludeList excludeList = new ExcludeList();
    if (allocateBlockRequest.hasExcludeList()) {
      excludeList =
          ExcludeList.getFromProtoBuf(allocateBlockRequest.getExcludeList());
    }

    // TODO: Here we are allocating block with out any check for key exist in
    //  open table or not and also with out any authorization checks.
    //  Assumption here is that allocateBlocks with out openKey will be less.
    //  There is a chance some one can misuse this api to flood allocateBlock
    //  calls. But currently allocateBlock is internally called from
    //  BlockOutputStreamEntryPool, so we are fine for now. But if one some
    //  one uses direct omclient we might be in trouble.


    // To allocate atleast one block passing requested size and scmBlockSize
    // as same value. When allocating block requested size is same as
    // scmBlockSize.
    List<OmKeyLocationInfo> omKeyLocationInfoList =
        allocateBlock(ozoneManager.getScmClient(),
            ozoneManager.getBlockTokenSecretManager(), keyArgs.getType(),
            keyArgs.getFactor(), excludeList, ozoneManager.getScmBlockSize(),
            ozoneManager.getScmBlockSize(),
            ozoneManager.getPreallocateBlocksMax(),
            ozoneManager.isGrpcBlockTokenEnabled(), ozoneManager.getOMNodeId());

    // Set modification time
    KeyArgs.Builder newKeyArgs = keyArgs.toBuilder()
        .setModificationTime(Time.now());

    AllocateBlockRequest.Builder newAllocatedBlockRequest =
        AllocateBlockRequest.newBuilder()
            .setClientID(allocateBlockRequest.getClientID())
            .setKeyArgs(newKeyArgs);



    if (allocateBlockRequest.hasExcludeList()) {
      newAllocatedBlockRequest.setExcludeList(
          allocateBlockRequest.getExcludeList());
    }

    // Add allocated block info.
    newAllocatedBlockRequest.setKeyLocation(
        omKeyLocationInfoList.get(0).getProtobuf());

    return getOmRequest().toBuilder().setUserInfo(getUserInfo())
        .setAllocateBlockRequest(newAllocatedBlockRequest).build();

  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    OzoneManagerProtocolProtos.AllocateBlockRequest allocateBlockRequest =
        getOmRequest().getAllocateBlockRequest();

    OzoneManagerProtocolProtos.KeyArgs keyArgs =
        allocateBlockRequest.getKeyArgs();

    OzoneManagerProtocolProtos.KeyLocation blockLocation =
        allocateBlockRequest.getKeyLocation();
    Preconditions.checkNotNull(blockLocation);

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();
    long clientID = allocateBlockRequest.getClientID();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumBlockAllocateCalls();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
    auditMap.put(OzoneConsts.CLIENT_ID, String.valueOf(clientID));

    OMResponse.Builder omResponse = OMResponse.newBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.AllocateBlock).setStatus(
        OzoneManagerProtocolProtos.Status.OK).setSuccess(true);

    IOException exception = null;
    OmKeyInfo omKeyInfo = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.KEY,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE,
            volumeName, bucketName, keyName);
      }

      OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
      validateBucketAndVolume(omMetadataManager, volumeName,
          bucketName);

      String openKey = omMetadataManager.getOpenKey(
          volumeName, bucketName, keyName, clientID);

      // Here we don't acquire bucket/volume lock because for a single client
      // allocateBlock is called in serial fashion.

      omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);
      if (omKeyInfo == null) {
        throw new OMException("Open Key not found " + openKey, KEY_NOT_FOUND);
      }

      // Append new block
      omKeyInfo.appendNewBlocks(Collections.singletonList(
          OmKeyLocationInfo.getFromProtobuf(blockLocation)), false);

      // Set modification time.
      omKeyInfo.setModificationTime(keyArgs.getModificationTime());

      // Add to cache.
      omMetadataManager.getOpenKeyTable().addCacheEntry(
          new CacheKey<>(openKey), new CacheValue<>(Optional.of(omKeyInfo),
              transactionLogIndex));

    } catch (IOException ex) {
      exception = ex;
    }

    auditLog(auditLogger, buildAuditMessage(OMAction.ALLOCATE_BLOCK, auditMap,
        exception, getOmRequest().getUserInfo()));

    OMClientResponse omClientResponse = null;
    if (exception == null) {
      omResponse.setAllocateBlockResponse(AllocateBlockResponse.newBuilder()
          .setKeyLocation(blockLocation).build());
      omClientResponse = new OMAllocateBlockResponse(omKeyInfo,
          clientID, omResponse.build());
    } else {
      omMetrics.incNumBlockAllocateCallFails();
      omClientResponse = new OMAllocateBlockResponse(null, -1L,
          createErrorOMResponse(omResponse, exception));
    }

    omClientResponse.setFlushFuture(
        ozoneManagerDoubleBufferHelper.add(omClientResponse,
            transactionLogIndex));
    return omClientResponse;

  }

}
