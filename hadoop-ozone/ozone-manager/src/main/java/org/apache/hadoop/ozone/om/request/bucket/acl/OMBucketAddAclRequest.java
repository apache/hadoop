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

package org.apache.hadoop.ozone.om.request.bucket.acl;

import java.io.IOException;

import com.google.common.base.Optional;
import org.apache.hadoop.ozone.om.request.util.ObjectParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.acl.OMBucketAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .AddAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .AddAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handle add Acl request for bucket.
 */
public class OMBucketAddAclRequest extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMBucketAddAclRequest.class);

  public OMBucketAddAclRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    AddAclRequest addAclRequest = getOmRequest().getAddAclRequest();

    OMResponse.Builder omResponse = OMResponse.newBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.AddAcl).setStatus(
        OzoneManagerProtocolProtos.Status.OK).setSuccess(true)
        .setAddAclResponse(AddAclResponse.newBuilder()
            .setResponse(true).build());

    OzoneAcl ozoneAcl = OzoneAcl.fromProtobuf(addAclRequest.getAcl());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    ozoneManager.getMetrics().incNumBucketUpdates();

    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    IOException exception = null;
    String volume = null;
    String bucket = null;
    try {
      ObjectParser objectParser =
          new ObjectParser(addAclRequest.getObj().getPath(),
              addAclRequest.getObj().getResType());

      volume= objectParser.getVolume();
      bucket = objectParser.getBucket();

      acquiredLock = omMetadataManager.getLock().acquireLock(BUCKET_LOCK,
          volume, bucket);

      String dbBucketKey = omMetadataManager.getBucketKey(volume, bucket);

      OmBucketInfo omBucketInfo =
          omMetadataManager.getBucketTable().get(dbBucketKey);

      if (omBucketInfo == null) {
        throw new OMException("Bucket " + bucket + " is not found",
            BUCKET_NOT_FOUND);
      }

      boolean addAcl = omBucketInfo.addAcl(ozoneAcl);
      omResponse.setAddAclResponse(AddAclResponse.newBuilder()
          .setResponse(addAcl).build());

      if (addAcl) {
        omMetadataManager.getBucketTable().addCacheEntry(
            new CacheKey<>(dbBucketKey),
            new CacheValue<>(Optional.of(omBucketInfo), transactionLogIndex));
      }

      omClientResponse = new OMBucketAclResponse(omBucketInfo,
          omResponse.build());

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMBucketAclResponse(null,
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(ozoneManagerDoubleBufferHelper.add(
            omClientResponse, transactionLogIndex));
      }
      if (acquiredLock) {
        omMetadataManager.getLock().releaseLock(BUCKET_LOCK, volume, bucket);
      }
    }

    // TODO: audit log
    if (!omResponse.getAddAclResponse().getResponse()) {
      LOG.warn("AddAcl is called to add an already exisiting ACL {} for " +
          "bucket {} in volume {} ", ozoneAcl, bucket, volume);
    }
    if (exception == null) {
      LOG.debug("AddAcl for bucket {} in volume {} is successful", bucket,
          volume);
    } else {
      ozoneManager.getMetrics().incNumBucketUpdateFails();
      LOG.error("AddAcl for bucket {} in volume {} failed", bucket, volume,
          exception);
    }

    return omClientResponse;
  }
}
