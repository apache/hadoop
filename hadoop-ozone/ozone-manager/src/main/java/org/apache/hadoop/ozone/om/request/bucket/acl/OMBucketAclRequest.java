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
import java.util.List;

import com.google.common.base.Optional;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.util.BooleanBiFunction;
import org.apache.hadoop.ozone.om.request.util.ObjectParser;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneObj.ObjectType;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Base class for Bucket acl request.
 */
public abstract class OMBucketAclRequest extends OMClientRequest {

  private BooleanBiFunction<List<OzoneAcl>, OmBucketInfo> omBucketAclOp;

  public OMBucketAclRequest(OMRequest omRequest,
      BooleanBiFunction<List<OzoneAcl>, OmBucketInfo> aclOp) {
    super(omRequest);
    omBucketAclOp = aclOp;
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    // protobuf guarantees acls are non-null.
    List<OzoneAcl> ozoneAcls = getAcls();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumBucketUpdates();
    OmBucketInfo omBucketInfo = null;

    OMResponse.Builder omResponse = onInit();
    OMClientResponse omClientResponse = null;
    IOException exception = null;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean lockAcquired = false;
    String volume = null;
    String bucket = null;
    boolean operationResult = false;
    try {
      ObjectParser objectParser = new ObjectParser(getPath(),
          ObjectType.BUCKET);

      volume = objectParser.getVolume();
      bucket = objectParser.getBucket();

      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
            volume, null, null);
      }
      lockAcquired =
          omMetadataManager.getLock().acquireLock(BUCKET_LOCK, volume, bucket);

      String dbBucketKey = omMetadataManager.getBucketKey(volume, bucket);
      omBucketInfo = omMetadataManager.getBucketTable().get(dbBucketKey);
      if (omBucketInfo == null) {
        throw new OMException(OMException.ResultCodes.BUCKET_NOT_FOUND);
      }

      operationResult = omBucketAclOp.apply(ozoneAcls, omBucketInfo);

      if (operationResult) {
        // update cache.
        omMetadataManager.getBucketTable().addCacheEntry(
            new CacheKey<>(dbBucketKey),
            new CacheValue<>(Optional.of(omBucketInfo), transactionLogIndex));
      }

      omClientResponse = onSuccess(omResponse, omBucketInfo, operationResult);

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = onFailure(omResponse, ex);
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
      if (lockAcquired) {
        omMetadataManager.getLock().releaseLock(BUCKET_LOCK, volume, bucket);
      }
    }


    onComplete(operationResult, exception, ozoneManager.getMetrics());

    return omClientResponse;
  }

  /**
   * Get the Acls from the request.
   * @return List of OzoneAcls, for add/remove it is a single element list
   * for set it can be non-single element list.
   */
  abstract List<OzoneAcl> getAcls();

  /**
   * Get the path name from the request.
   * @return path name
   */
  abstract String getPath();

  // TODO: Finer grain metrics can be moved to these callbacks. They can also
  // be abstracted into separate interfaces in future.
  /**
   * Get the initial om response builder with lock.
   * @return om response builder.
   */
  abstract OMResponse.Builder onInit();

  /**
   * Get the om client response on success case with lock.
   * @param omResponse
   * @param omBucketInfo
   * @param operationResult
   * @return OMClientResponse
   */
  abstract OMClientResponse onSuccess(
      OMResponse.Builder omResponse, OmBucketInfo omBucketInfo,
      boolean operationResult);

  /**
   * Get the om client response on failure case with lock.
   * @param omResponse
   * @param exception
   * @return OMClientResponse
   */
  abstract OMClientResponse onFailure(OMResponse.Builder omResponse,
      IOException exception);

  /**
   * Completion hook for final processing before return without lock.
   * Usually used for logging without lock and metric update.
   * @param operationResult
   * @param exception
   * @param omMetrics
   */
  abstract void onComplete(boolean operationResult, IOException exception,
      OMMetrics omMetrics);


}

