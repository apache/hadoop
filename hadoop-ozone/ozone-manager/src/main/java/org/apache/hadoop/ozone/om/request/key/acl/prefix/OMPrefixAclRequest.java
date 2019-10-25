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

package org.apache.hadoop.ozone.om.request.key.acl.prefix;

import java.io.IOException;

import com.google.common.base.Optional;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.PrefixManagerImpl;
import org.apache.hadoop.ozone.om.PrefixManagerImpl.OMPrefixAclOpResult;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.PREFIX_LOCK;

/**
 * Base class for Prefix acl request.
 */
public abstract class OMPrefixAclRequest extends OMClientRequest {

  public OMPrefixAclRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {


    OmPrefixInfo omPrefixInfo = null;

    OMResponse.Builder omResponse = onInit();
    OMClientResponse omClientResponse = null;
    IOException exception = null;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean lockAcquired = false;
    String volume = null;
    String bucket = null;
    String key = null;
    OMPrefixAclOpResult operationResult = null;
    boolean result = false;

    PrefixManagerImpl prefixManager =
        (PrefixManagerImpl) ozoneManager.getPrefixManager();
    try {
      String prefixPath = getOzoneObj().getPath();

      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.PREFIX,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
            volume, bucket, key);
      }

      lockAcquired =
          omMetadataManager.getLock().acquireWriteLock(PREFIX_LOCK, prefixPath);

      omPrefixInfo = omMetadataManager.getPrefixTable().get(prefixPath);

      try {
        operationResult = apply(prefixManager, omPrefixInfo);
      } catch (IOException ex) {
        // In HA case this will never happen.
        // As in add/remove/setAcl method we have logic to update database,
        // that can throw exception. But in HA case we shall not update DB.
        // The code in prefixManagerImpl is being done, because update
        // in-memory should be done after DB update for Non-HA code path.
        operationResult = new OMPrefixAclOpResult(null, false);
      }

      if (operationResult.isOperationsResult()) {
        // As for remove acl list, for a prefix if after removing acl from
        // the existing acl list, if list size becomes zero, delete the
        // prefix from prefix table.
        if (getOmRequest().hasRemoveAclRequest() &&
            operationResult.getOmPrefixInfo().getAcls().size() == 0) {
          omMetadataManager.getPrefixTable().addCacheEntry(
              new CacheKey<>(prefixPath),
              new CacheValue<>(Optional.absent(), transactionLogIndex));
        } else {
          // update cache.
          omMetadataManager.getPrefixTable().addCacheEntry(
              new CacheKey<>(prefixPath),
              new CacheValue<>(Optional.of(operationResult.getOmPrefixInfo()),
                  transactionLogIndex));
        }
      }

      result  = operationResult.isOperationsResult();
      omClientResponse = onSuccess(omResponse,
          operationResult.getOmPrefixInfo(), result);

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
        omMetadataManager.getLock().releaseWriteLock(PREFIX_LOCK,
            getOzoneObj().getPath());
      }
    }

    onComplete(result, exception, ozoneManager.getMetrics());

    return omClientResponse;
  }

  /**
   * Get the path name from the request.
   * @return path name
   */
  abstract OzoneObj getOzoneObj();

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
   * @param omPrefixInfo
   * @param operationResult
   * @return OMClientResponse
   */
  abstract OMClientResponse onSuccess(
      OMResponse.Builder omResponse, OmPrefixInfo omPrefixInfo,
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

  /**
   * Apply the acl operation, if successfully completed returns true,
   * else false.
   * @param prefixManager
   * @param omPrefixInfo
   * @throws IOException
   */
  abstract OMPrefixAclOpResult apply(PrefixManagerImpl prefixManager,
      OmPrefixInfo omPrefixInfo) throws IOException;


}

