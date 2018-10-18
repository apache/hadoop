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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.web.handlers;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ozone.OzoneRestUtils;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.client.rest.headers.Header;
import org.apache.hadoop.ozone.web.exceptions.ErrorTable;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.interfaces.UserAuth;
import org.apache.hadoop.ozone.web.interfaces.Volume;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_FUNCTION;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * VolumeHandler handles volume specific HTTP calls.
 *
 * Most functions in this file follow a simple pattern.
 * All calls are handled by VolumeProcessTemplate.handleCall, which
 * calls back into doProcess function.
 *
 * Everything common to volume handling is abstracted out in handleCall function
 * For Example : Checking that volume name is sane, we have a supported
 * ozone version number and a valid date. That is everything common is in
 * handleCall and actions specific to a call is inside doProcess callback.
 */
@InterfaceAudience.Private
public class VolumeHandler implements Volume {
  private static final Logger LOG = LoggerFactory.getLogger(VolumeHandler
      .class);
  /**
   * Creates a volume.
   *
   * @param volume Volume Name, this has to be unique at Ozone cluster level
   * @param quota Quota for this Storage Volume
   *             - {@literal <int>(<BYTES|MB|GB|TB>)}
   * @param req Request Object
   * @param uriInfo URI info
   * @param headers Http Headers
   *
   * @return Standard JAX-RS Response
   *
   * @throws OzoneException
   */
  @Override
  public Response createVolume(String volume, final String quota, Request req,
                               UriInfo uriInfo, HttpHeaders headers)
      throws OzoneException {
    MDC.put(OZONE_FUNCTION, "createVolume");
    return new VolumeProcessTemplate() {
      @Override
      public Response doProcess(VolumeArgs args)
          throws IOException, OzoneException {
        UserAuth auth = UserHandlerBuilder.getAuthHandler();
        if (auth.isAdmin(args)) {
          args.setAdminName(args.getUserName());
          String volumeOwner = auth.getOzoneUser(args);

          if (volumeOwner == null) {
            throw ErrorTable.newError(ErrorTable.USER_NOT_FOUND, args);
          }

          if (!auth.isUser(volumeOwner, args)) {
            throw ErrorTable.newError(ErrorTable.USER_NOT_FOUND, args);
          }

          args.setUserName(volumeOwner);
          args.setGroups(auth.getGroups(args));
          if (!quota.equals(Header.OZONE_QUOTA_UNDEFINED)) {
            setQuotaArgs(args, quota);
          }
          StorageHandler fs = StorageHandlerBuilder.getStorageHandler();
          fs.createVolume(args);
          return OzoneRestUtils.getResponse(args, HTTP_CREATED, "");
        } else {
          throw ErrorTable.newError(ErrorTable.ACCESS_DENIED, args);
        }
      }
    }.handleCall(volume, req, uriInfo, headers);
  }

  /**
   * Updates  volume metadata.
   *
   * There are only two actions possible currently with updateVolume.
   * Change the volume ownership or update quota. if you make a call
   * with neither of these actions, update just returns 200 OK.
   *
   * @param volume Volume Name, this has to be unique at Ozone Level
   * @param quota Quota for this volume
   *             - {@literal <int>(<BYTES|MB|GB|TB>)}|remove
   * @param req - Request Object
   * @param uriInfo - URI info
   * @param headers Http Headers
   *
   * @return Standard JAX-RS Response
   *
   * @throws OzoneException
   */
  @Override
  public Response updateVolume(String volume, final String quota, Request req,
                               UriInfo uriInfo, HttpHeaders headers)
      throws OzoneException {
    MDC.put(OZONE_FUNCTION, "updateVolume");
    return new VolumeProcessTemplate() {
      @Override
      public Response doProcess(VolumeArgs args)
          throws IOException, OzoneException {
        UserAuth auth = UserHandlerBuilder.getAuthHandler();
        if (auth.isAdmin(args)) {
          StorageHandler fs = StorageHandlerBuilder.getStorageHandler();
          args.setAdminName(args.getUserName());
          String newVolumeOwner = auth.getOzoneUser(args);

          if (newVolumeOwner != null) {
            if (!auth.isUser(newVolumeOwner, args)) {
              throw ErrorTable.newError(ErrorTable.USER_NOT_FOUND, args);
            }
            args.setUserName(newVolumeOwner);
            fs.setVolumeOwner(args);
          }

          if (!quota.equals(Header.OZONE_QUOTA_UNDEFINED)) {
            if (quota.equals(Header.OZONE_QUOTA_REMOVE)) {
              // if it is remove, just tell the file system to remove quota
              fs.setVolumeQuota(args, true);
            } else {
              setQuotaArgs(args, quota);
              fs.setVolumeQuota(args, false);
            }
          }
          return OzoneRestUtils.getResponse(args, HTTP_OK, "");
        } else {
          // Only Admins are allowed to update volumes
          throw ErrorTable.newError(ErrorTable.ACCESS_DENIED, args);
        }
      }
    }.handleCall(volume, req, uriInfo, headers);
  }


  /**
   * Deletes a volume if it is empty.
   *
   * @param volume Volume Name
   * @param req - Http Request
   * @param uriInfo - http URI
   * @param headers - http headers
   *
   * @return Standard JAX-RS Response
   *
   * @throws OzoneException
   */
  @Override
  public Response deleteVolume(String volume, Request req, UriInfo uriInfo,
                               HttpHeaders headers) throws OzoneException {
    MDC.put(OZONE_FUNCTION, "deleteVolume");

    return new VolumeProcessTemplate() {
      @Override
      public Response doProcess(VolumeArgs args)
          throws IOException, OzoneException {
        UserAuth auth = UserHandlerBuilder.getAuthHandler();
        if (auth.isAdmin(args)) {
          StorageHandler fs = StorageHandlerBuilder.getStorageHandler();
          fs.deleteVolume(args);
          return OzoneRestUtils.getResponse(args, HTTP_OK, "");
        } else {
          throw ErrorTable.newError(ErrorTable.ACCESS_DENIED, args);
        }
      }
    }.handleCall(volume, req, uriInfo, headers);
  }

  /**
   * Returns Volume info. This API can be invoked either by admin or the owner
   *
   * @param volume  - Storage Volume Name
   * @param info    - Info attribute
   * @param prefix  - Prefix key
   * @param maxKeys - Max results
   * @param prevKey - PrevKey
   * @param req     - Http Req
   * @param uriInfo - UriInfo.
   * @param headers - Http headers
   * @return
   * @throws OzoneException
   */
  @Override
  public Response getVolumeInfo(String volume, final String info,
                                final String prefix,
                                final int maxKeys,
                                final String prevKey,
                                final boolean rootScan,
                                Request req,
                                final UriInfo uriInfo, HttpHeaders headers)
      throws OzoneException {

    return new VolumeProcessTemplate() {
      @Override
      public Response doProcess(VolumeArgs args)
          throws IOException, OzoneException {

        switch (info) {
        case Header.OZONE_INFO_QUERY_BUCKET:
          MDC.put(OZONE_FUNCTION, "ListBucket");
          return getBucketsInVolume(args, prefix, maxKeys, prevKey);
        case Header.OZONE_INFO_QUERY_VOLUME:
          MDC.put(OZONE_FUNCTION, "InfoVolume");
          assertNoListParamPresent(uriInfo, args);
          return getVolumeInfoResponse(args); // Return volume info
        case Header.OZONE_LIST_QUERY_SERVICE:
          MDC.put(OZONE_FUNCTION, "ListVolume");
          return getVolumesByUser(args, prefix, maxKeys, prevKey, rootScan);
        default:
          LOG.debug("Unrecognized query param : {} ", info);
          OzoneException ozoneException =
              ErrorTable.newError(ErrorTable.INVALID_QUERY_PARAM, args);
          ozoneException.setMessage("Unrecognized query param : " + info);
          throw ozoneException;
        }
      }
    }.handleCall(volume, req, uriInfo, headers);
  }

  /**
   * Asserts no list query param is present during this call.
   *
   * @param uriInfo - UriInfo.   - UriInfo
   * @param args    - Volume Args - VolumeArgs.
   * @throws OzoneException
   */
  private void assertNoListParamPresent(final UriInfo uriInfo, VolumeArgs
      args) throws
      OzoneException {

    String prefix = uriInfo.getQueryParameters().getFirst("prefix");
    String maxKeys = uriInfo.getQueryParameters().getFirst("max_keys");
    String prevKey = uriInfo.getQueryParameters().getFirst("prev_key");
    if ((prefix != null && !prefix.equals(Header.OZONE_EMPTY_STRING)) ||
        (maxKeys != null && !maxKeys.equals(Header.OZONE_DEFAULT_LIST_SIZE)) ||
        (prevKey != null && !prevKey.equals(Header.OZONE_EMPTY_STRING))) {
      throw ErrorTable.newError(ErrorTable.INVALID_QUERY_PARAM, args);
    }
  }
}
