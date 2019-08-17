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
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ozone.OzoneRestUtils;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.web.exceptions.ErrorTable;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.interfaces.UserAuth;
import org.apache.hadoop.ozone.web.response.ListBuckets;
import org.apache.hadoop.ozone.web.response.ListVolumes;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_COMPONENT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_REQUEST;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_RESOURCE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_USER;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;


/**
 * This class abstracts way the repetitive tasks in
 * handling volume related code.
 */
@InterfaceAudience.Private
public abstract class VolumeProcessTemplate {
  private static final Logger LOG =
      LoggerFactory.getLogger(VolumeProcessTemplate.class);


  /**
   * The handle call is the common functionality for Volume
   * handling code.
   *
   * @param volume - Name of the Volume
   * @param request - request
   * @param info - UriInfo
   * @param headers - Http Headers
   *
   * @return Response
   *
   * @throws OzoneException
   */
  public Response handleCall(String volume, Request request, UriInfo info,
                             HttpHeaders headers) throws OzoneException {
    String reqID = OzoneUtils.getRequestID();
    String hostName = OzoneUtils.getHostName();
    MDC.put(OZONE_COMPONENT, "ozone");
    MDC.put(OZONE_REQUEST, reqID);
    UserArgs userArgs  = null;
    try {
      userArgs = new UserArgs(reqID, hostName, request, info, headers);
      OzoneRestUtils.validate(request, headers, reqID, volume, hostName);

      // we use the same logic for both bucket and volume names
      OzoneUtils.verifyResourceName(volume);
      UserAuth auth = UserHandlerBuilder.getAuthHandler();

      userArgs.setUserName(auth.getUser(userArgs));
      MDC.put(OZONE_USER, userArgs.getUserName());
      VolumeArgs args = new VolumeArgs(volume, userArgs);

      MDC.put(OZONE_RESOURCE, args.getResourceName());
      Response response =  doProcess(args);
      LOG.info("Success");
      MDC.clear();
      return response;

    } catch (IllegalArgumentException ex) {
      LOG.error("Illegal argument.", ex);
      throw ErrorTable.newError(ErrorTable.INVALID_VOLUME_NAME, userArgs, ex);
    } catch (IOException ex) {
      handleIOException(volume, reqID, hostName, ex);
    }
    return null;
  }

  /**
   * Specific handler for each call.
   *
   * @param args - Volume Args
   *
   * @return - Response
   *
   * @throws IOException
   * @throws OzoneException
   */
  public abstract Response doProcess(VolumeArgs args)
      throws IOException, OzoneException;

  /**
   * Maps Java File System Exceptions to Ozone Exceptions in the Volume path.
   *
   * @param volume - Name of the Volume
   * @param reqID - Request ID
   * @param hostName - HostName
   * @param fsExp - Exception
   *
   * @throws OzoneException
   */
  private void handleIOException(String volume, String reqID, String hostName,
                                 IOException fsExp) throws OzoneException {
    LOG.error("IOException:", fsExp);
    OzoneException exp = null;

    if ((fsExp != null && fsExp.getMessage().endsWith(
        OzoneManagerProtocolProtos.Status.VOLUME_ALREADY_EXISTS.name()))
        || fsExp instanceof FileAlreadyExistsException) {
      exp = ErrorTable
          .newError(ErrorTable.VOLUME_ALREADY_EXISTS, reqID, volume, hostName);
    }

    if (fsExp instanceof DirectoryNotEmptyException) {
      exp = ErrorTable
          .newError(ErrorTable.VOLUME_NOT_EMPTY, reqID, volume, hostName);
    }

    if (fsExp instanceof NoSuchFileException) {
      exp = ErrorTable
          .newError(ErrorTable.INVALID_VOLUME_NAME, reqID, volume, hostName);
    }

    if ((fsExp != null) && (exp != null)) {
      exp.setMessage(fsExp.getMessage());
    }

    // We don't handle that FS error yet, report a Server Internal Error
    if (exp == null) {
      exp =
          ErrorTable.newError(ErrorTable.SERVER_ERROR, reqID, volume, hostName);
      if (fsExp != null) {
        exp.setMessage(fsExp.getMessage());
      }
    }
    throw exp;
  }

  /**
   * Set the user provided string into args and throw ozone exception
   * if needed.
   *
   * @param args - volume args
   * @param quota - quota sting
   *
   * @throws OzoneException
   */
  void setQuotaArgs(VolumeArgs args, String quota) throws OzoneException {
    try {
      args.setQuota(quota);
    } catch (IllegalArgumentException ex) {
      LOG.debug("Malformed Quota.", ex);
      throw ErrorTable.newError(ErrorTable.MALFORMED_QUOTA, args, ex);
    }
  }

  /**
   * Wraps calls into volumeInfo data.
   *
   * @param args - volumeArgs
   *
   * @return - VolumeInfo
   *
   * @throws IOException
   * @throws OzoneException
   */
  Response getVolumeInfoResponse(VolumeArgs args)
      throws IOException, OzoneException {
    StorageHandler fs = StorageHandlerBuilder.getStorageHandler();
    VolumeInfo info = fs.getVolumeInfo(args);
    return OzoneRestUtils.getResponse(args, HTTP_OK, info.toJsonString());
  }

  /**
   * Returns all the volumes belonging to a user.
   *
   * @param user - userArgs
   * @return - Response
   * @throws OzoneException
   * @throws IOException
   */
  Response getVolumesByUser(UserArgs user, String prefix, int maxKeys,
      String prevKey, boolean rootScan) throws OzoneException, IOException {

    String validatedUser = user.getUserName();
    try {
      UserAuth auth = UserHandlerBuilder.getAuthHandler();
      if(rootScan && !auth.isAdmin(user)) {
        throw ErrorTable.newError(ErrorTable.UNAUTHORIZED, user);
      }
      if (auth.isAdmin(user)) {
        validatedUser = auth.getOzoneUser(user);
        if (validatedUser == null) {
          validatedUser = auth.getUser(user);
        }
      }

      UserArgs onBehalfOf =
          new UserArgs(validatedUser, user.getRequestID(), user.getHostName(),
              user.getRequest(), user.getUri(), user.getHeaders());

      StorageHandler fs = StorageHandlerBuilder.getStorageHandler();
      ListArgs<UserArgs> listArgs = new ListArgs<>(onBehalfOf, prefix,
          maxKeys, prevKey);
      listArgs.setRootScan(rootScan);
      ListVolumes volumes = fs.listVolumes(listArgs);
      return OzoneRestUtils.getResponse(user, HTTP_OK, volumes.toJsonString());
    } catch (IOException ex) {
      LOG.debug("unable to get the volume list for the user.", ex);
      OzoneException exp = ErrorTable.newError(ErrorTable.SERVER_ERROR,
          user, ex);
      exp.setMessage("unable to get the volume list for the user");
      throw exp;
    }
  }

  /**
   * Returns a list of Buckets in a Volume.
   *
   * @param args    - VolumeArgs
   * @param prefix  - Prefix to Match
   * @param maxKeys - Max results to return.
   * @param prevKey - PrevKey
   * @return List of Buckets
   * @throws OzoneException
   */
  Response getBucketsInVolume(VolumeArgs args, String prefix, int maxKeys,
                              String prevKey) throws OzoneException {
    try {
      // UserAuth auth = UserHandlerBuilder.getAuthHandler();
      // TODO : Check ACLS.
      StorageHandler fs = StorageHandlerBuilder.getStorageHandler();
      ListArgs<VolumeArgs> listArgs = new ListArgs<>(args, prefix,
          maxKeys, prevKey);
      ListBuckets bucketList = fs.listBuckets(listArgs);
      return OzoneRestUtils
          .getResponse(args, HTTP_OK, bucketList.toJsonString());
    } catch (IOException ex) {
      LOG.debug("unable to get the bucket list for the specified volume.", ex);
      OzoneException exp =
          ErrorTable.newError(ErrorTable.SERVER_ERROR, args, ex);
      exp.setMessage("unable to get the bucket list for the specified volume.");
      throw exp;
    }
  }
}
