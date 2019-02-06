/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.hadoop.ozone.web.handlers;

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneRestUtils;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.client.rest.headers.Header;
import org.apache.hadoop.ozone.web.exceptions.ErrorTable;
import org.apache.hadoop.ozone.web.interfaces.Bucket;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.slf4j.MDC;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_FUNCTION;


/**
 * Bucket Class handles all ozone Bucket related actions.
 */
public class BucketHandler implements Bucket {
  /**
   * createBucket call handles the POST request for Creating a Bucket.
   *
   * @param volume - Volume name
   * @param bucket - Bucket Name
   * @param req - Http request
   * @param info - Uri Info
   * @param headers - Http headers
   *
   * @return Response
   *
   * @throws OzoneException
   */
  @Override
  public Response createBucket(String volume, String bucket, Request req,
                               UriInfo info, HttpHeaders headers)
      throws OzoneException {
    MDC.put(OZONE_FUNCTION, "createBucket");
    return new BucketProcessTemplate() {
      @Override
      public Response doProcess(BucketArgs args)
          throws OzoneException, IOException {
        StorageHandler fs = StorageHandlerBuilder.getStorageHandler();
        getAclsFromHeaders(args, false);
        args.setVersioning(getVersioning(args));
        args.setStorageType(getStorageType(args));
        fs.createBucket(args);
        return OzoneRestUtils.getResponse(args, HTTP_CREATED, "");
      }
    }.handleCall(volume, bucket, req, info, headers);
  }

  /**
   * updateBucket call handles the PUT request for updating a Bucket.
   *
   * There are only three possible actions currently with updateBucket.
   * They are add/remove on ACLS, Bucket Versioning and  StorageType.
   *  if you make a call with any other action, update just returns 200 OK.
   *
   * @param volume - Storage volume name
   * @param bucket - Bucket name
   * @param req - Http request
   * @param info - Uri Info
   * @param headers - Http headers
   *
   * @return Response
   *
   * @throws OzoneException
   */
  @Override
  public Response updateBucket(String volume, String bucket, Request req,
                               UriInfo info, HttpHeaders headers)
      throws OzoneException {
    MDC.put(OZONE_FUNCTION, "updateBucket");
    return new BucketProcessTemplate() {
      @Override
      public Response doProcess(BucketArgs args)
          throws OzoneException, IOException {
        StorageHandler fs = StorageHandlerBuilder.getStorageHandler();
        getAclsFromHeaders(args, true);
        args.setVersioning(getVersioning(args));
        args.setStorageType(getStorageType(args));

        if ((args.getAddAcls() != null) || (args.getRemoveAcls() != null)) {
          fs.setBucketAcls(args);
        }

        if (args.getVersioning() != OzoneConsts.Versioning.NOT_DEFINED) {
          fs.setBucketVersioning(args);
        }

        if (args.getStorageType() != null) {
          fs.setBucketStorageClass(args);
        }
        return OzoneRestUtils.getResponse(args, HTTP_OK, "");
      }
    }.handleCall(volume, bucket, req, info, headers);
  }

  /**
   * Deletes an empty bucket.
   *
   * @param volume Volume name
   * @param bucket Bucket Name
   * @param req - Http request
   * @param info - Uri Info
   * @param headers - Http headers
   *
   * @return Response
   *
   * @throws OzoneException
   */
  @Override
  public Response deleteBucket(String volume, String bucket, Request req,
                               UriInfo info, HttpHeaders headers)
      throws OzoneException {
    MDC.put(OZONE_FUNCTION, "deleteBucket");
    return new BucketProcessTemplate() {
      @Override
      public Response doProcess(BucketArgs args)
          throws OzoneException, IOException {
        StorageHandler fs = StorageHandlerBuilder.getStorageHandler();
        fs.deleteBucket(args);
        return OzoneRestUtils.getResponse(args, HTTP_OK, "");
      }
    }.handleCall(volume, bucket, req, info, headers);
  }

  /**
   * List Buckets allows the user to list the bucket.
   *
   * @param volume - Storage Volume Name
   * @param bucket - Bucket Name
   * @param info - Uri Info
   * @param prefix - Prefix for the keys to be fetched
   * @param maxKeys - MaxNumber of Keys to Return
   * @param startPage - Continuation Token
   * @param req - Http request
   * @param headers - Http headers
   *
   * @return - Json Body
   *
   * @throws OzoneException
   */
  @Override
  public Response listBucket(String volume, String bucket, final String info,
                             final String prefix, final int maxKeys,
                             final String startPage, Request req,
                             UriInfo uriInfo, HttpHeaders headers)
      throws OzoneException {
    MDC.put(OZONE_FUNCTION, "listBucket");
    return new BucketProcessTemplate() {
      @Override
      public Response doProcess(BucketArgs args)
          throws OzoneException, IOException {
        switch (info) {
        case Header.OZONE_INFO_QUERY_KEY:
          ListArgs listArgs = new ListArgs(args, prefix, maxKeys, startPage);
          return getBucketKeysList(listArgs);
        case Header.OZONE_INFO_QUERY_BUCKET:
          return getBucketInfoResponse(args);
        default:
          OzoneException ozException =
              ErrorTable.newError(ErrorTable.INVALID_QUERY_PARAM, args);
          ozException.setMessage("Unrecognized query param : " + info);
          throw ozException;
        }
      }
    }.handleCall(volume, bucket, req, uriInfo, headers);
  }
}
