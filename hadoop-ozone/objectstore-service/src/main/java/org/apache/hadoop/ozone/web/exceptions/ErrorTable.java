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

package org.apache.hadoop.ozone.web.exceptions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.handlers.UserArgs;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

/**
 * Error Table represents the Errors from Ozone Rest API layer.
 *
 * Please note : The errors in this table are sorted by the HTTP_ERROR codes
 * if you add new error codes to this table please follow the same convention.
 */
@InterfaceAudience.Private
public final class ErrorTable {

  /* Error 400 */
  public static final OzoneException MISSING_VERSION =
      new OzoneException(HTTP_BAD_REQUEST, "missingVersion",
                         "x-ozone-version header is required.");

  public static final OzoneException MISSING_DATE =
      new OzoneException(HTTP_BAD_REQUEST, "missingDate",
                         "Date header is required.");

  public static final OzoneException BAD_DATE =
      new OzoneException(HTTP_BAD_REQUEST, "badDate",
                         "Unable to parse date format.");

  public static final OzoneException MALFORMED_QUOTA =
      new OzoneException(HTTP_BAD_REQUEST, "malformedQuota",
                         "Invalid quota specified.");

  public static final OzoneException MALFORMED_ACL =
      new OzoneException(HTTP_BAD_REQUEST, "malformedACL",
                         "Invalid ACL specified.");


  public static final OzoneException INVALID_VOLUME_NAME =
      new OzoneException(HTTP_BAD_REQUEST, "invalidVolumeName",
                         "Invalid volume name.");

  public static final OzoneException INVALID_QUERY_PARAM =
      new OzoneException(HTTP_BAD_REQUEST, "invalidQueryParam",
                         "Invalid query parameter.");

  public static final OzoneException INVALID_RESOURCE_NAME =
      new OzoneException(HTTP_BAD_REQUEST, "invalidResourceName",
                         "Invalid volume, bucket or key name.");

  public static final OzoneException INVALID_BUCKET_NAME =
      new OzoneException(HTTP_BAD_REQUEST, "invalidBucketName",
                         "Invalid bucket name.");

  public static final OzoneException INVALID_KEY =
      new OzoneException(HTTP_BAD_REQUEST, "invalidKey", "Invalid key.");

  public static final OzoneException INVALID_REQUEST =
      new OzoneException(HTTP_BAD_REQUEST, "invalidRequest",
                         "Error in request.");

  public static final OzoneException MALFORMED_BUCKET_VERSION =
      new OzoneException(HTTP_BAD_REQUEST, "malformedBucketVersion",
                         "Malformed bucket version or version not unique.");

  public static final OzoneException MALFORMED_STORAGE_TYPE =
      new OzoneException(HTTP_BAD_REQUEST, "malformedStorageType",
                         "Invalid storage Type specified.");

  public static final OzoneException MALFORMED_STORAGE_CLASS =
      new OzoneException(HTTP_BAD_REQUEST, "malformedStorageClass",
                         "Invalid storage class specified.");

  public static final OzoneException BAD_DIGEST =
      new OzoneException(HTTP_BAD_REQUEST, "badDigest",
                         "Content MD5 does not match.");

  public static final OzoneException INCOMPLETE_BODY =
      new OzoneException(HTTP_BAD_REQUEST, "incompleteBody",
                         "Content length does not match stream size.");

  public static final OzoneException BAD_AUTHORIZATION =
      new OzoneException(HTTP_BAD_REQUEST, "badAuthorization",
                         "Missing authorization or authorization has to be " +
                             "unique.");

  public static final OzoneException BAD_PROPERTY =
      new OzoneException(HTTP_BAD_REQUEST, "unknownProperty",
          "This property is not supported by this server.");

  /* Error 401 */
  public static final OzoneException UNAUTHORIZED =
      new OzoneException(HTTP_UNAUTHORIZED, "Unauthorized",
                         "Access token is missing or invalid token.");

  /* Error 403 */
  public static final OzoneException ACCESS_DENIED =
      new OzoneException(HTTP_FORBIDDEN, "accessDenied", "Access denied.");

  /* Error 404 */
  public static final OzoneException USER_NOT_FOUND =
      new OzoneException(HTTP_NOT_FOUND, "userNotFound", "Invalid user name.");

  public static final OzoneException VOLUME_NOT_FOUND =
      new OzoneException(HTTP_NOT_FOUND, "volumeNotFound", "No such volume.");

  /* Error 409 */
  public static final OzoneException VOLUME_ALREADY_EXISTS =
      new OzoneException(HTTP_CONFLICT, "volumeAlreadyExists",
                         "Duplicate volume name.");

  public static final OzoneException BUCKET_ALREADY_EXISTS =
      new OzoneException(HTTP_CONFLICT, "bucketAlreadyExists",
                         "Duplicate bucket name.");

  public static final OzoneException VOLUME_NOT_EMPTY =
      new OzoneException(HTTP_CONFLICT, "volumeNotEmpty",
                         "Volume must not have any buckets.");

  public static final OzoneException BUCKET_NOT_EMPTY =
      new OzoneException(HTTP_CONFLICT, "bucketNotEmpty",
                         "Bucket must not have any keys.");

  public static final OzoneException KEY_OPERATION_CONFLICT =
      new OzoneException(HTTP_CONFLICT, "keyOperationConflict",
                         "Conflicting operation on the specified key is going" +
                             " on.");

  /* Error 500 */
  public static final OzoneException SERVER_ERROR =
      new OzoneException(HTTP_INTERNAL_ERROR, "internalServerError",
                         "Internal server error.");

  /**
   * Create a new instance of Error.
   *
   * @param e Error Template
   * @param requestID Request ID
   * @param resource Resource Name
   * @param hostID hostID
   *
   * @return creates a new instance of error based on the template
   */
  public static OzoneException newError(OzoneException e, String requestID,
                                        String resource, String hostID) {
    OzoneException err =
        new OzoneException(e.getHttpCode(), e.getShortMessage(),
                           e.getMessage());
    err.setRequestId(requestID);
    err.setResource(resource);
    err.setHostID(hostID);
    return err;
  }

  /**
   * Create new instance of Error.
   *
   * @param e - Error Template
   * @param args - Args
   *
   * @return Ozone Exception
   */
  public static OzoneException newError(OzoneException e, UserArgs args) {
    OzoneException err =
        new OzoneException(e.getHttpCode(), e.getShortMessage(),
                           e.getMessage());
    if (args != null) {
      err.setRequestId(args.getRequestID());
      err.setResource(args.getResourceName());
      err.setHostID(args.getHostName());
    }
    return err;
  }

  /**
   * Create new instance of Error.
   *
   * @param e - Error Template
   * @param args - Args
   * @param ex Exception
   *
   * @return Ozone Exception
   */
  public static OzoneException newError(OzoneException e, UserArgs args,
                                        Exception ex) {
    OzoneException err =
        new OzoneException(e.getHttpCode(), e.getShortMessage(), ex);

    if(args != null) {
      err.setRequestId(args.getRequestID());
      err.setResource(args.getResourceName());
      err.setHostID(args.getHostName());
    }
    err.setMessage(ex.getMessage());
    return err;
  }

  private ErrorTable() {
    // Never constructed.
  }
}
