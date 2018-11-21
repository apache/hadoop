/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.exception;


import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.ozone.s3.util.S3Consts.RANGE_NOT_SATISFIABLE;

/**
 * This class represents errors from Ozone S3 service.
 * This class needs to be updated to add new errors when required.
 */
public final class S3ErrorTable {

  private S3ErrorTable() {
    //No one should construct this object.
  }

  public static final OS3Exception INVALID_URI = new OS3Exception("InvalidURI",
      "Couldn't parse the specified URI.", HTTP_BAD_REQUEST);

  public static final OS3Exception NO_SUCH_VOLUME = new OS3Exception(
      "NoSuchVolume", "The specified volume does not exist", HTTP_NOT_FOUND);

  public static final OS3Exception NO_SUCH_BUCKET = new OS3Exception(
      "NoSuchBucket", "The specified bucket does not exist", HTTP_NOT_FOUND);

  public static final OS3Exception BUCKET_NOT_EMPTY = new OS3Exception(
      "BucketNotEmpty", "The bucket you tried to delete is not empty.",
      HTTP_CONFLICT);

  public static final OS3Exception MALFORMED_HEADER = new OS3Exception(
      "AuthorizationHeaderMalformed", "The authorization header you provided " +
      "is invalid.", HTTP_NOT_FOUND);

  public static final OS3Exception NO_SUCH_KEY = new OS3Exception(
      "NoSuchKey", "The specified key does not exist", HTTP_NOT_FOUND);

  public static final OS3Exception INVALID_ARGUMENT = new OS3Exception(
      "InvalidArgument", "Invalid Argument", HTTP_BAD_REQUEST);

  public static final OS3Exception INVALID_REQUEST = new OS3Exception(
      "InvalidRequest", "Invalid Request", HTTP_BAD_REQUEST);

  public static final OS3Exception INVALID_RANGE = new OS3Exception(
      "InvalidRange", "The requested range is not satisfiable",
      RANGE_NOT_SATISFIABLE);

  /**
   * Create a new instance of Error.
   * @param e Error Template
   * @param resource Resource associated with this exception
   * @return creates a new instance of error based on the template
   */
  public static OS3Exception newError(OS3Exception e, String resource) {
    OS3Exception err =  new OS3Exception(e.getCode(), e.getErrorMessage(),
        e.getHttpCode());
    err.setResource(resource);
    return err;
  }
}
