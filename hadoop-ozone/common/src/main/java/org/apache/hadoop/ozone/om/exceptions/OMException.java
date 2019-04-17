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
package org.apache.hadoop.ozone.om.exceptions;

import java.io.IOException;

/**
 * Exception thrown by Ozone Manager.
 */
public class OMException extends IOException {

  public static final String STATUS_CODE = "STATUS_CODE=";
  private final OMException.ResultCodes result;

  /**
   * Constructs an {@code IOException} with {@code null}
   * as its error detail message.
   */
  public OMException(OMException.ResultCodes result) {
    this.result = result;
  }

  /**
   * Constructs an {@code IOException} with the specified detail message.
   *
   * @param message The detail message (which is saved for later retrieval by
   * the
   * {@link #getMessage()} method)
   */
  public OMException(String message, OMException.ResultCodes result) {
    super(message);
    this.result = result;
  }

  /**
   * Constructs an {@code IOException} with the specified detail message
   * and cause.
   * <p>
   * <p> Note that the detail message associated with {@code cause} is
   * <i>not</i> automatically incorporated into this exception's detail
   * message.
   *
   * @param message The detail message (which is saved for later retrieval by
   * the
   * {@link #getMessage()} method)
   * @param cause The cause (which is saved for later retrieval by the {@link
   * #getCause()} method).  (A null value is permitted, and indicates that the
   * cause is nonexistent or unknown.)
   * @since 1.6
   */
  public OMException(String message, Throwable cause,
      OMException.ResultCodes result) {
    super(message, cause);
    this.result = result;
  }

  /**
   * Constructs an {@code IOException} with the specified cause and a
   * detail message of {@code (cause==null ? null : cause.toString())}
   * (which typically contains the class and detail message of {@code cause}).
   * This constructor is useful for IO exceptions that are little more
   * than wrappers for other throwables.
   *
   * @param cause The cause (which is saved for later retrieval by the {@link
   * #getCause()} method).  (A null value is permitted, and indicates that the
   * cause is nonexistent or unknown.)
   * @since 1.6
   */
  public OMException(Throwable cause, OMException.ResultCodes result) {
    super(cause);
    this.result = result;
  }

  /**
   * Returns resultCode.
   * @return ResultCode
   */
  public OMException.ResultCodes getResult() {
    return result;
  }

  @Override
  public String toString() {
    return result + " " + super.toString();
  }
  /**
   * Error codes to make it easy to decode these exceptions.
   */
  public enum ResultCodes {

    OK,

    VOLUME_NOT_UNIQUE,

    VOLUME_NOT_FOUND,

    VOLUME_NOT_EMPTY,

    VOLUME_ALREADY_EXISTS,

    USER_NOT_FOUND,

    USER_TOO_MANY_VOLUMES,

    BUCKET_NOT_FOUND,

    BUCKET_NOT_EMPTY,

    BUCKET_ALREADY_EXISTS,

    KEY_ALREADY_EXISTS,

    KEY_NOT_FOUND,

    INVALID_KEY_NAME,

    ACCESS_DENIED,

    INTERNAL_ERROR,

    KEY_ALLOCATION_ERROR,

    KEY_DELETION_ERROR,

    KEY_RENAME_ERROR,

    METADATA_ERROR,

    OM_NOT_INITIALIZED,

    SCM_VERSION_MISMATCH_ERROR,

    S3_BUCKET_NOT_FOUND,

    S3_BUCKET_ALREADY_EXISTS,

    INITIATE_MULTIPART_UPLOAD_ERROR,

    MULTIPART_UPLOAD_PARTFILE_ERROR,

    NO_SUCH_MULTIPART_UPLOAD_ERROR,

    MISMATCH_MULTIPART_LIST,

    MISSING_UPLOAD_PARTS,

    COMPLETE_MULTIPART_UPLOAD_ERROR,

    ENTITY_TOO_SMALL,

    ABORT_MULTIPART_UPLOAD_FAILED,

    S3_SECRET_NOT_FOUND,

    INVALID_AUTH_METHOD,

    INVALID_TOKEN,

    TOKEN_EXPIRED,

    TOKEN_ERROR_OTHER,

    LIST_MULTIPART_UPLOAD_PARTS_FAILED,

    SCM_IN_SAFE_MODE,

    INVALID_REQUEST,

    BUCKET_ENCRYPTION_KEY_NOT_FOUND,

    UNKNOWN_CIPHER_SUITE,

    INVALID_KMS_PROVIDER,

    TOKEN_CREATION_ERROR,

    FILE_NOT_FOUND,

    DIRECTORY_NOT_FOUND,

    FILE_ALREADY_EXISTS,

    NOT_A_FILE
  }
}
