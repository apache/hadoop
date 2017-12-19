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
package org.apache.hadoop.ozone.ksm.exceptions;

import java.io.IOException;

/**
 * Exception thrown by KSM.
 */
public class KSMException extends IOException {
  private final KSMException.ResultCodes result;

  /**
   * Constructs an {@code IOException} with {@code null}
   * as its error detail message.
   */
  public KSMException(KSMException.ResultCodes result) {
    this.result = result;
  }

  /**
   * Constructs an {@code IOException} with the specified detail message.
   *
   * @param message The detail message (which is saved for later retrieval by
   * the
   * {@link #getMessage()} method)
   */
  public KSMException(String message, KSMException.ResultCodes result) {
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
  public KSMException(String message, Throwable cause,
                      KSMException.ResultCodes result) {
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
  public KSMException(Throwable cause, KSMException.ResultCodes result) {
    super(cause);
    this.result = result;
  }

  /**
   * Returns resultCode.
   * @return ResultCode
   */
  public KSMException.ResultCodes getResult() {
    return result;
  }

  /**
   * Error codes to make it easy to decode these exceptions.
   */
  public enum ResultCodes {
    FAILED_TOO_MANY_USER_VOLUMES,
    FAILED_VOLUME_ALREADY_EXISTS,
    FAILED_VOLUME_NOT_FOUND,
    FAILED_VOLUME_NOT_EMPTY,
    FAILED_USER_NOT_FOUND,
    FAILED_BUCKET_ALREADY_EXISTS,
    FAILED_BUCKET_NOT_FOUND,
    FAILED_BUCKET_NOT_EMPTY,
    FAILED_KEY_ALREADY_EXISTS,
    FAILED_KEY_NOT_FOUND,
    FAILED_KEY_ALLOCATION,
    FAILED_KEY_DELETION,
    FAILED_METADATA_ERROR,
    FAILED_INTERNAL_ERROR,
    KSM_NOT_INITIALIZED,
    SCM_VERSION_MISMATCH_ERROR
  }
}
