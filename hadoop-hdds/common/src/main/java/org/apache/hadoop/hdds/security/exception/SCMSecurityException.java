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

package org.apache.hadoop.hdds.security.exception;

import java.io.IOException;

/**
 * Root Security Exception call for all Certificate related Execptions.
 */
public class SCMSecurityException extends IOException {
  private final ErrorCode errorCode;

  /**
   * Ctor.
   * @param message - Error Message.
   */
  public SCMSecurityException(String message) {
    super(message);
    this.errorCode = ErrorCode.DEFAULT;
  }

  /**
   * Ctor.
   * @param message - Message.
   * @param cause  - Actual cause.
   */
  public SCMSecurityException(String message, Throwable cause) {
    super(message, cause);
    this.errorCode = ErrorCode.DEFAULT;
  }

  /**
   * Ctor.
   * @param message - Message.
   * @param error   - error code.
   */
  public SCMSecurityException(String message, ErrorCode error) {
    super(message);
    this.errorCode = error;
  }

  /**
   * Ctor.
   * @param cause - Base Exception.
   */
  public SCMSecurityException(Throwable cause) {
    super(cause);
    this.errorCode = ErrorCode.DEFAULT;
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  /**
   * Error codes to make it easy to decode these exceptions.
   */
  public enum ErrorCode {
    DEFAULT,
    MISSING_BLOCK_TOKEN,
    BLOCK_TOKEN_VERIFICATION_FAILED
  }
}
