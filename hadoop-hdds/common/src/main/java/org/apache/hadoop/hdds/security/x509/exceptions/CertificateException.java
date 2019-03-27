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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.security.x509.exceptions;

import org.apache.hadoop.hdds.security.exception.SCMSecurityException;

/**
 * Certificate Exceptions from the SCM Security layer.
 */
public class CertificateException extends SCMSecurityException {

  private ErrorCode errorCode;
  /**
   * Ctor.
   * @param message - Error Message.
   */
  public CertificateException(String message) {
    super(message);
  }

  /**
   * Ctor.
   * @param message - Message.
   * @param cause  - Actual cause.
   */
  public CertificateException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Ctor.
   * @param message - Message.
   * @param cause  - Actual cause.
   * @param errorCode
   */
  public CertificateException(String message, Throwable cause,
      ErrorCode errorCode) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  /**
   * Ctor.
   * @param message - Message.
   * @param errorCode
   */
  public CertificateException(String message, ErrorCode errorCode) {
    super(message);
    this.errorCode = errorCode;
  }

  /**
   * Ctor.
   * @param cause - Base Exception.
   */
  public CertificateException(Throwable cause) {
    super(cause);
  }

  /**
   * Error codes to make it easy to decode these exceptions.
   */
  public enum ErrorCode {
    KEYSTORE_ERROR,
    CRYPTO_SIGN_ERROR,
    CERTIFICATE_ERROR,
    BOOTSTRAP_ERROR,
    CSR_ERROR,
    CRYPTO_SIGNATURE_VERIFICATION_ERROR,
    CERTIFICATE_NOT_FOUND_ERROR
  }
}
