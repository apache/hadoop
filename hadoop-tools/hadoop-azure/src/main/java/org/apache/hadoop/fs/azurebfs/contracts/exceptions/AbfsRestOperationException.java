/**
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


package org.apache.hadoop.fs.azurebfs.contracts.exceptions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator.HttpException;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;

/**
 * Exception to wrap Azure service error responses.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AbfsRestOperationException extends AzureBlobFileSystemException {
  private final int statusCode;
  private final AzureServiceErrorCode errorCode;
  private final String errorMessage;

  public AbfsRestOperationException(
      final int statusCode,
      final String errorCode,
      final String errorMessage,
      final Exception innerException) {
    super("Status code: " + statusCode + " error code: " + errorCode + " error message: " + errorMessage, innerException);

    this.statusCode = statusCode;
    this.errorCode = AzureServiceErrorCode.getAzureServiceCode(this.statusCode, errorCode);
    this.errorMessage = errorMessage;
  }

  public AbfsRestOperationException(
      final int statusCode,
      final String errorCode,
      final String errorMessage,
      final Exception innerException,
      final AbfsHttpOperation abfsHttpOperation) {
    super(formatMessage(abfsHttpOperation), innerException);

    this.statusCode = statusCode;
    this.errorCode = AzureServiceErrorCode.getAzureServiceCode(this.statusCode, errorCode);
    this.errorMessage = errorMessage;
  }

  public AbfsRestOperationException(final HttpException innerException) {
    super(innerException.getMessage(), innerException);

    this.statusCode = innerException.getHttpErrorCode();
    this.errorCode = AzureServiceErrorCode.UNKNOWN;
    this.errorMessage = innerException.getMessage();
  }

  public int getStatusCode() {
    return this.statusCode;
  }

  public AzureServiceErrorCode getErrorCode() {
    return this.errorCode;
  }

  public String getErrorMessage() {
    return this.errorMessage;
  }

  private static String formatMessage(final AbfsHttpOperation abfsHttpOperation) {
    // HEAD request response doesn't have StorageErrorCode, StorageErrorMessage.
    if (abfsHttpOperation.getMethod().equals("HEAD")) {
      return String.format(
              "Operation failed: \"%1$s\", %2$s, HEAD, %3$s",
              abfsHttpOperation.getStatusDescription(),
              abfsHttpOperation.getStatusCode(),
              abfsHttpOperation.getUrl().toString());
    }

    return String.format(
            "Operation failed: \"%1$s\", %2$s, %3$s, %4$s, %5$s, \"%6$s\"",
            abfsHttpOperation.getStatusDescription(),
            abfsHttpOperation.getStatusCode(),
            abfsHttpOperation.getMethod(),
            abfsHttpOperation.getUrl().toString(),
            abfsHttpOperation.getStorageErrorCode(),
            // Remove break line to ensure the request id and timestamp can be shown in console.
            abfsHttpOperation.getStorageErrorMessage().replaceAll("\\n", " "));
  }
}
