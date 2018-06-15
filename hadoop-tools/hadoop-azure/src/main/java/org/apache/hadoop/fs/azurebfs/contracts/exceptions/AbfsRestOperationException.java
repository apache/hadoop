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
    super(formatMessage(abfsHttpOperation));

    this.statusCode = statusCode;
    this.errorCode = AzureServiceErrorCode.getAzureServiceCode(this.statusCode, errorCode);
    this.errorMessage = errorMessage;
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
    return String.format(
        "%1$s %2$s%nStatusCode=%3$s%nStatusDescription=%4$s%nErrorCode=%5$s%nErrorMessage=%6$s",
        abfsHttpOperation.getMethod(),
        abfsHttpOperation.getUrl().toString(),
        abfsHttpOperation.getStatusCode(),
        abfsHttpOperation.getStatusDescription(),
        abfsHttpOperation.getStorageErrorCode(),
        abfsHttpOperation.getStorageErrorMessage());
  }
}