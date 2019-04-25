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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Azure service error codes.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum AzureServiceErrorCode {
  FILE_SYSTEM_ALREADY_EXISTS("FilesystemAlreadyExists", HttpURLConnection.HTTP_CONFLICT, null),
  PATH_ALREADY_EXISTS("PathAlreadyExists", HttpURLConnection.HTTP_CONFLICT, null),
  INTERNAL_OPERATION_ABORT("InternalOperationAbortError", HttpURLConnection.HTTP_CONFLICT, null),
  PATH_CONFLICT("PathConflict", HttpURLConnection.HTTP_CONFLICT, null),
  FILE_SYSTEM_NOT_FOUND("FilesystemNotFound", HttpURLConnection.HTTP_NOT_FOUND, null),
  PATH_NOT_FOUND("PathNotFound", HttpURLConnection.HTTP_NOT_FOUND, null),
  PRE_CONDITION_FAILED("PreconditionFailed", HttpURLConnection.HTTP_PRECON_FAILED, null),
  SOURCE_PATH_NOT_FOUND("SourcePathNotFound", HttpURLConnection.HTTP_NOT_FOUND, null),
  INVALID_SOURCE_OR_DESTINATION_RESOURCE_TYPE("InvalidSourceOrDestinationResourceType", HttpURLConnection.HTTP_CONFLICT, null),
  RENAME_DESTINATION_PARENT_PATH_NOT_FOUND("RenameDestinationParentPathNotFound", HttpURLConnection.HTTP_NOT_FOUND, null),
  INVALID_RENAME_SOURCE_PATH("InvalidRenameSourcePath", HttpURLConnection.HTTP_CONFLICT, null),
  INGRESS_OVER_ACCOUNT_LIMIT(null, HttpURLConnection.HTTP_UNAVAILABLE, "Ingress is over the account limit."),
  EGRESS_OVER_ACCOUNT_LIMIT(null, HttpURLConnection.HTTP_UNAVAILABLE, "Egress is over the account limit."),
  INVALID_QUERY_PARAMETER_VALUE("InvalidQueryParameterValue", HttpURLConnection.HTTP_BAD_REQUEST, null),
  AUTHORIZATION_PERMISSION_MISS_MATCH("AuthorizationPermissionMismatch", HttpURLConnection.HTTP_FORBIDDEN, null),
  UNKNOWN(null, -1, null);

  private final String errorCode;
  private final int httpStatusCode;
  private final String errorMessage;
  AzureServiceErrorCode(String errorCode, int httpStatusCodes, String errorMessage) {
    this.errorCode = errorCode;
    this.httpStatusCode = httpStatusCodes;
    this.errorMessage = errorMessage;
  }

  public int getStatusCode() {
    return this.httpStatusCode;
  }

  public String getErrorCode() {
    return this.errorCode;
  }

  public static List<AzureServiceErrorCode> getAzureServiceCode(int httpStatusCode) {
    List<AzureServiceErrorCode> errorCodes = new ArrayList<>();
    if (httpStatusCode == UNKNOWN.httpStatusCode) {
      errorCodes.add(UNKNOWN);
      return errorCodes;
    }

    for (AzureServiceErrorCode azureServiceErrorCode : AzureServiceErrorCode.values()) {
      if (azureServiceErrorCode.httpStatusCode == httpStatusCode) {
        errorCodes.add(azureServiceErrorCode);
      }
    }

    return errorCodes;
  }

  public static AzureServiceErrorCode getAzureServiceCode(int httpStatusCode, String errorCode) {
    if (errorCode == null || errorCode.isEmpty() || httpStatusCode == UNKNOWN.httpStatusCode) {
      return UNKNOWN;
    }

    for (AzureServiceErrorCode azureServiceErrorCode : AzureServiceErrorCode.values()) {
      if (errorCode.equalsIgnoreCase(azureServiceErrorCode.errorCode)
          && azureServiceErrorCode.httpStatusCode == httpStatusCode) {
        return azureServiceErrorCode;
      }
    }

    return UNKNOWN;
  }

  public static AzureServiceErrorCode getAzureServiceCode(int httpStatusCode, String errorCode, final String errorMessage) {
    if (errorCode == null || errorCode.isEmpty() || httpStatusCode == UNKNOWN.httpStatusCode || errorMessage == null || errorMessage.isEmpty()) {
      return UNKNOWN;
    }

    for (AzureServiceErrorCode azureServiceErrorCode : AzureServiceErrorCode.values()) {
      if (azureServiceErrorCode.httpStatusCode == httpStatusCode
          && errorCode.equalsIgnoreCase(azureServiceErrorCode.errorCode)
          && errorMessage.equalsIgnoreCase(azureServiceErrorCode.errorMessage)
      ) {
        return azureServiceErrorCode;
      }
    }

    return UNKNOWN;
  }
}
