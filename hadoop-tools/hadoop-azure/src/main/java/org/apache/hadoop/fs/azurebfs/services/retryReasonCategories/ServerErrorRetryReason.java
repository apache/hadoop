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

package org.apache.hadoop.fs.azurebfs.services.retryReasonCategories;

import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_STATUS_CATEGORY_QUOTIENT;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.EGRESS_OVER_ACCOUNT_LIMIT;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.INGRESS_OVER_ACCOUNT_LIMIT;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.EGRESS_LIMIT_BREACH_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.INGRESS_LIMIT_BREACH_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.OPERATION_BREACH_MESSAGE;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.OPERATION_LIMIT_BREACH_ABBREVIATION;

/**
 * Category that can capture server-response errors for 5XX status-code.
 */
public class ServerErrorRetryReason extends RetryReasonCategory {

  @Override
  Boolean canCapture(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    if (statusCode == null || statusCode / HTTP_STATUS_CATEGORY_QUOTIENT != 5) {
      return false;
    }
    return true;
  }

  @Override
  String getAbbreviation(final Integer statusCode,
      final String serverErrorMessage) {
    if (statusCode == HTTP_UNAVAILABLE && serverErrorMessage != null) {
      String splitedServerErrorMessage = serverErrorMessage.split(System.lineSeparator(),
          2)[0];
      if (INGRESS_OVER_ACCOUNT_LIMIT.getErrorMessage().equalsIgnoreCase(
          splitedServerErrorMessage)) {
        return INGRESS_LIMIT_BREACH_ABBREVIATION;
      }
      if (EGRESS_OVER_ACCOUNT_LIMIT.getErrorMessage().equalsIgnoreCase(
          splitedServerErrorMessage)) {
        return EGRESS_LIMIT_BREACH_ABBREVIATION;
      }
      if (OPERATION_BREACH_MESSAGE.equalsIgnoreCase(
          splitedServerErrorMessage)) {
        return OPERATION_LIMIT_BREACH_ABBREVIATION;
      }
      return HTTP_UNAVAILABLE + "";
    }
    return statusCode + "";
  }
}
