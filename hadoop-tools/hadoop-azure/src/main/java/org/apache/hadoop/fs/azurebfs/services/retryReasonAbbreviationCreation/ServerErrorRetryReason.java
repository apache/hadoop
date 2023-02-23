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

package org.apache.hadoop.fs.azurebfs.services.retryReasonAbbreviationCreation;

import org.apache.hadoop.fs.azurebfs.services.retryReasonAbbreviationCreation.RetryReasonAbbreviationCreator;

import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_STATUS_CATEGORY_QUOTIENT;

public class ServerErrorRetryReason extends RetryReasonAbbreviationCreator {

  @Override
  public Boolean canCapture(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    if (statusCode == null || statusCode / HTTP_STATUS_CATEGORY_QUOTIENT != 5) {
      return false;
    }
    return true;
  }

  @Override
  public String getAbbreviation(final Integer statusCode,
      final String serverErrorMessage) {
    if (statusCode == HTTP_UNAVAILABLE) {
      String splitedServerErrorMessage = serverErrorMessage.split(System.lineSeparator(),
          2)[0];
      if ("Ingress is over the account limit.".equalsIgnoreCase(
          splitedServerErrorMessage)) {
        return "ING";
      }
      if ("Egress is over the account limit.".equalsIgnoreCase(
          splitedServerErrorMessage)) {
        return "EGR";
      }
      if ("Operations per second is over the account limit.".equalsIgnoreCase(
          splitedServerErrorMessage)) {
        return "OPR";
      }
      return HTTP_UNAVAILABLE + "";
    }
    return statusCode + "";
  }
}
