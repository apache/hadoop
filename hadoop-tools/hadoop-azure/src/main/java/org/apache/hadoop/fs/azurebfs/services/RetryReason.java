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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public enum RetryReason {
  CONNECTION_TIMEOUT(2, ((ex, statusCode, serverErrorMessage) -> {
    if(ex != null && "connect timed out".equalsIgnoreCase(
        ex.getMessage())) {
      return "CT";
    }
    return null;
  })),
  READ_TIMEOUT(2, ((exceptionCaptured, statusCode, serverErrorMessage) -> {
    if(exceptionCaptured != null && "Read timed out".equalsIgnoreCase(
        exceptionCaptured.getMessage())) {
      return "RT";
    }
    return null;
  })),
  UNKNOWN_HOST(2, ((ex, statusCode, serverErrorMessage) -> {
    if(ex instanceof UnknownHostException) {
      return "UH";
    }
    return null;
  })),
  CONNECTION_RESET(2, ((exceptionCaptured, statusCode, serverErrorMessage) -> {
    if(exceptionCaptured != null && exceptionCaptured.getMessage() != null
        && exceptionCaptured.getMessage().contains("Connection reset")) {
      return "CR";
    }
    return null;
  })),
  STATUS_5XX(0, ((ex, statusCode, serverErrorMessage) -> {
    if(statusCode == null || statusCode / 100 != 5) {
      return null;
    }
    if (statusCode == 503) {
      //ref: https://github.com/apache/hadoop/pull/4564/files#diff-75a2f54df6618d4015c63812e6a9916ddfb475d246850edfd2a6f57e36805e79
      serverErrorMessage = serverErrorMessage.split(System.lineSeparator(),
          2)[0];
      if ("Ingress is over the account limit.".equalsIgnoreCase(
          serverErrorMessage)) {
        return "ING";
      }
      if ("Egress is over the account limit.".equalsIgnoreCase(
          serverErrorMessage)) {
        return "EGR";
      }
      if ("Operations per second is over the account limit.".equalsIgnoreCase(
          serverErrorMessage)) {
        return "OPR";
      }
      return "503";
    }
    return statusCode + "";
  })),
  STATUS_4XX(0, ((ex, statusCode, serverErrorMessage) -> {
    if(statusCode == null || statusCode / 100 != 4) {
      return null;
    }
    return statusCode + "";
  })),
  UNKNOWN_SOCKET_EXCEPTION(1, ((ex, statusCode, serverErrorMessage) -> {
    if(ex instanceof  SocketException) {
      return "SE";
    }
    return null;
  })),
  UNKNOWN_IO_EXCEPTION(0, ((ex, statusCode, serverErrorMessage) -> {
    if(ex instanceof IOException) {
      return "IOE";
    }
    return null;
  }));

  private RetryReasonAbbreviationCreator retryReasonAbbreviationCreator = null;

  private int rank = 0;

  RetryReason(int rank,
      RetryReasonAbbreviationCreator abbreviationCreator) {
    this.rank = rank;
    this.retryReasonAbbreviationCreator = abbreviationCreator;
  }

  private static List<RetryReason> retryReasonLSortedist;

  private synchronized static void sortRetryReason() {
    if (retryReasonLSortedist != null) {
      return;
    }
    List<RetryReason> list = new ArrayList<>();
    for (RetryReason reason : values()) {
      list.add(reason);
    }
    list.sort((c1, c2) -> {
      return c1.rank - c2.rank;
    });
    retryReasonLSortedist = list;
  }

  static String getAbbreviation(Exception ex, Integer statusCode, String storageStatusCode) {
    String result = null;
    if (retryReasonLSortedist == null) {
      sortRetryReason();
    }
    for (RetryReason retryReason : retryReasonLSortedist) {
      String enumCaputredAndAbbreviate = retryReason.retryReasonAbbreviationCreator.capturableAndGetAbbreviation(ex, statusCode, storageStatusCode);
      if (enumCaputredAndAbbreviate != null) {
        result = enumCaputredAndAbbreviate;
      }
    }
    return result;
  }
}
