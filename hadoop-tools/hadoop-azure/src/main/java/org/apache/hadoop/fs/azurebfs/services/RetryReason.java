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

import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_STATUS_CATEGORY_QUOTIENT;

/**
 * In case of retry, this enum would give the information on the reason for
 * previous API call.
 * */
public enum RetryReason {
  CONNECTION_TIMEOUT(2,
      ((exceptionCaptured, statusCode, serverErrorMessage) -> {
        if (exceptionCaptured != null && "connect timed out".equalsIgnoreCase(
            exceptionCaptured.getMessage())) {
          return "CT";
        }
        return null;
      })),
  READ_TIMEOUT(2, ((exceptionCaptured, statusCode, serverErrorMessage) -> {
    if (exceptionCaptured != null && "Read timed out".equalsIgnoreCase(
        exceptionCaptured.getMessage())) {
      return "RT";
    }
    return null;
  })),
  UNKNOWN_HOST(2, ((exceptionCaptured, statusCode, serverErrorMessage) -> {
    if (exceptionCaptured instanceof UnknownHostException) {
      return "UH";
    }
    return null;
  })),
  CONNECTION_RESET(2, ((exceptionCaptured, statusCode, serverErrorMessage) -> {
    if (exceptionCaptured != null && exceptionCaptured.getMessage() != null
        && exceptionCaptured.getMessage().contains("Connection reset")) {
      return "CR";
    }
    return null;
  })),
  STATUS_5XX(0, ((exceptionCaptured, statusCode, serverErrorMessage) -> {
    if (statusCode == null || statusCode / HTTP_STATUS_CATEGORY_QUOTIENT != 5) {
      return null;
    }
    if (statusCode == HTTP_UNAVAILABLE) {
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
      return HTTP_UNAVAILABLE + "";
    }
    return statusCode + "";
  })),
  STATUS_4XX(0, ((exceptionCaptured, statusCode, serverErrorMessage) -> {
    if (statusCode == null || statusCode / HTTP_STATUS_CATEGORY_QUOTIENT != 4) {
      return null;
    }
    return statusCode + "";
  })),
  UNKNOWN_SOCKET_EXCEPTION(1,
      ((exceptionCaptured, statusCode, serverErrorMessage) -> {
        if (exceptionCaptured instanceof SocketException) {
          return "SE";
        }
        return null;
      })),
  UNKNOWN_IO_EXCEPTION(0,
      ((exceptionCaptured, statusCode, serverErrorMessage) -> {
        if (exceptionCaptured instanceof IOException) {
          return "IOE";
        }
        return null;
      }));

  private RetryReasonAbbreviationCreator retryReasonAbbreviationCreator = null;

  private int rank = 0;

  /**
   * Constructor to have rank and the implementation of {@link RetryReasonAbbreviationCreator}.
   * @param rank rank of a given enum. For example SocketTimeoutException is
   * subclass of IOException. Rank of SocketTimeoutException enum has to be
   * more than that of IOException enum.
   * @param abbreviationCreator The implementation of {@link RetryReasonAbbreviationCreator}
   * which would give the information if a given enum can be mapped to an error or not.
   * */
  RetryReason(int rank,
      RetryReasonAbbreviationCreator abbreviationCreator) {
    this.rank = rank;
    this.retryReasonAbbreviationCreator = abbreviationCreator;
  }

  private static List<RetryReason> retryReasonSortedList;

  /**
   * Synchronized method to assign sorted list in {@link RetryReason#retryReasonSortedList}.
   * Method would check if list is assigned or not. If yes, method would return. This is required
   * because multiple threads could be waiting to get into this method, and once a thread is done
   * with this method, other thread would get into this method. Since the list would be assigned by
   * first thread, the second thread need not run the whole mechanism of sorting.
   * The enums are sorted on the ascending order of their rank.
   * */
  private static synchronized void sortRetryReason() {
    if (retryReasonSortedList != null) {
      return;
    }
    List<RetryReason> list = new ArrayList<>();
    for (RetryReason reason : values()) {
      list.add(reason);
    }
    list.sort((c1, c2) -> {
      return c1.rank - c2.rank;
    });
    retryReasonSortedList = list;
  }

  /**
   * Method to get correct abbreviation for a given set of exception, statusCode,
   * storageStatusCode.
   * Method would iterate through the {@link RetryReason#retryReasonSortedList},
   * and would return the abbreviation returned by highest enum to be applicable on the group.
   * For example, if SocketTimeoutException(rank 2) and IOException(rank 0) can be
   * applied on the group, the abbreviation of SocketTimeoutException has to be returned.
   *
   * @param ex exception caught during server communication.
   * @param statusCode statusCode in the server response.
   * @param storageErrorMessage storageErrorMessage in the server response.
   * */
  static String getAbbreviation(Exception ex,
      Integer statusCode,
      String storageErrorMessage) {
    String result = null;
    if (retryReasonSortedList == null) {
      sortRetryReason();
    }
    for (RetryReason retryReason : retryReasonSortedList) {
      String enumCapturedAndAbbreviate
          = retryReason.retryReasonAbbreviationCreator.capturableAndGetAbbreviation(
          ex, statusCode, storageErrorMessage);
      if (enumCapturedAndAbbreviate != null) {
        result = enumCapturedAndAbbreviate;
      }
    }
    return result;
  }
}
