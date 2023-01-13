package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public enum RetryReason {
  //3character string
  //4xx exact status
  //in case of 503: give reason of throttling , ref anmol@ PR.
  //result.getStorageErrorMessage()
  //
  CONNECTION_TIMEOUT(((exceptionCaptured, statusCode) -> {
    return exceptionCaptured != null && "connect timed out".equalsIgnoreCase(
        exceptionCaptured.getMessage());
  }), 2, "CT"),
  READ_TIMEOUT(((exceptionCaptured, statusCode) -> {
    return exceptionCaptured != null && "Read timed out".equalsIgnoreCase(
        exceptionCaptured.getMessage());
  }), 2, "RT"),
  UNKNOWN_HOST("UH"),
  CONNECTION_RESET(((exceptionCaptured, statusCode) -> {
    return exceptionCaptured != null && exceptionCaptured.getMessage() != null
        && exceptionCaptured.getMessage().contains("Connection reset");
  }), 2, "CR"),
  STATUS_5XX(((exceptionCaptured, statusCode) -> {
    return statusCode / 100 == 5;
  }), 0, ((ex, statusCode, serverErrorMessage) -> {
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
  STATUS_4XX(((exceptionCaptured, statusCode) -> {
    return statusCode / 100 == 4;
  }), 0, ((ex, statusCode, serverErrorMessage) -> {
    return statusCode + "";
  })),
  UNKNOWN_SOCKET_EXCEPTION(((exceptionCaptured, statusCode) -> {
    return exceptionCaptured instanceof SocketException;
  }), 1, "SE"),
  UNKNOWN_IO_EXCEPTION(((exceptionCaptured, statusCode) -> {
    return exceptionCaptured instanceof IOException;
  }), 0, "IOE");

  private RetryReasonCaptureMechanism mechanism = null;

  private RetryReasonAbbreviationCreator retryReasonAbbreviationCreator = null;

  private int rank = 0;

  private String abbreviation;

  RetryReason(String abbreviation) {
    this.abbreviation = abbreviation;
  }

  RetryReason(RetryReasonCaptureMechanism mechanism,
      int rank,
      String abbreviation) {
    this.mechanism = mechanism;
    this.rank = rank;
    this.abbreviation = abbreviation;
  }

  RetryReason(RetryReasonCaptureMechanism mechanism,
      int rank,
      RetryReasonAbbreviationCreator abbreviationCreator) {
    this.mechanism = mechanism;
    this.rank = rank;
    this.retryReasonAbbreviationCreator = abbreviationCreator;
  }

  public String getAbbreviation(Exception ex,
      Integer statusCode,
      String serverErrorMessage) {
    if (abbreviation != null) {
      return abbreviation;
    }
    if (retryReasonAbbreviationCreator != null) {
      return retryReasonAbbreviationCreator.getAbbreviation(ex, statusCode,
          serverErrorMessage);
    }
    return null;
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

  static RetryReason getEnum(Exception ex, Integer statusCode) {
    RetryReason retryReasonResult = null;
    if (retryReasonLSortedist == null) {
      sortRetryReason();
    }
    for (RetryReason retryReason : retryReasonLSortedist) {
      if (retryReason.mechanism != null) {
        if (retryReason.mechanism.canCapture(ex, statusCode)) {
          retryReasonResult = retryReason;
        }
      }
    }
    return retryReasonResult;
  }
}
