package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.SocketException;
import java.util.Arrays;
import java.util.stream.Collectors;

public enum RetryReason {
  CONNECTION_TIMEOUT,
  READ_TIMEOUT(((exceptionCaptured, statusCode) -> {
    return "Read timed out".equalsIgnoreCase(exceptionCaptured.getMessage());
  }), 2),
  UNKNOWN_HOST,
  CONNECTION_RESET(((exceptionCaptured, statusCode) -> {
    return "connect timed out".equalsIgnoreCase(exceptionCaptured.getMessage());
  }), 2),
  STATUS_5XX(((exceptionCaptured, statusCode) -> {
    return statusCode/100 == 5;
  }), 0),
  STATUS_4XX(((exceptionCaptured, statusCode) -> {
    return statusCode/100 == 4;
  }), 0),
  UNKNOWN_SOCKET_EXCEPTION(((exceptionCaptured, statusCode) -> {
    return exceptionCaptured instanceof SocketException;
  }), 1),
  UNKNOWN_IO_EXCEPTION(((exceptionCaptured, statusCode) -> {
    return exceptionCaptured instanceof IOException;
  }), 0);

  private RetryReasonCaptureMechanism mechanism = null;
  private int rank = 0;

  RetryReason() {

  }

  RetryReason(RetryReasonCaptureMechanism mechanism, int rank) {
    this.mechanism = mechanism;
    this.rank = rank;
  }

  static RetryReason getEnum(Exception ex, Integer statusCode) {
    RetryReason retryReasonResult = null;
    for(RetryReason retryReason : Arrays.stream(values()).sorted().collect(
        Collectors.toList())) {
      if(retryReason.mechanism != null) {
        if(retryReason.mechanism.canCapture(ex, statusCode)) {
          retryReasonResult = retryReason;
        }
      }
    }
    return retryReasonResult;
  }
}
