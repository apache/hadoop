package org.apache.hadoop.fs.azurebfs.services.retryReason;

import java.net.SocketException;

import org.apache.hadoop.fs.azurebfs.services.RetryReasonAbbreviationCreator;

public class UnknownSocketExceptionRetryReason implements
    RetryReasonAbbreviationCreator {

  @Override
  public String capturableAndGetAbbreviation(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    if (ex instanceof SocketException) {
      return "SE";
    }
    return null;
  }
}
