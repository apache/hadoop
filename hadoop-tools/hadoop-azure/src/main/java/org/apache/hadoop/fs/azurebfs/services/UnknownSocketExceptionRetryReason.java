package org.apache.hadoop.fs.azurebfs.services;

import java.net.SocketException;

import org.apache.hadoop.fs.azurebfs.services.RetryReasonAbbreviationCreator;

class UnknownSocketExceptionRetryReason implements
    RetryReasonAbbreviationCreator {

  @Override
  public Boolean canCapture(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    if (ex instanceof SocketException) {
      return true;
    }
    return false;
  }

  @Override
  public String getAbbreviation(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    return "SE";
  }
}
