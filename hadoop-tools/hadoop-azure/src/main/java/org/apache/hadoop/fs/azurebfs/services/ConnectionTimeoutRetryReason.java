package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.services.RetryReasonAbbreviationCreator;

import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.connectionTimeoutJdkMessage;

class ConnectionTimeoutRetryReason implements
    RetryReasonAbbreviationCreator {

  @Override
  public String getAbbreviation(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    return "CT";
  }

  @Override
  public Boolean canCapture(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    return buildFromExceptionMessage(ex, connectionTimeoutJdkMessage, "CT");
  }
}
