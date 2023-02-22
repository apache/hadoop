package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.services.RetryReasonAbbreviationCreator;

import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.connectionResetMessage;

class ConnectionResetRetryReason implements
    RetryReasonAbbreviationCreator {

  @Override
  public Boolean canCapture(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    return buildFromExceptionMessage(ex, connectionResetMessage, "CR");
  }

  @Override
  public String getAbbreviation(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    return "CR";
  }
}
