package org.apache.hadoop.fs.azurebfs.services.retryReason;

import org.apache.hadoop.fs.azurebfs.services.RetryReasonAbbreviationCreator;

import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.connectionResetMessage;

public class ConnectionResetRetryReason implements
    RetryReasonAbbreviationCreator {

  @Override
  public String capturableAndGetAbbreviation(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    return buildFromExceptionMessage(ex, connectionResetMessage, "CR");
  }
}
