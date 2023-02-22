package org.apache.hadoop.fs.azurebfs.services.retryReason;

import org.apache.hadoop.fs.azurebfs.services.RetryReasonAbbreviationCreator;

import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.connectionTimeoutJdkMessage;

public class ConnectionTimeoutRetryReason implements
    RetryReasonAbbreviationCreator {

  @Override
  public String capturableAndGetAbbreviation(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    return buildFromExceptionMessage(ex, connectionTimeoutJdkMessage, "CT");
  }
}
