package org.apache.hadoop.fs.azurebfs.services.retryReason;

import java.net.UnknownHostException;

import org.apache.hadoop.fs.azurebfs.services.RetryReasonAbbreviationCreator;

public class UnknownHostRetryReason implements RetryReasonAbbreviationCreator {

  @Override
  public String capturableAndGetAbbreviation(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    if (ex instanceof UnknownHostException) {
      return "UH";
    }
    return null;
  }
}
