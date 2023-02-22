package org.apache.hadoop.fs.azurebfs.services.retryReason;

import java.io.IOException;

import org.apache.hadoop.fs.azurebfs.services.RetryReasonAbbreviationCreator;

public class UnknownIOExceptionRetryReason implements
    RetryReasonAbbreviationCreator {

  @Override
  public String capturableAndGetAbbreviation(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    if (ex instanceof IOException) {
      return "IOE";
    }
    return null;
  }
}
