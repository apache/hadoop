package org.apache.hadoop.fs.azurebfs.services;

import java.net.UnknownHostException;

import org.apache.hadoop.fs.azurebfs.services.RetryReasonAbbreviationCreator;

class UnknownHostRetryReason implements RetryReasonAbbreviationCreator {

  @Override
  public Boolean canCapture(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    if (ex instanceof UnknownHostException) {
      return true;
    }
    return false;
  }

  @Override
  public String getAbbreviation(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    return "UH";
  }
}
