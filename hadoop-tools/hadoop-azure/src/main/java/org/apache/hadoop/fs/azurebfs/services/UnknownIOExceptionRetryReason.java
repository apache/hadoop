package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

import org.apache.hadoop.fs.azurebfs.services.RetryReasonAbbreviationCreator;

class UnknownIOExceptionRetryReason implements
    RetryReasonAbbreviationCreator {

  @Override
  public Boolean canCapture(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    if (ex instanceof IOException) {
      return true;
    }
    return false;
  }

  @Override
  public String getAbbreviation(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    return "IOE";
  }
}
