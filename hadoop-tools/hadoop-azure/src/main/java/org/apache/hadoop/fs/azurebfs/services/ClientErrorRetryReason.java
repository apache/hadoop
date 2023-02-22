package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.services.RetryReasonAbbreviationCreator;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_STATUS_CATEGORY_QUOTIENT;

class ClientErrorRetryReason implements RetryReasonAbbreviationCreator {

  @Override
  public Boolean canCapture(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    if (statusCode == null || statusCode / HTTP_STATUS_CATEGORY_QUOTIENT != 4) {
      return false;
    }
    return true;
  }

  @Override
  public String getAbbreviation(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    return statusCode + "";
  }
}
