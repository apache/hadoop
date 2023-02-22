package org.apache.hadoop.fs.azurebfs.services.retryReason;

import org.apache.hadoop.fs.azurebfs.services.RetryReasonAbbreviationCreator;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_STATUS_CATEGORY_QUOTIENT;

public class ClientErrorRetryReason implements RetryReasonAbbreviationCreator {

  @Override
  public String capturableAndGetAbbreviation(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    if (statusCode == null || statusCode / HTTP_STATUS_CATEGORY_QUOTIENT != 4) {
      return null;
    }
    return statusCode + "";
  }
}
