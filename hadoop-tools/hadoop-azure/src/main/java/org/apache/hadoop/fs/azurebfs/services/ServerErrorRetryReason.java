package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.services.RetryReasonAbbreviationCreator;

import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_STATUS_CATEGORY_QUOTIENT;

class ServerErrorRetryReason implements RetryReasonAbbreviationCreator {

  @Override
  public Boolean canCapture(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    if (statusCode == null || statusCode / HTTP_STATUS_CATEGORY_QUOTIENT != 5) {
      return false;
    }
    return true;
  }

  @Override
  public String getAbbreviation(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    if (statusCode == HTTP_UNAVAILABLE) {
      String splitedServerErrorMessage = serverErrorMessage.split(System.lineSeparator(),
          2)[0];
      if ("Ingress is over the account limit.".equalsIgnoreCase(
          splitedServerErrorMessage)) {
        return "ING";
      }
      if ("Egress is over the account limit.".equalsIgnoreCase(
          splitedServerErrorMessage)) {
        return "EGR";
      }
      if ("Operations per second is over the account limit.".equalsIgnoreCase(
          splitedServerErrorMessage)) {
        return "OPR";
      }
      return HTTP_UNAVAILABLE + "";
    }
    return statusCode + "";
  }
}
