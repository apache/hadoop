package org.apache.hadoop.fs.azurebfs.services.retryReasonCategories;

import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_TAIL_LATENCY_REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.TAIL_LATENCY_TIMEOUT_ABBREVIATION;

public class TailLatencyRequestTimeoutRetryReason  extends
    RetryReasonCategory {

  @Override
  String getAbbreviation(final Integer statusCode,
      final String serverErrorMessage) {
    return TAIL_LATENCY_TIMEOUT_ABBREVIATION;
  }

  @Override
  Boolean canCapture(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    return checkExceptionMessage(ex, ERR_TAIL_LATENCY_REQUEST_TIMEOUT);
  }
}