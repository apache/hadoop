package org.apache.hadoop.fs.azurebfs.services;

public interface RetryReasonAbbreviationCreator {
  String getAbbreviation(Exception ex, Integer statusCode, String serverErrorMessage);
}
