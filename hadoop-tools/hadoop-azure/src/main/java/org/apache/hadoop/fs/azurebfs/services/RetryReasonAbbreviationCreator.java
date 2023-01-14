package org.apache.hadoop.fs.azurebfs.services;

public interface RetryReasonAbbreviationCreator {
  String capturableAndGetAbbreviation(Exception ex, Integer statusCode, String serverErrorMessage);
}
