package org.apache.hadoop.fs.azurebfs.services;

interface RetryReasonCaptureMechanism {
  boolean canCapture(Exception exceptionCaptured, Integer statusCode);
}
