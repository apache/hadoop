package org.apache.hadoop.compat;

public class HdfsCompatIllegalArgumentException
    extends IllegalArgumentException {
  HdfsCompatIllegalArgumentException(String message) {
    super(message);
  }
}