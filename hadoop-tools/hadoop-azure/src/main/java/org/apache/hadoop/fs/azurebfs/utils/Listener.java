package org.apache.hadoop.fs.azurebfs.utils;

public interface Listener {
  void callTracingHeaderValidator(String header, TracingContextFormat format);
  void updatePrimaryRequestID(String primaryRequestID);
  Listener getClone();
  void setOperation(String operation);
}
