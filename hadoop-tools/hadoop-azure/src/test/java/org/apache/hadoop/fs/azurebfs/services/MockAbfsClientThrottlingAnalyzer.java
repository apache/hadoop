package org.apache.hadoop.fs.azurebfs.services;

public class MockAbfsClientThrottlingAnalyzer extends AbfsClientThrottlingAnalyzer {
  private int failedInstances = 0;

  public MockAbfsClientThrottlingAnalyzer(final String name)
      throws IllegalArgumentException {
    super(name);
  }

  @Override
  public void addBytesTransferred(final long count,
      final boolean isFailedOperation) {
    if(isFailedOperation) {
      failedInstances++;
    }
    super.addBytesTransferred(count, isFailedOperation);
  }

  public int getFailedInstances() {
    return failedInstances;
  }
}