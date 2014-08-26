package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;

/**
 * Used to inject certain faults for testing.
 */
public class EncryptionFaultInjector {
  @VisibleForTesting
  public static EncryptionFaultInjector instance =
      new EncryptionFaultInjector();

  @VisibleForTesting
  public static EncryptionFaultInjector getInstance() {
    return instance;
  }

  @VisibleForTesting
  public void startFileAfterGenerateKey() throws IOException {}
}
