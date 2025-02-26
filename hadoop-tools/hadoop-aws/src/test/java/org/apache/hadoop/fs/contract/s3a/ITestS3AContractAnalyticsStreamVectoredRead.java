package org.apache.hadoop.fs.contract.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.enableAnalyticsAccelerator;

/**
 * S3A contract tests for vectored reads with the Analytics stream. The analytics stream does
 * not explicitly implement the vectoredRead() method, or currently do and vectored-read specific
 * optimisations (such as range coalescing). However, this test ensures that the base implementation
 * of readVectored {@link org.apache.hadoop.fs.PositionedReadable} still works.
 */
public class ITestS3AContractAnalyticsStreamVectoredRead extends AbstractContractVectoredReadTest {

  public ITestS3AContractAnalyticsStreamVectoredRead(String bufferType) {
    super(bufferType);
  }

  /**
   * Create a configuration.
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    enableAnalyticsAccelerator(conf);
    conf.set("fs.contract.vector-io-early-eof-check", "false");
    return conf;
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }
}
