package org.apache.hadoop.fs.s3a.scale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.Constants;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ITestS3AHugeFileUpload extends S3AScaleTestBase{
  final private Logger LOG = LoggerFactory.getLogger(
      ITestS3AHugeFileUpload.class.getName());

  private long fileSize = Integer.MAX_VALUE * 2L;
  @Override
  protected Configuration createScaleConfiguration() {
    Configuration configuration = super.createScaleConfiguration();
    configuration.setBoolean(Constants.ALLOW_MULTIPART_UPLOADS, false);
    configuration.setInt(KEY_TEST_TIMEOUT, 36000);
    return configuration;
  }

  @Test
  public void uploadFileSinglePut() throws IOException {
    LOG.info("Creating file with size : {}", fileSize);
    ContractTestUtils.createAndVerifyFile(getFileSystem(),
        getTestPath(), fileSize );
  }
}
