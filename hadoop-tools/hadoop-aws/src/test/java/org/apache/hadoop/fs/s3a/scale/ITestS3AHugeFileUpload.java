package org.apache.hadoop.fs.s3a.scale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.Constants;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.IO_CHUNK_BUFFER_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;

public class ITestS3AHugeFileUpload extends S3AScaleTestBase{
  final private Logger LOG = LoggerFactory.getLogger(
      ITestS3AHugeFileUpload.class.getName());

  private long fileSize = Integer.MAX_VALUE * 2L;
  @Override
  protected Configuration createScaleConfiguration() {
    Configuration configuration = super.createScaleConfiguration();
    configuration.setBoolean(Constants.ALLOW_MULTIPART_UPLOADS, false);
    configuration.setLong(MULTIPART_SIZE, 53687091200L);
    configuration.setInt(KEY_TEST_TIMEOUT, 36000);
    configuration.setInt(IO_CHUNK_BUFFER_SIZE, 655360);
    return configuration;
  }

  @Test
  public void uploadFileSinglePut() throws IOException {
    LOG.info("Creating file with size : {}", fileSize);
    ContractTestUtils.createAndVerifyFile(getFileSystem(),
        getTestPath(), fileSize );
  }
}
