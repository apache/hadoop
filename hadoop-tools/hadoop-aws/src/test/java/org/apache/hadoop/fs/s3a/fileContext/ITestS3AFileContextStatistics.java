/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.s3a.fileContext;

import java.io.IOException;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FCStatisticsBaseTest;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.auth.STSClientFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import static org.apache.hadoop.fs.s3a.S3ATestConstants.KMS_KEY_GENERATION_REQUEST_PARAMS_BYTES_WRITTEN;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3AUtils.getEncryptionAlgorithm;
import static org.apache.hadoop.fs.s3a.S3AUtils.getS3EncryptionKey;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.CSE_PADDING_LENGTH;

/**
 * S3a implementation of FCStatisticsBaseTest.
 */
public class ITestS3AFileContextStatistics extends FCStatisticsBaseTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(STSClientFactory.class);

  private Path testRootPath;
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    fc = S3ATestUtils.createTestFileContext(conf);
    testRootPath = fileContextTestHelper.getTestRootPath(fc, "test");
    fc.mkdir(testRootPath,
        FileContext.DEFAULT_PERM, true);
    FileContext.clearStatistics();
  }

  @After
  public void tearDown() throws Exception {
    S3ATestUtils.callQuietly(LOG,
        () -> fc != null && fc.delete(testRootPath, true));
  }

  @Override
  protected void verifyReadBytes(FileSystem.Statistics stats) {
    // one blockSize for read, one for pread
    Assert.assertEquals(2 * blockSize, stats.getBytesRead());
  }

  /**
   * A method to verify the bytes written.
   * <br>
   * NOTE: if Client side encryption is enabled, expected bytes written
   * should increase by 16(padding of data) + bytes for the key ID set + 94(KMS
   * key generation) in case of storage type CryptoStorageMode as
   * ObjectMetadata(Default). If Crypto Storage mode is instruction file then
   * add additional bytes as that file is stored separately and would account
   * for bytes written.
   *
   * @param stats Filesystem statistics.
   */
  @Override
  protected void verifyWrittenBytes(FileSystem.Statistics stats)
      throws IOException {
    //No extra bytes are written
    long expectedBlockSize = blockSize;
    if (S3AEncryptionMethods.CSE_KMS.getMethod()
        .equals(getEncryptionAlgorithm(getTestBucketName(conf), conf)
            .getMethod())) {
      String keyId = getS3EncryptionKey(getTestBucketName(conf), conf);
      // Adding padding length and KMS key generation bytes written.
      expectedBlockSize += CSE_PADDING_LENGTH + keyId.getBytes().length +
          KMS_KEY_GENERATION_REQUEST_PARAMS_BYTES_WRITTEN;
    }
    Assert.assertEquals("Mismatch in bytes written", expectedBlockSize,
        stats.getBytesWritten());
  }

  @Override
  protected URI getFsUri() {
    return fc.getHomeDirectory().toUri();
  }

}
