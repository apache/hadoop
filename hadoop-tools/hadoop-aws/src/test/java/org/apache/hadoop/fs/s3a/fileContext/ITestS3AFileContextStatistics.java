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

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FCStatisticsBaseTest;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.auth.STSClientFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfAnalyticsAcceleratorEnabled;

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
    // Analytics accelerator currently does not support IOStatistics, this will be added as
    // part of https://issues.apache.org/jira/browse/HADOOP-19364
    skipIfAnalyticsAcceleratorEnabled(conf,
        "Analytics Accelerator currently does not support stream statistics");
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
   * @param stats Filesystem statistics.
   */
  @Override
  protected void verifyWrittenBytes(FileSystem.Statistics stats) {
    //No extra bytes are written
    Assert.assertEquals("Mismatch in bytes written", blockSize,
        stats.getBytesWritten());
  }

  @Override
  protected URI getFsUri() {
    return fc.getHomeDirectory().toUri();
  }

}
