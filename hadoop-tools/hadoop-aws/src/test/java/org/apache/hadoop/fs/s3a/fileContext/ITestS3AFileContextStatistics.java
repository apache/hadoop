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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FCStatisticsBaseTest;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

/**
 * S3a implementation of FCStatisticsBaseTest.
 */
public class ITestS3AFileContextStatistics extends FCStatisticsBaseTest {

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    fc = S3ATestUtils.createTestFileContext(conf);
    fc.mkdir(fileContextTestHelper.getTestRootPath(fc, "test"),
        FileContext.DEFAULT_PERM, true);
    FileContext.clearStatistics();
  }

  @After
  public void tearDown() throws Exception {
    if (fc != null) {
      fc.delete(fileContextTestHelper.getTestRootPath(fc, "test"), true);
    }
  }

  @Override
  protected void verifyReadBytes(FileSystem.Statistics stats) {
    // one blockSize for read, one for pread
    Assert.assertEquals(2 * blockSize, stats.getBytesRead());
  }

  @Override
  protected void verifyWrittenBytes(FileSystem.Statistics stats) {
    //No extra bytes are written
    Assert.assertEquals(blockSize, stats.getBytesWritten());
  }

  @Override
  protected URI getFsUri() {
    return fc.getHomeDirectory().toUri();
  }

}
