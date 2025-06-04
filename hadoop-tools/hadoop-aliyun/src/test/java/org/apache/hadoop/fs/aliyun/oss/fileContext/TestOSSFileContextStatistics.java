/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss.fileContext;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FCStatisticsBaseTest;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.URI;

/**
 * OSS implementation of FCStatisticsBaseTest.
 */
public class TestOSSFileContextStatistics extends FCStatisticsBaseTest {

  @BeforeEach
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    fc = AliyunOSSTestUtils.createTestFileContext(conf);
    fc.mkdir(fileContextTestHelper.getTestRootPath(fc, "test"),
        FileContext.DEFAULT_PERM, true);
    FileContext.clearStatistics();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (fc != null) {
      fc.delete(fileContextTestHelper.getTestRootPath(fc, "test"), true);
    }
  }

  @Override
  protected void verifyReadBytes(FileSystem.Statistics stats) {
    // one blockSize for read, one for pread
    assertEquals(2 * blockSize, stats.getBytesRead());
  }

  @Override
  protected void verifyWrittenBytes(FileSystem.Statistics stats) {
    // no extra bytes are written
    assertEquals(blockSize, stats.getBytesWritten());
  }

  @Override
  protected URI getFsUri() {
    return fc.getHomeDirectory().toUri();
  }
}
