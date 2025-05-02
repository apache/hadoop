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

package org.apache.hadoop.fs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;

import org.apache.hadoop.fs.FileSystem.Statistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * <p>
 *    Tests the File Context Statistics for {@link LocalFileSystem}
 * </p>
 */
public class TestLocalFsFCStatistics extends FCStatisticsBaseTest {
  
  static final String LOCAL_FS_ROOT_URI =  "file:///tmp/test";

  @BeforeEach
  public void setUp() throws Exception {
    fc = FileContext.getLocalFSFileContext();
    fc.mkdir(fileContextTestHelper.getTestRootPath(fc, "test"), FileContext.DEFAULT_PERM, true);
  }

  @AfterEach
  public void tearDown() throws Exception {
    fc.delete(fileContextTestHelper.getTestRootPath(fc, "test"), true);
  }

  @Override
  protected void verifyReadBytes(Statistics stats) {
    // one blockSize for read, one for pread
    assertEquals(2*blockSize, stats.getBytesRead());
  }

  @Override
  protected void verifyWrittenBytes(Statistics stats) {
    //Extra 12 bytes are written apart from the block.
    assertEquals(blockSize + 12, stats.getBytesWritten());
  }
  
  @Override
  protected URI getFsUri() {
    return URI.create(LOCAL_FS_ROOT_URI);
  }

}
