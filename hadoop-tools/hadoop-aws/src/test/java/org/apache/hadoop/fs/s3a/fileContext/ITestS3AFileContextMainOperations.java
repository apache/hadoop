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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContextMainOperationsBaseTest;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * S3A implementation of FileContextMainOperationsBaseTest.
 */
public class ITestS3AFileContextMainOperations
        extends FileContextMainOperationsBaseTest {

  @Before
  public void setUp() throws IOException, Exception {
    Configuration conf = new Configuration();
    fc = S3ATestUtils.createTestFileContext(conf);
    super.setUp();
  }

  @Override
  protected boolean listCorruptedBlocksSupported() {
    return false;
  }

  @Test
  @Ignore
  public void testCreateFlagAppendExistingFile() throws IOException {
    //append not supported, so test removed
  }

  @Test
  @Ignore
  public void testCreateFlagCreateAppendExistingFile() throws IOException {
    //append not supported, so test removed
  }

  @Test
  @Ignore
  public void testSetVerifyChecksum() throws IOException {
    //checksums ignored, so test removed
  }

}
