/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.fileContext;

import java.io.IOException;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContextMainOperationsBaseTest;
import org.apache.hadoop.fs.FileContextTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.test.tags.IntegrationTest;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.setPerformanceFlags;

/**
 * S3A implementation of FileContextMainOperationsBaseTest.
 */
@IntegrationTest
public class ITestS3AFileContextMainOperations
    extends FileContextMainOperationsBaseTest {


  @BeforeEach
  public void setUp() throws IOException, Exception {
    Configuration conf = setPerformanceFlags(
        new Configuration(),
        "");

    fc = S3ATestUtils.createTestFileContext(conf);
    super.setUp();
  }


  /**
   * Called before even our own constructor and fields are
   * inited.
   * @return a test helper using the s3a test path.
   */
  @Override
  protected FileContextTestHelper createFileContextHelper() {
    Path testPath =
        S3ATestUtils.createTestPath(new Path("/" + UUID.randomUUID()));
    return new FileContextTestHelper(testPath.toUri().toString());
  }

  @Override
  protected boolean listCorruptedBlocksSupported() {
    return false;
  }

  @Test
  @Disabled
  public void testCreateFlagAppendExistingFile() throws IOException {
    //append not supported, so test removed
  }

  @Test
  @Disabled
  public void testCreateFlagCreateAppendExistingFile() throws IOException {
    //append not supported, so test removed
  }

  @Test
  @Disabled
  public void testBuilderCreateAppendExistingFile() throws IOException {
    // not supported
  }

  @Test
  @Disabled
  public void testSetVerifyChecksum() throws IOException {
    //checksums ignored, so test removed
  }

}
