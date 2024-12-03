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

import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContextCreateMkdirBaseTest;
import org.apache.hadoop.fs.s3a.S3ATestUtils;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.setPerformanceFlags;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Extends FileContextCreateMkdirBaseTest for a S3a FileContext with
 * create performance mode.
 */
public class ITestS3AFileContextCreateMkdirCreatePerf
        extends FileContextCreateMkdirBaseTest {

  @Before
  public void setUp() throws Exception {
    Configuration conf = setPerformanceFlags(
        new Configuration(),
        "mkdir");
    fc = S3ATestUtils.createTestFileContext(conf);
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    if (fc != null) {
      super.tearDown();
    }
  }

  @Test
  public void testMkdirRecursiveWithExistingFile() throws Exception {
    intercept(
        AssertionError.class,
        MKDIR_FILE_PRESENT_ERROR,
        "Dir creation should not have failed. "
            + "Creation performance mode is expected "
            + "to create dir without checking file "
            + "status of parent dir.",
        super::testMkdirRecursiveWithExistingFile);
  }

}
