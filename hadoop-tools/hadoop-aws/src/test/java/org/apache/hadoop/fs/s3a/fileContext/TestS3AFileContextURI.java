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
import org.apache.hadoop.fs.FileContextURIBase;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * S3a implementation of FileContextURIBase
 */
public class TestS3AFileContextURI extends FileContextURIBase {

  @Before
  public void setUp() throws IOException, Exception {
    Configuration conf = new Configuration();
    fc1 = S3ATestUtils.createTestFileContext(conf);
    fc2 = S3ATestUtils.createTestFileContext(conf); //different object, same FS
    super.setUp();
  }

  @Test
  @Ignore
  public void testFileStatus() throws IOException {
    //test disabled (the statistics tested with this method are not relevant for an S3FS)
  }

}
