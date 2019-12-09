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
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.createTestFileSystem;

/**
 * S3a implementation of FileContextURIBase.
 */
public class ITestS3AFileContextURI extends FileContextURIBase {

  private Configuration conf;
  private boolean hasMetadataStore;

  @Before
  public void setUp() throws IOException, Exception {
    conf = new Configuration();
    try(S3AFileSystem s3aFS = createTestFileSystem(conf)) {
      hasMetadataStore = s3aFS.hasMetadataStore();
    }
    fc1 = S3ATestUtils.createTestFileContext(conf);
    fc2 = S3ATestUtils.createTestFileContext(conf); //different object, same FS
    super.setUp();
  }

  @Test
  @Ignore
  public void testFileStatus() throws IOException {
    // test disabled
    // (the statistics tested with this method are not relevant for an S3FS)
  }

  @Test
  @Override
  public void testModificationTime() throws IOException {
    // skip modtime tests as there may be some inconsistency during creation
    assume("modification time tests are skipped", !hasMetadataStore);
    super.testModificationTime();
  }
}
