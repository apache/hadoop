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
import java.net.URISyntaxException;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TestFileContext;
import org.apache.hadoop.fs.UnsupportedFileSystemException;

import static org.junit.Assert.assertEquals;

/**
 * Implementation of TestFileContext for S3a.
 */
public class ITestS3AFileContext extends TestFileContext {

  @Test
  public void testScheme()
      throws URISyntaxException, UnsupportedFileSystemException {
    Configuration conf = new Configuration();
    URI uri = new URI("s3://mybucket/path");
    conf.set("fs.AbstractFileSystem.s3.impl",
        "org.apache.hadoop.fs.s3a.S3A");
    FileContext fc = FileContext.getFileContext(uri, conf);
    assertEquals("s3", fc.getDefaultFileSystem().getUri().getScheme());
    Path path = fc.makeQualified(new Path("tmp/path"));
    assertEquals("s3", path.toUri().getScheme());
  }
}
