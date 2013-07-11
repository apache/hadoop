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

import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

public class TestSymlinkHdfsFileContext extends TestSymlinkHdfs {

  private static FileContext fc;

  @BeforeClass
  public static void testSetup() throws Exception {
    fc = FileContext.getFileContext(cluster.getURI(0));
    wrapper = new FileContextTestWrapper(fc, "/tmp/TestSymlinkHdfsFileContext");
  }

  @Test(timeout=1000)
  /** Test access a symlink using AbstractFileSystem */
  public void testAccessLinkFromAbstractFileSystem() throws IOException {
    Path file = new Path(testBaseDir1(), "file");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file);
    wrapper.createSymlink(file, link, false);
    try {
      AbstractFileSystem afs = fc.getDefaultFileSystem();
      afs.open(link);
      fail("Opened a link using AFS");
    } catch (UnresolvedLinkException x) {
      // Expected
    }
  }
}
