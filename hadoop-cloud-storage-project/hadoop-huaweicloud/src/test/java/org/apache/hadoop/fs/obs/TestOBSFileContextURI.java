/*
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

package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileContextURIBase;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assume;
import org.junit.BeforeClass;

import java.net.URI;

/**
 * <p>
 * A collection of tests for the {@link FileContext} to test path names passed
 * as URIs. This test should be used for testing an instance of FileContext that
 * has been initialized to a specific default FileSystem such a LocalFileSystem,
 * HDFS,OBS, etc, and where path names are passed that are URIs in a different
 * FileSystem.
 * </p>
 *
 * <p>
 * To test a given {@link FileSystem} implementation create a subclass of this
 * test and override {@link #setUp()} to initialize the <code>fc1</code> and
 * <code>fc2</code>
 * <p>
 * The tests will do operations on fc1 that use a URI in fc2
 * <p>
 * {@link FileContext} instance variable.
 * </p>
 */
public class TestOBSFileContextURI extends FileContextURIBase {

  @BeforeClass
  public static void skipTestCheck() {
    Assume.assumeTrue(OBSContract.isContractTestEnabled());
  }

  @Override
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.addResource(OBSContract.CONTRACT_XML);
    String fileSystem = conf.get(OBSTestConstants.TEST_FS_OBS_NAME);
    if (fileSystem == null || fileSystem.trim().length() == 0) {
      throw new Exception("Default file system not configured.");
    }

    URI uri = new URI(fileSystem);
    FileSystem fs = OBSTestUtils.createTestFileSystem(conf);
    fc1 = FileContext.getFileContext(new DelegateToFileSystem(uri, fs,
        conf, fs.getScheme(), false) {
    }, conf);

    fc2 = FileContext.getFileContext(new DelegateToFileSystem(uri, fs,
        conf, fs.getScheme(), false) {
    }, conf);
    super.setUp();
  }

  @Override
  public void testMkdirsFailsForSubdirectoryOfExistingFile() {
    Assume.assumeTrue("unsupport.", false);
  }

  @Override
  public void testFileStatus() {
    Assume.assumeTrue("unsupport.", false);
  }

}
