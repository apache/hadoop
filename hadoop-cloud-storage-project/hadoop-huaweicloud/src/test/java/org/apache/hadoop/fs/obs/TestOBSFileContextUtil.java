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
import org.apache.hadoop.fs.FileContextUtilBase;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assume;
import org.junit.BeforeClass;

import java.net.URI;

/**
 * <p>
 * A collection of Util tests for the {@link FileContext#util()}. This test
 * should be used for testing an instance of {@link FileContext#util()} that has
 * been initialized to a specific default FileSystem such a LocalFileSystem,
 * HDFS,OBS, etc.
 * </p>
 * <p>
 * To test a given {@link FileSystem} implementation create a subclass of this
 * test and override {@link #setUp()} to initialize the <code>fc</code> {@link
 * FileContext} instance variable.
 *
 * </p>
 */
public class TestOBSFileContextUtil extends FileContextUtilBase {

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
    fc = FileContext.getFileContext(new DelegateToFileSystem(uri, fs,
        conf, fs.getScheme(), false) {
    }, conf);
    super.setUp();
  }
}
