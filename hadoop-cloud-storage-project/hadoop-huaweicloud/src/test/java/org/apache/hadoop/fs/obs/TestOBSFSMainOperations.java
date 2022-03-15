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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.TestFSMainOperationsLocalFileSystem;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;

/**
 * <p>
 * A collection of tests for the {@link FileSystem}. This test should be used
 * for testing an instance of FileSystem that has been initialized to a specific
 * default FileSystem such a LocalFileSystem, HDFS,OBS, etc.
 * </p>
 * <p>
 * To test a given {@link FileSystem} implementation create a subclass of this
 * test and override {@link #setUp()} to initialize the <code>fSys</code> {@link
 * FileSystem} instance variable.
 * <p>
 * Since this a junit 4 you can also do a single setup before the start of any
 * tests. E.g.
 *
 *
 * </p>
 */
public class TestOBSFSMainOperations extends
    TestFSMainOperationsLocalFileSystem {

  @Override
  @Before
  public void setUp() throws Exception {
    skipTestCheck();
    Configuration conf = new Configuration();
    conf.addResource(OBSContract.CONTRACT_XML);
    fSys = OBSTestUtils.createTestFileSystem(conf);
  }

  @Override
  public void testWorkingDirectory() {
    Assume.assumeTrue("unspport.", false);
  }

  @Override
  public void testListStatusThrowsExceptionForUnreadableDir() {
    Assume.assumeTrue("unspport.", false);
  }

  @Override
  public void testRenameDirectoryToItself() {
    Assume.assumeTrue("unspport.", false);
  }

  @Override
  public void testGlobStatusThrowsExceptionForUnreadableDir() {
    Assume.assumeTrue("unspport.", false);
  }

  @Override
  public void testRenameFileToItself() {
    Assume.assumeTrue("unspport.", false);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if(fSys != null) {
      super.tearDown();
    }
  }

  public void skipTestCheck() {
    Assume.assumeTrue(OBSContract.isContractTestEnabled());
  }
}
