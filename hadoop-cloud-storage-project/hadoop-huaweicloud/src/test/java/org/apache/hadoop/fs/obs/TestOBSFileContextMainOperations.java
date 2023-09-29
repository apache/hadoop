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
import org.apache.hadoop.fs.FileContextMainOperationsBaseTest;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;

/**
 * Rename test cases on obs file system.
 */
public class TestOBSFileContextMainOperations extends
    FileContextMainOperationsBaseTest {

  @BeforeClass
  public static void skipTestCheck() {
    Assume.assumeTrue(OBSContract.isContractTestEnabled());
  }

  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
      "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
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

  @Override
  protected boolean listCorruptedBlocksSupported() {
    return false;
  }

  @Override
  @Test
  public void testSetVerifyChecksum() {
    Assume.assumeTrue("unsupport.", false);
  }

  @Override
  public void testMkdirsFailsForSubdirectoryOfExistingFile() {
    Assume.assumeTrue("unsupport.", false);
  }
}
