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
 *
 */

package org.apache.hadoop.fs.adl.live;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileContextMainOperationsBaseTest;
import org.apache.hadoop.fs.FileContextTestHelper;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assume;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * Verify Adls file system adhere to Hadoop file system contract using bunch of
 * available test in FileContextMainOperationsBaseTest.
 */
public class TestAdlWebHdfsFileContextMainOperationsLive
    extends FileContextMainOperationsBaseTest {

  private static final String KEY_FILE_SYSTEM = "fs.defaultFS";

  @BeforeClass
  public static void skipTestCheck() {
    Assume.assumeTrue(AdlStorageConfiguration.isContractTestEnabled());
  }

  @Override
  public void setUp() throws Exception {
    Configuration conf = AdlStorageConfiguration.getConfiguration();
    String fileSystem = conf.get(KEY_FILE_SYSTEM);
    if (fileSystem == null || fileSystem.trim().length() == 0) {
      throw new Exception("Default file system not configured.");
    }
    URI uri = new URI(fileSystem);
    FileSystem fs = AdlStorageConfiguration.createAdlStorageConnector();
    fc = FileContext.getFileContext(
        new DelegateToFileSystem(uri, fs, conf, fs.getScheme(), false) {
        }, conf);
    super.setUp();
  }

  /**
   * Required to override since the getRandmizedTestDir on Windows generates
   * absolute path of the local file path which contains ":" character.
   * Example file system path generated is "adl://<FileSystem Path>/d:/a/b/c
   *
   * Adls does not support : character in the path hence overriding to remove
   * unsupported character from the path.
   *
   * @return FileContextTestHelper
   */
  @Override
  protected FileContextTestHelper createFileContextHelper() {
    return new FileContextTestHelper(
        new File(RandomStringUtils.randomAlphanumeric(10)).getAbsolutePath()
            .replaceAll(":", ""));
  }

  @Override
  protected boolean listCorruptedBlocksSupported() {
    return false;
  }

  @Override
  public void testUnsupportedSymlink() throws IOException {
    Assume.assumeTrue("Symbolic link are not supported by Adls", false);
  }

  /**
   * In case this test is causing failure due to
   * java.lang.RuntimeException: java.io.FileNotFoundException: Hadoop bin
   * directory does not exist: <path>\hadoop-common-project
   * \hadoop-common\target\bin -see https://wiki.apache
   * .org/hadoop/WindowsProblems. then do build the hadoop dependencies
   * otherwise mark this test as skip.
   */
  @Override
  public void testWorkingDirectory() throws Exception {
    super.testWorkingDirectory();
  }
}
