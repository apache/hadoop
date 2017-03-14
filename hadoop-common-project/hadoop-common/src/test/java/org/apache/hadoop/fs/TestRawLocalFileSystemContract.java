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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Shell;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test filesystem contracts with {@link RawLocalFileSystem}.
 * Root directory related tests from super class will work into target
 * directory since we have no permission to write / on local filesystem.
 */
public class TestRawLocalFileSystemContract extends FileSystemContractBaseTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRawLocalFileSystemContract.class);

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    fs = FileSystem.getLocal(conf).getRawFileSystem();
  }

  /**
   * Actually rename is supported in RawLocalFileSystem but
   * it works different as the other filesystems. Short term we do not test it.
   * Please check HADOOP-13082.
   * @return true if rename supported so rename related tests will run
   */
  @Override
  protected boolean renameSupported() {
    return false;
  }

  @Override
  public String getDefaultWorkingDirectory() {
    return fs.getWorkingDirectory().toUri().getPath();
  }

  @Override
  protected Path path(String pathString) {
    // For testWorkingDirectory
    if (pathString.equals(getDefaultWorkingDirectory()) ||
        pathString.equals(".") || pathString.equals("..")) {
      return super.path(pathString);
    }

    return new Path(GenericTestUtils.getTempPath(pathString)).
        makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }

  @Override
  protected boolean filesystemIsCaseSensitive() {
    return !(Shell.WINDOWS || Shell.MAC);
  }
}