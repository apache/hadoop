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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFsShellTouch {
  static final Log LOG = LogFactory.getLog(TestFsShellTouch.class);

  static FsShell shell;
  static LocalFileSystem lfs;
  static Path testRootDir;

  @BeforeClass
  public static void setup() throws Exception {
    Configuration conf = new Configuration();
    shell = new FsShell(conf);
    lfs = FileSystem.getLocal(conf);
    testRootDir = lfs.makeQualified(
        new Path(GenericTestUtils.getTempPath("testFsShell")));

    lfs.mkdirs(testRootDir);
    lfs.setWorkingDirectory(testRootDir);
  }

  @Before
  public void prepFiles() throws Exception {
    lfs.setVerifyChecksum(true);
    lfs.setWriteChecksum(true);
  }

  private int shellRun(String... args) throws Exception {
    int exitCode = shell.run(args);
    LOG.info("exit " + exitCode + " - " + StringUtils.join(" ", args));
    return exitCode;
  }

  @Test
  public void testTouchz() throws Exception {
    // Ensure newFile does not exist
    final String newFileName = "newFile";
    final Path newFile = new Path(newFileName);
    lfs.delete(newFile, true);
    assertThat(lfs.exists(newFile), is(false));

    assertThat("Expected successful touchz on a new file",
        shellRun("-touchz", newFileName), is(0));
    shellRun("-ls", newFileName);

    assertThat("Expected successful touchz on an existing zero-length file",
        shellRun("-touchz", newFileName), is(0));

    // Ensure noDir does not exist
    final String noDirName = "noDir";
    final Path noDir = new Path(noDirName);
    lfs.delete(noDir, true);
    assertThat(lfs.exists(noDir), is(false));

    assertThat("Expected failed touchz in a non-existent directory",
        shellRun("-touchz", noDirName + "/foo"), is(not(0)));
  }
}
