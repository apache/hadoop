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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test FsShell -ls command.
 */
public class TestFsShellList {
  private static Configuration conf;
  private static FsShell shell;
  private static LocalFileSystem lfs;
  private static Path testRootDir;

  @BeforeClass
  public static void setup() throws Exception {
    conf = new Configuration();
    shell = new FsShell(conf);
    lfs = FileSystem.getLocal(conf);
    lfs.setVerifyChecksum(true);
    lfs.setWriteChecksum(true);

    String root = System.getProperty("test.build.data", "test/build/data");
    testRootDir = lfs.makeQualified(new Path(root, "testFsShellList"));
    assertThat(lfs.mkdirs(testRootDir), is(true));
  }

  @AfterClass
  public static void teardown() throws Exception {
    lfs.delete(testRootDir, true);
  }

  private void createFile(Path filePath) throws Exception {
    FSDataOutputStream out = lfs.create(filePath);
    out.writeChars("I am " + filePath);
    out.close();
    assertThat(lfs.exists(lfs.getChecksumFile(filePath)), is(true));
  }

  @Test
  public void testList() throws Exception {
    createFile(new Path(testRootDir, "abc"));
    String[] lsArgv = new String[]{"-ls", testRootDir.toString()};
    assertThat(shell.run(lsArgv), is(0));

    createFile(new Path(testRootDir, "abc\bd\tef"));
    createFile(new Path(testRootDir, "ghi"));
    createFile(new Path(testRootDir, "qq\r123"));
    lsArgv = new String[]{"-ls", testRootDir.toString()};
    assertThat(shell.run(lsArgv), is(0));

    lsArgv = new String[]{"-ls", "-q", testRootDir.toString()};
    assertThat(shell.run(lsArgv), is(0));
  }
}
