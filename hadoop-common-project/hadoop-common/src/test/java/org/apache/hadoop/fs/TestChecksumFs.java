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

import java.io.IOException;
import java.util.EnumSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.fs.CreateFlag.*;

/**
 * This class tests the functionality of ChecksumFs.
 */
public class TestChecksumFs extends HadoopTestBase {
  private Configuration conf;
  private Path testRootDirPath;
  private FileContext fc;

  @Before
  public void setUp() throws Exception {
    conf = getTestConfiguration();
    fc = FileContext.getFileContext(conf);
    testRootDirPath = new Path(GenericTestUtils.getRandomizedTestDir()
        .getAbsolutePath());
    mkdirs(testRootDirPath);
  }

  @After
  public void tearDown() throws Exception {
    if (fc != null) {
      fc.delete(testRootDirPath, true);
    }
  }

  @Test
  public void testRenameFileToFile() throws Exception {
    Path srcPath = new Path(testRootDirPath, "testRenameSrc");
    Path dstPath = new Path(testRootDirPath, "testRenameDst");
    verifyRename(srcPath, dstPath, false);
  }

  @Test
  public void testRenameFileToFileWithOverwrite() throws Exception {
    Path srcPath = new Path(testRootDirPath, "testRenameSrc");
    Path dstPath = new Path(testRootDirPath, "testRenameDst");
    verifyRename(srcPath, dstPath, true);
  }

  @Test
  public void testRenameFileIntoDirFile() throws Exception {
    Path srcPath = new Path(testRootDirPath, "testRenameSrc");
    Path dstPath = new Path(testRootDirPath, "testRenameDir/testRenameDst");
    mkdirs(dstPath);
    verifyRename(srcPath, dstPath, false);
  }

  @Test
  public void testRenameFileIntoDirFileWithOverwrite() throws Exception {
    Path srcPath = new Path(testRootDirPath, "testRenameSrc");
    Path dstPath = new Path(testRootDirPath, "testRenameDir/testRenameDst");
    mkdirs(dstPath);
    verifyRename(srcPath, dstPath, true);
  }

  private void verifyRename(Path srcPath, Path dstPath,
      boolean overwrite) throws Exception {
    ChecksumFs fs = (ChecksumFs) fc.getDefaultFileSystem();

    fs.delete(srcPath, true);
    fs.delete(dstPath, true);

    Options.Rename renameOpt = Options.Rename.NONE;
    if (overwrite) {
      renameOpt = Options.Rename.OVERWRITE;
      createTestFile(fs, dstPath, 2);
    }

    // ensure file + checksum are moved
    createTestFile(fs, srcPath, 1);
    assertTrue("Checksum file doesn't exist for source file - " + srcPath,
        fc.util().exists(fs.getChecksumFile(srcPath)));
    fs.rename(srcPath, dstPath, renameOpt);
    assertTrue("Checksum file doesn't exist for dest file - " + srcPath,
        fc.util().exists(fs.getChecksumFile(dstPath)));
    try (FSDataInputStream is = fs.open(dstPath)) {
      assertEquals(1, is.readInt());
    }
  }

  private static Configuration getTestConfiguration() {
    Configuration conf = new Configuration(false);
    conf.set("fs.defaultFS", "file:///");
    conf.setClass("fs.AbstractFileSystem.file.impl",
        org.apache.hadoop.fs.local.LocalFs.class,
        org.apache.hadoop.fs.AbstractFileSystem.class);
    return conf;
  }

  private void createTestFile(ChecksumFs fs, Path path, int content)
      throws IOException {
    try (FSDataOutputStream fout = fs.create(path,
        EnumSet.of(CREATE, OVERWRITE),
        Options.CreateOpts.perms(FsPermission.getDefault()))) {
      fout.writeInt(content);
    }
  }

  private void mkdirs(Path dirPath) throws IOException {
    fc.mkdir(dirPath, FileContext.DEFAULT_PERM, true);
  }
}
