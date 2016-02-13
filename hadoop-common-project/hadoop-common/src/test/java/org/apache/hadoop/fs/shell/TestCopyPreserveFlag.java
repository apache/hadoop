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
package org.apache.hadoop.fs.shell;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.CopyCommands.Cp;
import org.apache.hadoop.fs.shell.CopyCommands.Get;
import org.apache.hadoop.fs.shell.CopyCommands.Put;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCopyPreserveFlag {
  private static final int MODIFICATION_TIME = 12345000;
  private static final int ACCESS_TIME = 23456000;
  private static final Path DIR_FROM = new Path("d0");
  private static final Path DIR_TO1 = new Path("d1");
  private static final Path DIR_TO2 = new Path("d2");
  private static final Path FROM = new Path(DIR_FROM, "f0");
  private static final Path TO = new Path(DIR_TO1, "f1");
  private static final FsPermission PERMISSIONS = new FsPermission(
    FsAction.ALL,
    FsAction.EXECUTE,
    FsAction.READ_WRITE);

  private FileSystem fs;
  private Path testDir;
  private Configuration conf;

  @Before
  public void initialize() throws Exception {
    conf = new Configuration(false);
    conf.set("fs.file.impl", LocalFileSystem.class.getName());
    fs = FileSystem.getLocal(conf);
    testDir = new FileSystemTestHelper().getTestRootPath(fs);
    // don't want scheme on the path, just an absolute path
    testDir = new Path(fs.makeQualified(testDir).toUri().getPath());

    FileSystem.setDefaultUri(conf, fs.getUri());
    fs.setWorkingDirectory(testDir);
    fs.mkdirs(DIR_FROM);
    fs.mkdirs(DIR_TO1);
    fs.createNewFile(FROM);

    FSDataOutputStream output = fs.create(FROM, true);
    for(int i = 0; i < 100; ++i) {
        output.writeInt(i);
        output.writeChar('\n');
    }
    output.close();
    fs.setTimes(FROM, MODIFICATION_TIME, ACCESS_TIME);
    fs.setPermission(FROM, PERMISSIONS);
    fs.setTimes(DIR_FROM, MODIFICATION_TIME, ACCESS_TIME);
    fs.setPermission(DIR_FROM, PERMISSIONS);
  }

  @After
  public void cleanup() throws Exception {
    fs.delete(testDir, true);
    fs.close();
  }

  private void assertAttributesPreserved(Path to) throws IOException {
    FileStatus status = fs.getFileStatus(to);
    assertEquals(MODIFICATION_TIME, status.getModificationTime());
    assertEquals(ACCESS_TIME, status.getAccessTime());
    assertEquals(PERMISSIONS, status.getPermission());
  }

  private void assertAttributesChanged(Path to) throws IOException {
    FileStatus status = fs.getFileStatus(to);
    assertNotEquals(MODIFICATION_TIME, status.getModificationTime());
    assertNotEquals(ACCESS_TIME, status.getAccessTime());
    assertNotEquals(PERMISSIONS, status.getPermission());
  }

  private void run(CommandWithDestination cmd, String... args) {
    cmd.setConf(conf);
    assertEquals(0, cmd.run(args));
  }

  @Test(timeout = 10000)
  public void testPutWithP() throws Exception {
    run(new Put(), "-p", FROM.toString(), TO.toString());
    assertAttributesPreserved(TO);
  }

  @Test(timeout = 10000)
  public void testPutWithoutP() throws Exception {
    run(new Put(), FROM.toString(), TO.toString());
    assertAttributesChanged(TO);
  }

  @Test(timeout = 10000)
  public void testGetWithP() throws Exception {
    run(new Get(), "-p", FROM.toString(), TO.toString());
    assertAttributesPreserved(TO);
  }

  @Test(timeout = 10000)
  public void testGetWithoutP() throws Exception {
    run(new Get(), FROM.toString(), TO.toString());
    assertAttributesChanged(TO);
  }

  @Test(timeout = 10000)
  public void testCpWithP() throws Exception {
      run(new Cp(), "-p", FROM.toString(), TO.toString());
      assertAttributesPreserved(TO);
  }

  @Test(timeout = 10000)
  public void testCpWithoutP() throws Exception {
      run(new Cp(), FROM.toString(), TO.toString());
      assertAttributesChanged(TO);
  }

  @Test(timeout = 10000)
  public void testDirectoryCpWithP() throws Exception {
    run(new Cp(), "-p", DIR_FROM.toString(), DIR_TO2.toString());
    assertAttributesPreserved(DIR_TO2);
  }

  @Test(timeout = 10000)
  public void testDirectoryCpWithoutP() throws Exception {
    run(new Cp(), DIR_FROM.toString(), DIR_TO2.toString());
    assertAttributesChanged(DIR_TO2);
  }
}
