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
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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
  private static final Path FROM = new Path("d1", "f1");
  private static final Path TO = new Path("d2", "f2");
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
    testDir = new Path(
        System.getProperty("test.build.data", "build/test/data") + "/testStat"
    );
    // don't want scheme on the path, just an absolute path
    testDir = new Path(fs.makeQualified(testDir).toUri().getPath());

    FileSystem.setDefaultUri(conf, fs.getUri());
    fs.setWorkingDirectory(testDir);
    fs.mkdirs(new Path("d1"));
    fs.mkdirs(new Path("d2"));
    fs.createNewFile(FROM);

    FSDataOutputStream output = fs.create(FROM, true);
    for(int i = 0; i < 100; ++i) {
        output.writeInt(i);
        output.writeChar('\n');
    }
    output.close();
    fs.setTimes(FROM, MODIFICATION_TIME, 0);
    fs.setPermission(FROM, PERMISSIONS);
    fs.setTimes(new Path("d1"), MODIFICATION_TIME, 0);
    fs.setPermission(new Path("d1"), PERMISSIONS);
  }

  @After
  public void cleanup() throws Exception {
    fs.delete(testDir, true);
    fs.close();
  }

  private void assertAttributesPreserved() throws IOException {
    assertEquals(MODIFICATION_TIME, fs.getFileStatus(TO).getModificationTime());
    assertEquals(PERMISSIONS, fs.getFileStatus(TO).getPermission());
  }

  private void assertAttributesChanged() throws IOException {
      assertTrue(MODIFICATION_TIME != fs.getFileStatus(TO).getModificationTime());
      assertTrue(!PERMISSIONS.equals(fs.getFileStatus(TO).getPermission()));
  }

  private void run(CommandWithDestination cmd, String... args) {
    cmd.setConf(conf);
    assertEquals(0, cmd.run(args));
  }

  @Test(timeout = 10000)
  public void testPutWithP() throws Exception {
    run(new Put(), "-p", FROM.toString(), TO.toString());
    assertAttributesPreserved();
  }

  @Test(timeout = 10000)
  public void testPutWithoutP() throws Exception {
    run(new Put(), FROM.toString(), TO.toString());
    assertAttributesChanged();
  }

  @Test(timeout = 10000)
  public void testGetWithP() throws Exception {
    run(new Get(), "-p", FROM.toString(), TO.toString());
    assertAttributesPreserved();
  }

  @Test(timeout = 10000)
  public void testGetWithoutP() throws Exception {
    run(new Get(), FROM.toString(), TO.toString());
    assertAttributesChanged();
  }

  @Test(timeout = 10000)
  public void testCpWithP() throws Exception {
      run(new Cp(), "-p", FROM.toString(), TO.toString());
      assertAttributesPreserved();
  }

  @Test(timeout = 10000)
  public void testCpWithoutP() throws Exception {
      run(new Cp(), FROM.toString(), TO.toString());
      assertAttributesChanged();
  }

  @Test(timeout = 10000)
  public void testDirectoryCpWithP() throws Exception {
    run(new Cp(), "-p", "d1", "d3");
    assertEquals(fs.getFileStatus(new Path("d1")).getModificationTime(),
        fs.getFileStatus(new Path("d3")).getModificationTime());
    assertEquals(fs.getFileStatus(new Path("d1")).getPermission(),
        fs.getFileStatus(new Path("d3")).getPermission());
  }

  @Test(timeout = 10000)
  public void testDirectoryCpWithoutP() throws Exception {
    run(new Cp(), "d1", "d4");
    assertTrue(fs.getFileStatus(new Path("d1")).getModificationTime() !=
        fs.getFileStatus(new Path("d4")).getModificationTime());
    assertTrue(!fs.getFileStatus(new Path("d1")).getPermission()
        .equals(fs.getFileStatus(new Path("d4")).getPermission()));
  }
}
