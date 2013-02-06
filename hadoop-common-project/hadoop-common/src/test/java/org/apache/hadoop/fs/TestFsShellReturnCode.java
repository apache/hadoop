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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ftpserver.command.impl.STAT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.FsCommand;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.io.IOUtils;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test validates that chmod, chown, chgrp returning correct exit codes
 * 
 */
public class TestFsShellReturnCode {
  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.fs.TestFsShellReturnCode");

  private static final Configuration conf = new Configuration();
  private static FileSystem fileSys;
  private static FsShell fsShell;
  
  @BeforeClass
  public static void setup() throws IOException {
    conf.setClass("fs.file.impl", LocalFileSystemExtn.class, LocalFileSystem.class);
    fileSys = FileSystem.get(conf);
    fsShell = new FsShell(conf);
  }
  
  private static String TEST_ROOT_DIR = System.getProperty("test.build.data",
      "build/test/data/testCHReturnCode");

  static void writeFile(FileSystem fs, Path name) throws Exception {
    FSDataOutputStream stm = fs.create(name);
    stm.writeBytes("42\n");
    stm.close();
  }

  private void change(int exit, String owner, String group, String...files)
  throws Exception {
    FileStatus[][] oldStats = new FileStatus[files.length][];
    for (int i=0; i < files.length; i++) {
      oldStats[i] = fileSys.globStatus(new Path(files[i]));
    }
    
    List<String>argv = new LinkedList<String>();
    if (owner != null) {
      argv.add("-chown");

      String chown = owner;
      if (group != null) {
        chown += ":" + group;
        if (group.isEmpty()) group = null; // avoid testing for it later
      }
      argv.add(chown);
    } else {
      argv.add("-chgrp");
      argv.add(group);
    }
    
    Collections.addAll(argv, files);
    
    assertEquals(exit, fsShell.run(argv.toArray(new String[0])));

    for (int i=0; i < files.length; i++) {
      FileStatus[] stats = fileSys.globStatus(new Path(files[i]));
      if (stats != null) {
        for (int j=0; j < stats.length; j++) {
          assertEquals("check owner of " + files[i],
              ((owner != null) ? "STUB-"+owner : oldStats[i][j].getOwner()),
              stats[j].getOwner()
          );
          assertEquals("check group of " + files[i],
              ((group != null) ? "STUB-"+group : oldStats[i][j].getGroup()),
              stats[j].getGroup()
          );        
        }
      }
    }
  }

  /**
   * Test Chmod 1. Create and write file on FS 2. Verify that exit code for
   * chmod on existing file is 0 3. Verify that exit code for chmod on
   * non-existing file is 1 4. Verify that exit code for chmod with glob input
   * on non-existing file is 1 5. Verify that exit code for chmod with glob
   * input on existing file in 0
   * 
   * @throws Exception
   */
  @Test
  public void testChmod() throws Exception {

    final String f1 = TEST_ROOT_DIR + "/" + "testChmod/fileExists";
    final String f2 = TEST_ROOT_DIR + "/" + "testChmod/fileDoesNotExist";
    final String f3 = TEST_ROOT_DIR + "/" + "testChmod/nonExistingfiles*";

    Path p1 = new Path(f1);

    final Path p4 = new Path(TEST_ROOT_DIR + "/" + "testChmod/file1");
    final Path p5 = new Path(TEST_ROOT_DIR + "/" + "testChmod/file2");
    final Path p6 = new Path(TEST_ROOT_DIR + "/" + "testChmod/file3");

    final String f7 = TEST_ROOT_DIR + "/" + "testChmod/file*";

    // create and write test file
    writeFile(fileSys, p1);
    assertTrue(fileSys.exists(p1));

    // Test 1: Test 1: exit code for chmod on existing is 0
    String argv[] = { "-chmod", "777", f1 };
    assertEquals(0, fsShell.run(argv));

    // Test 2: exit code for chmod on non-existing path is 1
    String argv2[] = { "-chmod", "777", f2 };
    assertEquals(1, fsShell.run(argv2));

    // Test 3: exit code for chmod on non-existing path with globbed input is 1
    String argv3[] = { "-chmod", "777", f3 };
    assertEquals(1, fsShell.run(argv3));

    // create required files
    writeFile(fileSys, p4);
    assertTrue(fileSys.exists(p4));
    writeFile(fileSys, p5);
    assertTrue(fileSys.exists(p5));
    writeFile(fileSys, p6);
    assertTrue(fileSys.exists(p6));

    // Test 4: exit code for chmod on existing path with globbed input is 0
    String argv4[] = { "-chmod", "777", f7 };
    assertEquals(0, fsShell.run(argv4));

  }

  /**
   * Test Chown 1. Create and write file on FS 2. Verify that exit code for
   * Chown on existing file is 0 3. Verify that exit code for Chown on
   * non-existing file is 1 4. Verify that exit code for Chown with glob input
   * on non-existing file is 1 5. Verify that exit code for Chown with glob
   * input on existing file in 0
   * 
   * @throws Exception
   */
  @Test
  public void testChown() throws Exception {

    final String f1 = TEST_ROOT_DIR + "/" + "testChown/fileExists";
    final String f2 = TEST_ROOT_DIR + "/" + "testChown/fileDoesNotExist";
    final String f3 = TEST_ROOT_DIR + "/" + "testChown/nonExistingfiles*";

    Path p1 = new Path(f1);

    final Path p4 = new Path(TEST_ROOT_DIR + "/" + "testChown/file1");
    final Path p5 = new Path(TEST_ROOT_DIR + "/" + "testChown/file2");
    final Path p6 = new Path(TEST_ROOT_DIR + "/" + "testChown/file3");

    final String f7 = TEST_ROOT_DIR + "/" + "testChown/file*";

    // create and write test file
    writeFile(fileSys, p1);
    assertTrue(fileSys.exists(p1));

    // Test 1: exit code for chown on existing file is 0
    change(0, "admin", null, f1);

    // Test 2: exit code for chown on non-existing path is 1
    change(1, "admin", null, f2);

    // Test 3: exit code for chown on non-existing path with globbed input is 1
    change(1, "admin", null, f3);

    // create required files
    writeFile(fileSys, p4);
    assertTrue(fileSys.exists(p4));
    writeFile(fileSys, p5);
    assertTrue(fileSys.exists(p5));
    writeFile(fileSys, p6);
    assertTrue(fileSys.exists(p6));

    // Test 4: exit code for chown on existing path with globbed input is 0
    change(0, "admin", null, f7);

   //Test 5: test for setOwner invocation on FS from command handler.
    change(0, "admin", "Test", f1);
    change(0, "admin", "", f1);
  }

  /**
   * Test Chgrp 1. Create and write file on FS 2. Verify that exit code for
   * chgrp on existing file is 0 3. Verify that exit code for chgrp on
   * non-existing file is 1 4. Verify that exit code for chgrp with glob input
   * on non-existing file is 1 5. Verify that exit code for chgrp with glob
   * input on existing file in 0
   * 
   * @throws Exception
   */
  @Test
  public void testChgrp() throws Exception {

    final String f1 = TEST_ROOT_DIR + "/" + "testChgrp/fileExists";
    final String f2 = TEST_ROOT_DIR + "/" + "testChgrp/fileDoesNotExist";
    final String f3 = TEST_ROOT_DIR + "/" + "testChgrp/nonExistingfiles*";

    Path p1 = new Path(f1);

    final Path p4 = new Path(TEST_ROOT_DIR + "/" + "testChgrp/file1");
    final Path p5 = new Path(TEST_ROOT_DIR + "/" + "testChgrp/file2");
    final Path p6 = new Path(TEST_ROOT_DIR + "/" + "testChgrp/file3");

    final String f7 = TEST_ROOT_DIR + "/" + "testChgrp/file*";

    // create and write test file
    writeFile(fileSys, p1);
    assertTrue(fileSys.exists(p1));

    // Test 1: exit code for chgrp on existing file is 0
    change(0, null, "admin", f1);

    // Test 2: exit code for chgrp on non existing path is 1
    change(1, null, "admin", f2);
    change(1, null, "admin", f2, f1); // exit code used to be for last item

    // Test 3: exit code for chgrp on non-existing path with globbed input is 1
    change(1, null, "admin", f3);
    change(1, null, "admin", f3, f1);

    // create required files
    writeFile(fileSys, p4);
    assertTrue(fileSys.exists(p4));
    writeFile(fileSys, p5);
    assertTrue(fileSys.exists(p5));
    writeFile(fileSys, p6);
    assertTrue(fileSys.exists(p6));

    // Test 4: exit code for chgrp on existing path with globbed input is 0
    change(0, null, "admin", f7);
    change(1, null, "admin", f2, f7);
  }
  
  @Test
  public void testGetWithInvalidSourcePathShouldNotDisplayNullInConsole()
      throws Exception {
    Configuration conf = new Configuration();
    FsShell shell = new FsShell();
    shell.setConf(conf);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldErr = System.err;
    System.setErr(out);
    final String results;
    try {
      Path tdir = new Path(TEST_ROOT_DIR, "notNullCopy");
      fileSys.delete(tdir, true);
      fileSys.mkdirs(tdir);
      String[] args = new String[3];
      args[0] = "-get";
      args[1] = tdir+"/invalidSrc";
      args[2] = tdir+"/invalidDst";
      assertTrue("file exists", !fileSys.exists(new Path(args[1])));
      assertTrue("file exists", !fileSys.exists(new Path(args[2])));
      int run = shell.run(args);
      results = bytes.toString();
      assertEquals("Return code should be 1", 1, run);
      assertTrue(" Null is coming when source path is invalid. ",!results.contains("get: null"));
      assertTrue(" Not displaying the intended message ",results.contains("get: `"+args[1]+"': No such file or directory"));
    } finally {
      IOUtils.closeStream(out);
      System.setErr(oldErr);
    }
  }
  
  @Test
  public void testRmWithNonexistentGlob() throws Exception {
    Configuration conf = new Configuration();
    FsShell shell = new FsShell();
    shell.setConf(conf);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream err = new PrintStream(bytes);
    final PrintStream oldErr = System.err;
    System.setErr(err);
    final String results;
    try {
      int exit = shell.run(new String[]{"-rm", "nomatch*"});
      assertEquals(1, exit);
      results = bytes.toString();
      assertTrue(results.contains("rm: `nomatch*': No such file or directory"));
    } finally {
      IOUtils.closeStream(err);
      System.setErr(oldErr);
    }
  }

  @Test
  public void testRmForceWithNonexistentGlob() throws Exception {
    Configuration conf = new Configuration();
    FsShell shell = new FsShell();
    shell.setConf(conf);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream err = new PrintStream(bytes);
    final PrintStream oldErr = System.err;
    System.setErr(err);
    try {
      int exit = shell.run(new String[]{"-rm", "-f", "nomatch*"});
      assertEquals(0, exit);
      assertTrue(bytes.toString().isEmpty());
    } finally {
      IOUtils.closeStream(err);
      System.setErr(oldErr);
    }
  }

  @Test
  public void testInvalidDefaultFS() throws Exception {
    // if default fs doesn't exist or is invalid, but the path provided in 
    // arguments is valid - fsshell should work
    FsShell shell = new FsShell();
    Configuration conf = new Configuration();
    conf.set(FS_DEFAULT_NAME_KEY, "hhhh://doesnotexist/");
    shell.setConf(conf);
    String [] args = new String[2];
    args[0] = "-ls";
    args[1] = "file:///"; // this is valid, so command should run
    int res = shell.run(args);
    System.out.println("res =" + res);
    shell.setConf(conf);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldErr = System.err;
    System.setErr(out);
    final String results;
    try {
      int run = shell.run(args);
      results = bytes.toString();
      LOG.info("result=" + results);
      assertTrue("Return code should be 0", run == 0);
    } finally {
      IOUtils.closeStream(out);
      System.setErr(oldErr);
    }
    
  }
  
  @Test
  public void testInterrupt() throws Exception {
    MyFsShell shell = new MyFsShell();
    shell.setConf(new Configuration());
    final Path d = new Path(TEST_ROOT_DIR, "testInterrupt");
    final Path f1 = new Path(d, "f1");
    final Path f2 = new Path(d, "f2");
    assertTrue(fileSys.mkdirs(d));
    writeFile(fileSys, f1);
    assertTrue(fileSys.isFile(f1));
    writeFile(fileSys, f2);
    assertTrue(fileSys.isFile(f2));

    int exitCode = shell.run(
        new String[]{ "-testInterrupt", f1.toString(), f2.toString() });
    // processing a file throws an interrupt, it should blow on first file
    assertEquals(1, InterruptCommand.processed);
    assertEquals(130, exitCode);
    
    exitCode = shell.run(
        new String[]{ "-testInterrupt", d.toString() });
    // processing a file throws an interrupt, it should blow on file
    // after descent into dir
    assertEquals(2, InterruptCommand.processed);
    assertEquals(130, exitCode);
  }
  
  static class LocalFileSystemExtn extends LocalFileSystem {
    public LocalFileSystemExtn() {
      super(new RawLocalFileSystemExtn());
    }
  }

  static class RawLocalFileSystemExtn extends RawLocalFileSystem {
    protected static HashMap<String,String> owners = new HashMap<String,String>();
    protected static HashMap<String,String> groups = new HashMap<String,String>();

    @Override
    public FSDataOutputStream create(Path p) throws IOException {
      //owners.remove(p);
      //groups.remove(p);
      return super.create(p);
    }

    @Override
    public void setOwner(Path p, String username, String groupname)
        throws IOException {
      String f = makeQualified(p).toString();
      if (username != null)  {
        owners.put(f, username);
      }
      if (groupname != null) {
        groups.put(f, groupname);
      }
    }

    @Override
    public FileStatus getFileStatus(Path p) throws IOException {
      String f = makeQualified(p).toString();
      FileStatus stat = super.getFileStatus(p);
      
      stat.getPermission();
      if (owners.containsKey(f)) {
        stat.setOwner("STUB-"+owners.get(f));      
      } else {
        stat.setOwner("REAL-"+stat.getOwner());
      }
      if (groups.containsKey(f)) {
        stat.setGroup("STUB-"+groups.get(f));      
      } else {
        stat.setGroup("REAL-"+stat.getGroup());
      }
      return stat;
    }
  }
  
  static class MyFsShell extends FsShell {
    protected void registerCommands(CommandFactory factory) {
      factory.addClass(InterruptCommand.class, "-testInterrupt");
    }
  }

  static class InterruptCommand extends FsCommand {
    static int processed = 0;
    InterruptCommand() {
      processed = 0;
      setRecursive(true);
    }
    @Override
    protected void processPath(PathData item) throws IOException {
      System.out.println("processing: "+item);
      processed++;
      if (item.stat.isFile()) {
        System.out.println("throw interrupt");
        throw new InterruptedIOException();
      }
    }
  }  
}
