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

import static org.apache.hadoop.test.PlatformAssumptions.assumeWindows;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFsShellCopy {
  static final Log LOG = LogFactory.getLog(TestFsShellCopy.class);

  static Configuration conf;
  static FsShell shell; 
  static LocalFileSystem lfs;
  static Path testRootDir, srcPath, dstPath;
  
  @BeforeClass
  public static void setup() throws Exception {
    conf = new Configuration();
    shell = new FsShell(conf);
    lfs = FileSystem.getLocal(conf);
    testRootDir = lfs.makeQualified(new Path(GenericTestUtils.getTempPath(
        "testFsShellCopy")));
    
    lfs.mkdirs(testRootDir);
    lfs.setWorkingDirectory(testRootDir);
    srcPath = new Path(testRootDir, "srcFile");
    dstPath = new Path(testRootDir, "dstFile");
  }
  
  @Before
  public void prepFiles() throws Exception {
    lfs.setVerifyChecksum(true);
    lfs.setWriteChecksum(true);
    
    lfs.delete(srcPath, true);
    lfs.delete(dstPath, true);
    FSDataOutputStream out = lfs.create(srcPath);
    out.writeChars("hi");
    out.close();
    assertTrue(lfs.exists(lfs.getChecksumFile(srcPath)));
  }

  private void shellRun(int n, String ... args) throws Exception {
    assertEquals(n, shell.run(args));
  }

  private int shellRun(String... args) throws Exception {
    int exitCode = shell.run(args);
    LOG.info("exit " + exitCode + " - " + StringUtils.join(" ", args));
    return exitCode;
  }

  @Test
  public void testCopyNoCrc() throws Exception {
    shellRun(0, "-get", srcPath.toString(), dstPath.toString());
    checkPath(dstPath, false);
  }

  @Test
  public void testCopyCrc() throws Exception {
    shellRun(0, "-get", "-crc", srcPath.toString(), dstPath.toString());
    checkPath(dstPath, true);
  }

  
  @Test
  public void testCorruptedCopyCrc() throws Exception {
    FSDataOutputStream out = lfs.getRawFileSystem().create(srcPath);
    out.writeChars("bang");
    out.close();
    shellRun(1, "-get", srcPath.toString(), dstPath.toString());
  }

  @Test
  public void testCorruptedCopyIgnoreCrc() throws Exception {
    shellRun(0, "-get", "-ignoreCrc", srcPath.toString(), dstPath.toString());
    checkPath(dstPath, false);
  }

  private void checkPath(Path p, boolean expectChecksum) throws IOException {
    assertTrue(lfs.exists(p));
    boolean hasChecksum = lfs.exists(lfs.getChecksumFile(p));
    assertEquals(expectChecksum, hasChecksum);
  }

  @Test
  public void testCopyFileFromLocal() throws Exception {
    Path testRoot = new Path(testRootDir, "testPutFile");
    lfs.delete(testRoot, true);
    lfs.mkdirs(testRoot);

    Path targetDir = new Path(testRoot, "target");    
    Path filePath = new Path(testRoot, new Path("srcFile"));
    lfs.create(filePath).close();
    checkPut(filePath, targetDir, false);
  }

  @Test
  public void testCopyDirFromLocal() throws Exception {
    Path testRoot = new Path(testRootDir, "testPutDir");
    lfs.delete(testRoot, true);
    lfs.mkdirs(testRoot);
    
    Path targetDir = new Path(testRoot, "target");    
    Path dirPath = new Path(testRoot, new Path("srcDir"));
    lfs.mkdirs(dirPath);
    lfs.create(new Path(dirPath, "srcFile")).close();
    checkPut(dirPath, targetDir, false);
  }

  @Test
  public void testCopyFileFromWindowsLocalPath() throws Exception {
    assumeWindows();
    String windowsTestRootPath = (new File(testRootDir.toUri().getPath()
        .toString())).getAbsolutePath();
    Path testRoot = new Path(windowsTestRootPath, "testPutFile");
    lfs.delete(testRoot, true);
    lfs.mkdirs(testRoot);

    Path targetDir = new Path(testRoot, "target");
    Path filePath = new Path(testRoot, new Path("srcFile"));
    lfs.create(filePath).close();
    checkPut(filePath, targetDir, true);
  }

  @Test
  public void testCopyDirFromWindowsLocalPath() throws Exception {
    assumeWindows();
    String windowsTestRootPath = (new File(testRootDir.toUri().getPath()
        .toString())).getAbsolutePath();
    Path testRoot = new Path(windowsTestRootPath, "testPutDir");
    lfs.delete(testRoot, true);
    lfs.mkdirs(testRoot);

    Path targetDir = new Path(testRoot, "target");
    Path dirPath = new Path(testRoot, new Path("srcDir"));
    lfs.mkdirs(dirPath);
    lfs.create(new Path(dirPath, "srcFile")).close();
    checkPut(dirPath, targetDir, true);
  }

  
  private void checkPut(Path srcPath, Path targetDir, boolean useWindowsPath)
  throws Exception {
    lfs.delete(targetDir, true);
    lfs.mkdirs(targetDir);    
    lfs.setWorkingDirectory(targetDir);

    final Path dstPath = new Path("path");
    final Path childPath = new Path(dstPath, "childPath");
    lfs.setWorkingDirectory(targetDir);
    
    // copy to new file, then again
    prepPut(dstPath, false, false);
    checkPut(0, srcPath, dstPath, useWindowsPath);
    if (lfs.isFile(srcPath)) {
      checkPut(1, srcPath, dstPath, useWindowsPath);
    } else { // directory works because it copies into the dir
      // clear contents so the check won't think there are extra paths
      prepPut(dstPath, true, true);
      checkPut(0, srcPath, dstPath, useWindowsPath);
    }

    // copy to non-existent dir
    prepPut(dstPath, false, false);
    checkPut(1, srcPath, childPath, useWindowsPath);

    // copy into dir, then with another name
    prepPut(dstPath, true, true);
    checkPut(0, srcPath, dstPath, useWindowsPath);
    prepPut(childPath, true, true);
    checkPut(0, srcPath, childPath, useWindowsPath);

    // try to put to pwd with existing dir
    prepPut(targetDir, true, true);
    checkPut(0, srcPath, null, useWindowsPath);
    prepPut(targetDir, true, true);
    checkPut(0, srcPath, new Path("."), useWindowsPath);

    // try to put to pwd with non-existent cwd
    prepPut(dstPath, false, true);
    lfs.setWorkingDirectory(dstPath);
    checkPut(1, srcPath, null, useWindowsPath);
    prepPut(dstPath, false, true);
    checkPut(1, srcPath, new Path("."), useWindowsPath);
  }

  private void prepPut(Path dst, boolean create,
                       boolean isDir) throws IOException {
    lfs.delete(dst, true);
    assertFalse(lfs.exists(dst));
    if (create) {
      if (isDir) {
        lfs.mkdirs(dst);
        assertTrue(lfs.isDirectory(dst));
      } else {
        lfs.mkdirs(new Path(dst.getName()));
        lfs.create(dst).close();
        assertTrue(lfs.isFile(dst));
      }
    }
  }
  
  private void checkPut(int exitCode, Path src, Path dest,
      boolean useWindowsPath) throws Exception {
    String argv[] = null;
    String srcPath = src.toString();
    if (useWindowsPath) {
      srcPath = (new File(srcPath)).getAbsolutePath();
    }
    if (dest != null) {
      argv = new String[]{ "-put", srcPath, pathAsString(dest) };
    } else {
      argv = new String[]{ "-put", srcPath };
      dest = new Path(Path.CUR_DIR);
    }
    
    Path target;
    if (lfs.exists(dest)) {
      if (lfs.isDirectory(dest)) {
        target = new Path(pathAsString(dest), src.getName());
      } else {
        target = dest;
      }
    } else {
      target = new Path(lfs.getWorkingDirectory(), dest);
    }
    boolean targetExists = lfs.exists(target);
    Path parent = lfs.makeQualified(target).getParent();
    
    System.out.println("COPY src["+src.getName()+"] -> ["+dest+"] as ["+target+"]");
    String lsArgv[] = new String[]{ "-ls", "-R", pathAsString(parent) };
    shell.run(lsArgv);
    
    int gotExit = shell.run(argv);
    
    System.out.println("copy exit:"+gotExit);
    lsArgv = new String[]{ "-ls", "-R", pathAsString(parent) };
    shell.run(lsArgv);
    
    if (exitCode == 0) {
      assertTrue(lfs.exists(target));
      assertTrue(lfs.isFile(src) == lfs.isFile(target));
      assertEquals(1, lfs.listStatus(lfs.makeQualified(target).getParent()).length);      
    } else {
      assertEquals(targetExists, lfs.exists(target));
    }
    assertEquals(exitCode, gotExit);
  }
  
  @Test
  public void testRepresentsDir() throws Exception {
    Path subdirDstPath = new Path(dstPath, srcPath.getName());
    String argv[] = null;
    lfs.delete(dstPath, true);
    assertFalse(lfs.exists(dstPath));

    argv = new String[]{ "-put", srcPath.toString(), dstPath.toString() };
    assertEquals(0, shell.run(argv));
    assertTrue(lfs.exists(dstPath) && lfs.isFile(dstPath));

    lfs.delete(dstPath, true);
    assertFalse(lfs.exists(dstPath));

    // since dst path looks like a dir, it should not copy the file and
    // rename it to what looks like a directory
    lfs.delete(dstPath, true); // make copy fail
    for (String suffix : new String[]{ "/", "/." } ) {
      argv = new String[]{
          "-put", srcPath.toString(), dstPath.toString()+suffix };
      assertEquals(1, shell.run(argv));
      assertFalse(lfs.exists(dstPath));
      assertFalse(lfs.exists(subdirDstPath));
    }

    // since dst path looks like a dir, it should not copy the file and
    // rename it to what looks like a directory
    for (String suffix : new String[]{ "/", "/." } ) {
      // empty out the directory and create to make copy succeed
      lfs.delete(dstPath, true);
      lfs.mkdirs(dstPath);
      argv = new String[]{
          "-put", srcPath.toString(), dstPath.toString()+suffix };
      assertEquals(0, shell.run(argv));
      assertTrue(lfs.exists(subdirDstPath));
      assertTrue(lfs.isFile(subdirDstPath));
    }

    // ensure .. is interpreted as a dir
    String dotdotDst = dstPath+"/foo/..";
    lfs.delete(dstPath, true);
    lfs.mkdirs(new Path(dstPath, "foo"));
    argv = new String[]{ "-put", srcPath.toString(), dotdotDst };
    assertEquals(0, shell.run(argv));
    assertTrue(lfs.exists(subdirDstPath));
    assertTrue(lfs.isFile(subdirDstPath));
  }
  
  @Test
  public void testCopyMerge() throws Exception {
    Path root = new Path(testRootDir, "TestMerge");
    Path f1 = new Path(root, "f1");
    Path f2 = new Path(root, "f2");
    Path f3 = new Path(root, "f3");
    Path empty = new Path(root, "empty");
    Path fnf = new Path(root, "fnf");
    Path d = new Path(root, "dir");
    Path df1 = new Path(d, "df1");
    Path df2 = new Path(d, "df2");
    Path df3 = new Path(d, "df3");
    
    createFile(f1, f2, f3, df1, df2, df3);
    createEmptyFile(empty);

    int exit;
    // one file, kind of silly
    exit = shell.run(new String[]{
        "-getmerge",
        f1.toString(),
        "out" });
    assertEquals(0, exit);
    assertEquals("f1", readFile("out"));

    exit = shell.run(new String[]{
        "-getmerge",
        fnf.toString(),
        "out" });
    assertEquals(1, exit);
    assertFalse(lfs.exists(new Path("out")));

    // two files
    exit = shell.run(new String[]{
        "-getmerge",
        f1.toString(), f2.toString(),
        "out" });
    assertEquals(0, exit);
    assertEquals("f1f2", readFile("out"));

    // two files, preserves order
    exit = shell.run(new String[]{
        "-getmerge",
        f2.toString(), f1.toString(),
        "out" });
    assertEquals(0, exit);
    assertEquals("f2f1", readFile("out"));

    // two files
    exit = shell.run(new String[]{
        "-getmerge", "-nl",
        f1.toString(), f2.toString(),
        "out" });
    assertEquals(0, exit);
    assertEquals("f1\nf2\n", readFile("out"));

    exit = shell.run(new String[]{
        "-getmerge", "-nl", "-skip-empty-file",
        f1.toString(), f2.toString(), empty.toString(),
    "out" });
    assertEquals(0, exit);
    assertEquals("f1\nf2\n", readFile("out"));

    // glob three files
    shell.run(new String[]{
        "-getmerge", "-nl",
        new Path(root, "f*").toString(),
        "out" });
    assertEquals(0, exit);
    assertEquals("f1\nf2\nf3\n", readFile("out"));

    // directory with 1 empty + 3 non empty files, should skip subdir
    shell.run(new String[]{
        "-getmerge", "-nl",
        root.toString(),
        "out" });
    assertEquals(0, exit);
    assertEquals("\nf1\nf2\nf3\n", readFile("out"));

    // subdir
    shell.run(new String[]{
        "-getmerge", "-nl",
        d.toString(), "out"});
    assertEquals(0, exit);
    assertEquals("df1\ndf2\ndf3\n", readFile("out"));

    // file, dir, file
    shell.run(new String[]{
        "-getmerge", "-nl",
        f1.toString(), d.toString(), f2.toString(), "out" });
    assertEquals(0, exit);
    assertEquals("f1\ndf1\ndf2\ndf3\nf2\n", readFile("out"));
  }


  @Test
  public void testMoveFileFromLocal() throws Exception {
    Path testRoot = new Path(testRootDir, "testPutFile");
    lfs.delete(testRoot, true);
    lfs.mkdirs(testRoot);

    Path target = new Path(testRoot, "target");    
    Path srcFile = new Path(testRoot, new Path("srcFile"));
    lfs.createNewFile(srcFile);

    int exit = shell.run(new String[]{
        "-moveFromLocal", srcFile.toString(), target.toString() });
    assertEquals(0, exit);
    assertFalse(lfs.exists(srcFile));
    assertTrue(lfs.exists(target));
    assertTrue(lfs.isFile(target));
  }
  
  @Test
  public void testMoveDirFromLocal() throws Exception {    
    Path testRoot = new Path(testRootDir, "testPutDir");
    lfs.delete(testRoot, true);
    lfs.mkdirs(testRoot);
    
    Path srcDir = new Path(testRoot, "srcDir");
    lfs.mkdirs(srcDir);
    Path targetDir = new Path(testRoot, "target");    

    int exit = shell.run(new String[]{
        "-moveFromLocal", srcDir.toString(), targetDir.toString() });
    assertEquals(0, exit);
    assertFalse(lfs.exists(srcDir));
    assertTrue(lfs.exists(targetDir));
  }

  @Test
  public void testMoveDirFromLocalDestExists() throws Exception {    
    Path testRoot = new Path(testRootDir, "testPutDir");
    lfs.delete(testRoot, true);
    lfs.mkdirs(testRoot);
    
    Path srcDir = new Path(testRoot, "srcDir");
    lfs.mkdirs(srcDir);
    Path targetDir = new Path(testRoot, "target");
    lfs.mkdirs(targetDir);

    int exit = shell.run(new String[]{
        "-moveFromLocal", srcDir.toString(), targetDir.toString() });
    assertEquals(0, exit);
    assertFalse(lfs.exists(srcDir));
    assertTrue(lfs.exists(new Path(targetDir, srcDir.getName())));
    
    lfs.mkdirs(srcDir);
    exit = shell.run(new String[]{
        "-moveFromLocal", srcDir.toString(), targetDir.toString() });
    assertEquals(1, exit);
    assertTrue(lfs.exists(srcDir));
  }
  
  @Test
  public void testMoveFromWindowsLocalPath() throws Exception {
    assumeWindows();
    Path testRoot = new Path(testRootDir, "testPutFile");
    lfs.delete(testRoot, true);
    lfs.mkdirs(testRoot);

    Path target = new Path(testRoot, "target");
    Path srcFile = new Path(testRoot, new Path("srcFile"));
    lfs.createNewFile(srcFile);

    String winSrcFile = (new File(srcFile.toUri().getPath()
        .toString())).getAbsolutePath();
    shellRun(0, "-moveFromLocal", winSrcFile, target.toString());
    assertFalse(lfs.exists(srcFile));
    assertTrue(lfs.exists(target));
    assertTrue(lfs.isFile(target));
  }

  @Test
  public void testGetWindowsLocalPath() throws Exception {
    assumeWindows();
    String winDstFile = (new File(dstPath.toUri().getPath()
        .toString())).getAbsolutePath();
    shellRun(0, "-get", srcPath.toString(), winDstFile);
    checkPath(dstPath, false);
  }
  
  @Test
  public void testDirectCopy() throws Exception {
    Path testRoot = new Path(testRootDir, "testPutFile");
    lfs.delete(testRoot, true);
    lfs.mkdirs(testRoot);

    Path target_COPYING_File = new Path(testRoot, "target._COPYING_");
    Path target_File = new Path(testRoot, "target");
    Path srcFile = new Path(testRoot, new Path("srcFile"));
    lfs.createNewFile(srcFile);

    // If direct write is false , then creation of "file1" ,will delete file
    // (file1._COPYING_) if already exist.
    checkDirectCopy(srcFile, target_File, target_COPYING_File, false);
    shell.run(new String[] { "-rm", target_File.toString() });

    // If direct write is true , then creation of "file1", will not create a
    // temporary file and will not delete (file1._COPYING_) if already exist.
    checkDirectCopy(srcFile, target_File, target_COPYING_File, true);
  }

  private void checkDirectCopy(Path srcFile, Path target_File,
      Path target_COPYING_File,boolean direct) throws Exception {
    int directWriteExitCode = direct ? 0 : 1;
    shell
        .run(new String[] { "-copyFromLocal", srcFile.toString(),
        target_COPYING_File.toString() });
    int srcFileexist = shell
        .run(new String[] { "-cat", target_COPYING_File.toString() });
    assertEquals(0, srcFileexist);

    if (!direct) {
      shell.run(new String[] { "-copyFromLocal", srcFile.toString(),
          target_File.toString() });
    } else {
      shell.run(new String[] { "-copyFromLocal", "-d", srcFile.toString(),
          target_File.toString() });
    }
    // cat of "target._COPYING_" will return exitcode :
    // as 1(file does not exist), if direct write is false.
    // as 0, if direct write is true.
    srcFileexist = shell.run(new String[] { "-cat",
        target_COPYING_File.toString() });
    assertEquals(directWriteExitCode, srcFileexist);
  }

  private void createFile(Path ... paths) throws IOException {
    for (Path path : paths) {
      FSDataOutputStream out = lfs.create(path);
      out.write(path.getName().getBytes());
      out.close();
    }
  }

  private void createEmptyFile(Path ... paths) throws IOException {
    for (Path path : paths) {
      FSDataOutputStream out = lfs.create(path);
      out.close();
    }
  }

  private String readFile(String out) throws IOException {
    Path path = new Path(out);
    FileStatus stat = lfs.getFileStatus(path);
    FSDataInputStream in = lfs.open(path);
    byte[] buffer = new byte[(int)stat.getLen()];
    in.readFully(buffer);
    in.close();
    lfs.delete(path, false);
    return new String(buffer);
  }
  
  // path handles "." rather oddly
  private String pathAsString(Path p) {
    String s = (p == null) ? Path.CUR_DIR : p.toString();
    return s.isEmpty() ? Path.CUR_DIR : s;
  }

  /**
   * Test copy to a path with non-existent parent directory.
   */
  @Test
  public void testCopyNoParent() throws Exception {
    final String noDirName = "noDir";
    final Path noDir = new Path(noDirName);
    lfs.delete(noDir, true);
    assertThat(lfs.exists(noDir), is(false));

    assertThat("Expected failed put to a path without parent directory",
        shellRun("-put", srcPath.toString(), noDirName + "/foo"), is(not(0)));

    // Note the trailing '/' in the target path.
    assertThat("Expected failed copyFromLocal to a non-existent directory",
        shellRun("-copyFromLocal", srcPath.toString(), noDirName + "/"),
        is(not(0)));
  }

  @Test
  public void testPutSrcDirNoPerm()
      throws Exception {
    final Path src = new Path(testRootDir, "srcNoPerm");
    final Path dst = new Path(testRootDir, "dst");
    lfs.delete(src, true);
    lfs.mkdirs(src, new FsPermission((short)0));
    lfs.delete(dst, true);

    try {
      final ByteArrayOutputStream err = new ByteArrayOutputStream();
      PrintStream oldErr = System.err;
      System.setErr(new PrintStream(err));
      shellRun(1, "-put", src.toString(), dst.toString());
      System.setErr(oldErr);
      System.err.print(err.toString());
      assertTrue(err.toString().contains(
          FSExceptionMessages.PERMISSION_DENIED));
    } finally {
      // Make sure the test directory can be deleted
      lfs.setPermission(src, new FsPermission((short)0755));
    }
  }

  @Test
  public void testPutSrcFileNoPerm()
      throws Exception {
    final Path src = new Path(testRootDir, "srcNoPerm");
    final Path dst = new Path(testRootDir, "dst");
    lfs.delete(src, true);
    lfs.create(src);
    lfs.setPermission(src, new FsPermission((short)0));
    lfs.delete(dst, true);

    try {
      final ByteArrayOutputStream err = new ByteArrayOutputStream();
      PrintStream oldErr = System.err;
      System.setErr(new PrintStream(err));
      shellRun(1, "-put", src.toString(), dst.toString());
      System.setErr(oldErr);
      System.err.print(err.toString());
      assertTrue(err.toString().contains("(Permission denied)"));
    } finally {
      // make sure the test file can be deleted
      lfs.setPermission(src, new FsPermission((short)0755));
    }
  }
}
