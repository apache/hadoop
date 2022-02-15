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

package org.apache.hadoop.fs.s3a;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import static org.apache.hadoop.test.GenericTestUtils.getTempPath;

/**
 * Test of hadoop fs shell against S3A
 */
public class ITestS3AFsShell extends AbstractS3ATestBase {

  // size of generated test file in byte
  private static final int TEST_FILE_SIZE = 1024;

  private FileSystem fs;
  private LocalFileSystem lfs;
  private FsShell fsShell;

  @Override
  public void setup() throws Exception {
    super.setup();
    Configuration conf = getConfiguration();
    fs = getFileSystem();
    lfs = FileSystem.getLocal(conf);
    fsShell = new FsShell(conf);
  }

  @Test
  public void testFsShellDirectoryOperations() throws IOException {
    Path path = methodPath();
    String testDir = path.toString();

    ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setErr(new PrintStream(err));

    assertEquals("Should create directory success", 0,
        fsShell.run(new String[] {"-mkdir", testDir}));
    assertTrue("Directory should exist", fs.getFileStatus(new Path(testDir)).isDirectory());

    assertEquals("Should recursively create directory success", 0,
        fsShell.run(new String[] {"-mkdir", "-p", testDir + "/subdir1/subdir2"}));
    assertTrue("Directory should exist",
        fs.getFileStatus(new Path(testDir + "/subdir1/subdir2")).isDirectory());

    // create a new bucket with hadoop fs will return error file exists
    // because innerGetFileStatus return root directory status without a probe
    String newBucketName =
        "hadoop-fs-shell-test-" + ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    assertNotEquals("Should not be able to create new bucket", 0,
        fsShell.run(new String[] {"-mkdir", "s3a://" + newBucketName + "/"}));
    assertTrue(err.toString().contains("mkdir: `s3a://" + newBucketName + "/': File exists"));
    err.reset();

    assertEquals("Should list directory success", 0, fsShell.run(new String[] {"-ls", testDir}));

    assertEquals("Should list directory as a plain file success", 0,
        fsShell.run(new String[] {"-ls", "-d", testDir}));

    assertNotEquals("Should fail when list with display erasure coding policy flag", 0,
        fsShell.run(new String[] {"-ls", "-e", testDir}));
    assertTrue(err.toString()
        .contains("FileSystem " + fs.getUri().toString() + " does not support Erasure Coding"));
    err.reset();

    assertEquals("Should recursively list directory success", 0,
        fsShell.run(new String[] {"-ls", "-R", testDir}));

    assertEquals("Should delete directory success", 0,
        fsShell.run(new String[] {"-rmdir", testDir + "/subdir1/subdir2"}));
    assertFalse("Directory should not exist", fs.exists(new Path(testDir + "/subdir1/subdir2")));

    assertNotEquals("Should not be able to delete non-empty directory", 0,
        fsShell.run(new String[] {"-rmdir", testDir}));
    assertTrue(err.toString().contains("Directory is not empty"));
    assertTrue("Directory should exist", fs.exists(new Path(testDir)));
    err.reset();

    assertEquals("Should recursively delete directory success", 0,
        fsShell.run(new String[] {"-rm", "-r", testDir}));
    assertFalse("Directory should not exist", fs.exists(new Path(testDir)));

    assertNotEquals("Should not be able to delete root directory", 0,
        fsShell.run(new String[] {"-rm", "-r", "-f", fs.getUri().toString() + "/"}));
    assertTrue(err.toString().contains("Input/output error"));
    err.reset();
  }

  @Test
  public void testFsShellFileOperations() throws IOException {
    Path path = methodPath();
    String testDir = path.toString();

    fs.mkdirs(path);
    fs.mkdirs(new Path(path, "subdir"));
    ContractTestUtils.touch(fs, new Path(path, "emptyFile"));
    byte[] block = ContractTestUtils.dataset(TEST_FILE_SIZE, 0, 255);
    ContractTestUtils.createFile(fs, new Path(path, "testFile"), true, block);

    ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setErr(new PrintStream(err));

    long lastModifiedTime;
    long modifiedTime;

    // test working with remote files
    assertEquals("Should touch file success", 0,
        fsShell.run(new String[] {"-touch", testDir + "/touchFile"}));
    assertTrue("File should exist", fs.getFileStatus(new Path(testDir + "/touchFile")).isFile());

    lastModifiedTime = fs.getFileStatus(new Path(testDir, "touchFile")).getModificationTime();
    assertEquals("Should touch with specific timestamp success", 0,
        fsShell.run(new String[] {"-touch", "-t", "20180809:230000", testDir + "/touchFile"}));
    modifiedTime = fs.getFileStatus(new Path(testDir, "touchFile")).getModificationTime();
    assertEquals("Object modification time can't be updated", lastModifiedTime, modifiedTime);

    assertEquals("Should create zero length file success", 0,
        fsShell.run(new String[] {"-touchz", testDir + "/touchzFile"}));
    assertTrue("File should exist", fs.getFileStatus(new Path(testDir + "/touchzFile")).isFile());

    assertEquals("Should copy a file success", 0,
        fsShell.run(new String[] {"-cp", testDir + "/touchFile", testDir + "/copiedFile"}));
    assertTrue("File should exist", fs.getFileStatus(new Path(testDir + "/copiedFile")).isFile());

    assertEquals("Should copy a file with preserve flag success", 0,
        fsShell.run(new String[] {"-cp", "-p", testDir + "/touchFile", testDir + "/copiedFile2"}));
    assertTrue("File should exist", fs.getFileStatus(new Path(testDir + "/copiedFile2")).isFile());

    lastModifiedTime = fs.getFileStatus(new Path(testDir, "copiedFile")).getModificationTime();
    assertEquals("Should copy and overwrite a file success", 0,
        fsShell.run(new String[] {"-cp", "-f", testDir + "/touchFile", testDir + "/copiedFile"}));
    modifiedTime = fs.getFileStatus(new Path(testDir, "copiedFile")).getModificationTime();
    assertTrue("File should exist", fs.getFileStatus(new Path(testDir + "/copiedFile")).isFile());
    assertNotEquals("Modification time should be updated from overwrite", lastModifiedTime,
        modifiedTime);

    assertNotEquals("Should fail when copy multiple files but destination is not a directory", 0,
        fsShell.run(new String[] {"-cp", testDir + "/touchFile", testDir + "/touchzFile",
            testDir + "/emptyFile"}));
    assertTrue(err.toString().contains("/emptyFile': Is not a directory"));
    err.reset();

    assertEquals("Should copy multiple files success", 0, fsShell.run(
        new String[] {"-cp", testDir + "/touchFile", testDir + "/touchzFile",
            testDir + "/subdir"}));
    assertTrue("File should exist",
        fs.getFileStatus(new Path(testDir + "/subdir/touchFile")).isFile());
    assertTrue("File should exist",
        fs.getFileStatus(new Path(testDir + "/subdir/touchzFile")).isFile());

    assertEquals("Should move a file success", 0,
        fsShell.run(new String[] {"-mv", testDir + "/copiedFile", testDir + "/movedFile"}));
    assertFalse("Source file should not exist after moved",
        fs.exists(new Path(testDir + "/copiedFile")));
    assertTrue("File should exist", fs.getFileStatus(new Path(testDir + "/movedFile")).isFile());

    assertEquals("Should delete a file success", 0,
        fsShell.run(new String[] {"-rm", testDir + "/movedFile"}));
    assertFalse("File should not exist", fs.exists(new Path(testDir, "/movedFile")));

    assertEquals("Should delete multiple files success", 0, fsShell.run(
        new String[] {"-rm", testDir + "/subdir/touchFile", testDir + "/subdir/touchzFile"}));
    assertFalse("File should not exist", fs.exists(new Path(testDir + "/subdir/touchFile")));
    assertFalse("File should not exist", fs.exists(new Path(testDir + "/subdir/touchzFile")));

    assertNotEquals("Should fail when move multiple files but destination is not a directory", 0,
        fsShell.run(new String[] {"-mv", testDir + "/touchFile", testDir + "/touchzFile",
            testDir + "/emptyFile"}));
    assertTrue(err.toString().contains("/emptyFile': Is not a directory"));
    err.reset();

    assertEquals("Should move multiple files success", 0, fsShell.run(
        new String[] {"-mv", testDir + "/touchFile", testDir + "/touchzFile",
            testDir + "/subdir"}));
    assertFalse("Source file should not exist after moved",
        fs.exists(new Path(testDir + "/touchFile")));
    assertFalse("Source file should not exist after moved",
        fs.exists(new Path(testDir + "/touchzFile")));
    assertTrue("File should exist",
        fs.getFileStatus(new Path(testDir + "/subdir/touchFile")).isFile());
    assertTrue("File should exist",
        fs.getFileStatus(new Path(testDir + "/subdir/touchzFile")).isFile());

    assertEquals("Should delete files from trash success", 0,
        fsShell.run(new String[] {"-expunge", "-immediate", "-fs", fs.getUri().toString() + "/"}));

    assertEquals("Should list against a file success", 0,
        fsShell.run(new String[] {"-ls", testDir + "/testFile"}));

    assertEquals("Should copy source file to stdout", 0,
        fsShell.run(new String[] {"-cat", testDir + "/testFile"}));

    assertEquals("Should display first kilobyte of file", 0,
        fsShell.run(new String[] {"-head", testDir + "/testFile"}));

    assertEquals("Should display last kilobyte of file", 0,
        fsShell.run(new String[] {"-tail", testDir + "/testFile"}));

    assertEquals("Should display file in text format", 0,
        fsShell.run(new String[] {"-text", testDir + "/testFile"}));

    assertEquals("Should display file checksum", 0,
        fsShell.run(new String[] {"-checksum", testDir + "/testFile"}));

    assertEquals("Should print matched files and directories by name", 0,
        fsShell.run(new String[] {"-find", testDir, "-name", "*File*", "-print"}));

    assertNotEquals("Should fail on concat", 0, fsShell.run(
        new String[] {"-concat", testDir + "/emptyFile", testDir + "/testFile",
            testDir + "/testFile"}));
    assertTrue(err.toString().contains("Not implemented by the S3AFileSystem"));
    err.reset();

    assertNotEquals("Should fail on truncate", 0,
        fsShell.run(new String[] {"-truncate", "-w", "1", testDir + "/testFile"}));
    assertTrue(err.toString().contains("Not implemented by the S3AFileSystem"));
    err.reset();

    assertNotEquals("Should fail on appendToFile", 0,
        fsShell.run(new String[] {"-appendToFile", "-", testDir + "/testFile"}));
    assertTrue(err.toString().contains("Append is not supported by S3AFileSystem"));
    err.reset();
  }

  @Test
  public void testFsShellFileTransferOperations() throws IOException {
    Path path = methodPath();
    String testDir = path.toString();

    fs.mkdirs(path);
    byte[] block = ContractTestUtils.dataset(TEST_FILE_SIZE, 0, 255);
    ContractTestUtils.createFile(fs, new Path(path, "testFile"), true, block);

    ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setErr(new PrintStream(err));

    Path localTestDir = lfs.makeQualified(new Path(getTempPath(getMethodName())));
    Path localSrcPath = new Path(localTestDir, "srcFile");
    Path localDstPath = new Path(localTestDir, "dstFile");
    Path localSrcFile = new Path(localSrcPath, "localFile");
    lfs.mkdirs(localTestDir);
    lfs.mkdirs(localSrcPath);
    lfs.mkdirs(localDstPath);
    lfs.setWorkingDirectory(localTestDir);
    lfs.createNewFile(localSrcFile);

    try {
      assertEquals("Should put file to S3 success", 0,
          fsShell.run(new String[] {"-put", localSrcFile.toString(), testDir + "/putFile"}));
      assertTrue("File should exist", fs.getFileStatus(new Path(testDir + "/putFile")).isFile());

      assertEquals("Should put and overwrite file to S3 success", 0,
          fsShell.run(new String[] {"-put", "-f", localSrcFile.toString(), testDir + "/putFile"}));
      assertTrue("File should exist", fs.getFileStatus(new Path(testDir + "/putFile")).isFile());

      assertEquals("Should put file to S3 success with preserve flag", 0, fsShell.run(
          new String[] {"-put", "-p", localSrcFile.toString(), testDir + "/putFile-p"}));
      assertTrue("File should exist", fs.getFileStatus(new Path(testDir + "/putFile-p")).isFile());

      assertEquals("Should put file to S3 success with specific thread count", 0, fsShell.run(
          new String[] {"-put", "-t", "2", localSrcFile.toString(), testDir + "/putFile-t"}));
      assertTrue("File should exist", fs.getFileStatus(new Path(testDir + "/putFile-t")).isFile());

      assertEquals("Should put file to S3 success with lazily persist option", 0, fsShell.run(
          new String[] {"-put", "-l", localSrcFile.toString(), testDir + "/putFile-l"}));
      assertTrue("File should exist", fs.getFileStatus(new Path(testDir + "/putFile-l")).isFile());

      assertEquals("Should put file to S3 success with skip temporary file creation option", 0,
          fsShell.run(
              new String[] {"-put", "-d", localSrcFile.toString(), testDir + "/putFile-d"}));
      assertTrue("File should exist", fs.getFileStatus(new Path(testDir + "/putFile-d")).isFile());

      assertEquals("Should move file to S3 success", 0, fsShell.run(
          new String[] {"-moveFromLocal", localSrcFile.toString(), testDir + "/movedLocalFile"}));
      assertTrue("File should exist",
          fs.getFileStatus(new Path(testDir + "/movedLocalFile")).isFile());

      assertEquals("Should copy file from S3 to local", 0,
          fsShell.run(new String[] {"-get", testDir + "/testFile", localDstPath + "/testGet"}));
      assertTrue("File should exist locally",
          lfs.getFileStatus(new Path(localDstPath, "testGet")).isFile());

      assertEquals("Should copy file from S3 and overwrite to local", 0, fsShell.run(
          new String[] {"-get", "-f", testDir + "/testFile", localDstPath + "/testGet"}));
      assertTrue("File should exist locally",
          lfs.getFileStatus(new Path(localDstPath, "testGet")).isFile());

      assertEquals("Should copy file from S3 to local with ignore crc option", 0, fsShell.run(
          new String[] {"-get", "-ignoreCrc", testDir + "/testFile",
              localDstPath + "/testGet-ignoreCrc"}));
      assertTrue("File should exist locally",
          lfs.getFileStatus(new Path(localDstPath, "testGet-ignoreCrc")).isFile());

      Path getFileCrc = new Path(localDstPath, "testGet-crc");
      assertEquals("Should copy file from S3 to local with crc option", 0,
          fsShell.run(new String[] {"-get", "-crc", testDir + "/testFile", getFileCrc.toString()}));
      assertTrue("File should exist locally", lfs.getFileStatus(getFileCrc).isFile());
      assertTrue("Should has checksum file", lfs.exists(lfs.getChecksumFile(getFileCrc)));

      Path mergedFile = new Path(localDstPath, "testGetMerge");
      assertEquals("Should merge files under the path and save to local", 0,
          fsShell.run(new String[] {"-getmerge", "-nl", testDir, mergedFile.toString()}));
      assertTrue("Merged file size should more than an original file",
          lfs.getFileStatus(mergedFile).getLen() > TEST_FILE_SIZE);

      assertNotEquals("Should error not implemented", 0, fsShell.run(
          new String[] {"-moveToLocal", testDir + "/testFile", localDstPath + "/testMoveToLocal"}));
      assertTrue(err.toString().contains("Option '-moveToLocal' is not implemented yet."));
      err.reset();
    } finally {
      lfs.delete(localTestDir, true);
    }
  }

  @Test
  public void testFsShellStatOperations() throws IOException {
    Path path = methodPath();
    String testDir = path.toString();

    fs.mkdirs(path);
    ContractTestUtils.touch(fs, new Path(path, "emptyFile"));
    byte[] block = ContractTestUtils.dataset(TEST_FILE_SIZE, 0, 255);
    ContractTestUtils.createFile(fs, new Path(path, "nonEmptyFile"), true, block);

    assertEquals("Should display free space", 0, fsShell.run(new String[] {"-df", testDir}));

    assertEquals("Should display sizes of files and directories", 0,
        fsShell.run(new String[] {"-du", testDir}));

    assertEquals("Should count directories, files and bytes under the path", 0,
        fsShell.run(new String[] {"-count", testDir}));

    assertEquals("Should print statistics", 0, fsShell.run(
        new String[] {"-stat", "\"type:%F perm:%a %u:%g size:%b mtime:%y atime:%x name:%n\"",
            testDir}));

    assertEquals("Should return zero for a directory", 0,
        fsShell.run(new String[] {"-test", "-d", testDir}));

    assertNotEquals("Should return non-zero for a file", 0,
        fsShell.run(new String[] {"-test", "-d", testDir + "/emptyFile"}));

    assertEquals("Should return zero when the path exists", 0,
        fsShell.run(new String[] {"-test", "-e", testDir}));

    assertNotEquals("Should return non-zero when the path doesn't exist", 0,
        fsShell.run(new String[] {"-test", "-e", testDir + "/notExistPath"}));

    assertEquals("Should return zero for a file", 0,
        fsShell.run(new String[] {"-test", "-f", testDir + "/emptyFile"}));

    assertNotEquals("Should return non-zero for a directory", 0,
        fsShell.run(new String[] {"-test", "-f", testDir}));

    assertEquals("Should return zero for a non-empty path", 0,
        fsShell.run(new String[] {"-test", "-s", testDir + "/nonEmptyFile"}));

    assertNotEquals("Should return non-zero for an empty path", 0,
        fsShell.run(new String[] {"-test", "-s", testDir}));

    assertEquals("Should return zero when path exists and write permission is granted", 0,
        fsShell.run(new String[] {"-test", "-w", testDir}));

    assertEquals("Should return zero when path exists and read permission is granted", 0,
        fsShell.run(new String[] {"-test", "-r", testDir}));

    assertEquals("Should return zero for a zero length file", 0,
        fsShell.run(new String[] {"-test", "-z", testDir + "/emptyFile"}));
  }

  @Test
  public void testFsShellPermissionOperations() throws IOException {
    Path path = methodPath();
    String testDir = path.toString();

    fs.mkdirs(path);
    Path emptyFile = new Path(path, "emptyFile");
    ContractTestUtils.touch(fs, emptyFile);

    ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setErr(new PrintStream(err));

    // permission
    // Even if some commands are runnable, but it has no effect on S3 object
    assertEquals("Should run chgrp success", 0,
        fsShell.run(new String[] {"-chgrp", "hadoopaws", testDir + "/emptyFile"}));
    assertNotEquals("File group does not change", "hadoopaws",
        fs.getFileStatus(emptyFile).getGroup());

    assertEquals("Should run chmod success", 0,
        fsShell.run(new String[] {"-chmod", "400", testDir + "/emptyFile"}));
    assertEquals("File permission always be rw-rw-rw-", "rw-rw-rw-",
        fs.getFileStatus(emptyFile).getPermission().toString());

    assertEquals("Should run chown success", 0,
        fsShell.run(new String[] {"-chown", "hadoopaws", testDir + "/emptyFile"}));
    assertNotEquals("File owner does not change", "hadoopaws",
        fs.getFileStatus(emptyFile).getOwner());

    assertEquals("Should run getfacl success", 0,
        fsShell.run(new String[] {"-getfacl", testDir + "/emptyFile"}));

    assertEquals("Should run getfattr success", 0,
        fsShell.run(new String[] {"-getfattr", "-d", testDir + "/emptyFile"}));

    assertNotEquals("Should fail on setfacl", 0,
        fsShell.run(new String[] {"-setfacl", "-m", "user:hadoop:rw-", testDir + "/emptyFile"}));
    assertTrue(err.toString().contains("S3AFileSystem doesn't support modifyAclEntries"));
    err.reset();

    assertNotEquals("Should fail on setfattr", 0, fsShell.run(
        new String[] {"-setfattr", "-n", "user.myAttr", "-v", "myValue", testDir + "/emptyFile"}));
    assertTrue(err.toString().contains("S3AFileSystem doesn't support setXAttr"));
    err.reset();
  }

  @Test
  public void testFsShellSnapshotOperations() throws IOException {
    Path path = methodPath();
    String testDir = path.toString();

    fs.mkdirs(path);

    ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setErr(new PrintStream(err));

    assertNotEquals("Should fail on createSnapshot", 0,
        fsShell.run(new String[] {"-createSnapshot", testDir, "snapshot1"}));
    assertTrue(err.toString().contains("S3AFileSystem doesn't support createSnapshot"));
    err.reset();

    assertNotEquals("Should fail on deleteSnapshot", 0,
        fsShell.run(new String[] {"-deleteSnapshot", testDir, "snapshot1"}));
    assertTrue(err.toString().contains("S3AFileSystem doesn't support deleteSnapshot"));
    err.reset();

    assertNotEquals("Should fail on renameSnapshot", 0,
        fsShell.run(new String[] {"-renameSnapshot", testDir, "snapshot1", "snapshot2"}));
    assertTrue(err.toString().contains("S3AFileSystem doesn't support renameSnapshot"));
    err.reset();
  }

  @Test
  public void testFsShellReplicationOperations() throws IOException {
    Path path = methodPath();
    String testDir = path.toString();

    fs.mkdirs(path);

    // Set replication factor command will success but has no effect on the object
    assertEquals("Should return zero when set replication factor", 0,
        fsShell.run(new String[] {"-setrep", "2", testDir}));
    assertEquals("Replication factor is always 1", 1, fs.getFileStatus(path).getReplication());

    assertEquals("Should return zero when set replication factor and wait to finish", 0,
        fsShell.run(new String[] {"-setrep", "-w", "2", testDir}));
    assertEquals("Replication factor is always 1", 1, fs.getFileStatus(path).getReplication());
  }
}
