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
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import static org.apache.hadoop.test.GenericTestUtils.getTestDir;

/**
 * Test of hadoop fs shell against S3A
 */
public class ITestS3AFsShell extends AbstractS3ATestBase {

  @Test
  public void testFsShellDirectoryOperations() throws IOException {
    Configuration conf = getConfiguration();
    FileSystem fs = getFileSystem();
    FsShell fsShell = new FsShell(conf);
    Path path = methodPath();
    String testDir = path.toString();

    ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setErr(new PrintStream(err));

    assertEquals("Should create directory success", 0,
        fsShell.run(new String[] {"-mkdir", testDir}));

    assertEquals("Should recursively create directory success", 0,
        fsShell.run(new String[] {"-mkdir", "-p", testDir + "/subdir1/subdir2"}));

    // create a new bucket with hadoop fs will return error file exists
    String newBucketName =
        "hadoop-fs-shell-test-" + ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    assertNotEquals("Should not be able to create new bucket", 0,
        fsShell.run(new String[] {"-mkdir", "s3a://" + newBucketName + "/"}));
    assertTrue(err.toString().contains("mkdir: `s3a://" + newBucketName + "/': File exists"));
    err.reset();

    assertEquals("Should list directory success", 0, fsShell.run(new String[] {"-ls", testDir}));

    assertEquals("Should recursively list directory success", 0,
        fsShell.run(new String[] {"-ls", "-R", testDir}));

    assertEquals("Should delete directory success", 0,
        fsShell.run(new String[] {"-rmdir", testDir + "/subdir1/subdir2"}));

    assertNotEquals("Should not be able to delete non-empty directory", 0,
        fsShell.run(new String[] {"-rmdir", testDir}));
    assertTrue(err.toString().contains("Directory is not empty"));
    err.reset();

    assertEquals("Should recursively delete directory success", 0,
        fsShell.run(new String[] {"-rm", "-r", testDir}));

    assertNotEquals("Should not be able to delete root directory", 0,
        fsShell.run(new String[] {"-rm", "-r", "-f", fs.getUri().toString() + "/"}));
    assertTrue(err.toString().contains("S3A: Cannot delete the root directory"));
    err.reset();
  }

  @Test
  public void testFsShellFileOperations() throws IOException {
    Configuration conf = getConfiguration();
    FileSystem fs = getFileSystem();
    FsShell fsShell = new FsShell(conf);
    Path path = methodPath();
    String testDir = path.toString();

    fs.mkdirs(path);
    fs.mkdirs(new Path(path, "subdir"));
    ContractTestUtils.touch(fs, new Path(path, "emptyFile"));
    byte[] block = ContractTestUtils.dataset(1024, 0, 255);
    ContractTestUtils.createFile(fs, new Path(path, "testFile"), true, block);

    ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setErr(new PrintStream(err));

    // test working with remote files
    assertEquals("Should touch file success", 0,
        fsShell.run(new String[] {"-touch", testDir + "/touchFile"}));

    assertEquals("Should create zero length file success", 0,
        fsShell.run(new String[] {"-touchz", testDir + "/touchzFile"}));

    assertEquals("Should copy a file success", 0,
        fsShell.run(new String[] {"-cp", testDir + "/touchFile", testDir + "/copiedFile"}));

    assertEquals("Should copy and overwrite a file success", 0,
        fsShell.run(new String[] {"-cp", "-f", testDir + "/touchFile", testDir + "/copiedFile"}));

    assertNotEquals("Should fail when copy multiple files but destination is not a directory", 0,
        fsShell.run(new String[] {"-cp", testDir + "/touchFile", testDir + "/touchzFile",
            testDir + "/emptyFile"}));
    assertTrue(err.toString().contains("/emptyFile': Is not a directory"));
    err.reset();

    assertEquals("Should copy multiple files success", 0, fsShell.run(
        new String[] {"-cp", testDir + "/touchFile", testDir + "/touchzFile",
            testDir + "/subdir"}));

    assertEquals("Should move a file success", 0,
        fsShell.run(new String[] {"-mv", testDir + "/copiedFile", testDir + "/movedFile"}));

    assertEquals("Should delete a file success", 0,
        fsShell.run(new String[] {"-rm", testDir + "/movedFile"}));

    assertEquals("Should delete multiple files success", 0, fsShell.run(
        new String[] {"-rm", testDir + "/subdir/touchFile", testDir + "/subdir/touchzFile"}));

    assertNotEquals("Should fail when move multiple files but destination is not a directory", 0,
        fsShell.run(new String[] {"-mv", testDir + "/touchFile", testDir + "/touchzFile",
            testDir + "/emptyFile"}));
    assertTrue(err.toString().contains("/emptyFile': Is not a directory"));
    err.reset();

    assertEquals("Should move multiple files success", 0, fsShell.run(
        new String[] {"-mv", testDir + "/touchFile", testDir + "/touchzFile",
            testDir + "/subdir"}));

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

    // test working with local files
    File localTestDir = getTestDir("tmp-" + getMethodName());
    localTestDir.mkdirs();
    File localFile = File.createTempFile("localFile", ".txt", localTestDir);
    FileUtils.writeStringToFile(localFile, "File content\n", StandardCharsets.UTF_8);
    File testGetFile = new File(localTestDir, "testGet");
    File testMergeFile = new File(localTestDir, "testGetMerge");
    File testMoveFile = new File(localTestDir, "testMoveToLocal");

    try {
      assertEquals("Should put file to S3 success", 0, fsShell.run(
          new String[] {"-put", localFile.getAbsolutePath(), testDir + "/copiedLocalFile"}));

      assertEquals("Should move file to S3 success", 0, fsShell.run(
          new String[] {"-moveFromLocal", localFile.getAbsolutePath(),
              testDir + "/movedLocalFile"}));

      assertEquals("Should copy file from S3 to local", 0,
          fsShell.run(new String[] {"-get", testDir + "/testFile", testGetFile.getAbsolutePath()}));

      assertEquals("Should merge files under the path and save to local", 0,
          fsShell.run(new String[] {"-getmerge", "-nl", testDir, testMergeFile.getAbsolutePath()}));

      assertNotEquals("Should error not implemented", 0, fsShell.run(
          new String[] {"-moveToLocal", testDir + "/testFile", testMoveFile.getAbsolutePath()}));
      assertTrue(err.toString().contains("Option '-moveToLocal' is not implemented yet."));
      err.reset();
    } finally {
      FileUtils.deleteDirectory(localTestDir);
    }
  }

  @Test
  public void testFsShellStatOperations() throws IOException {
    Configuration conf = getConfiguration();
    S3AFileSystem fs = getFileSystem();
    FsShell fsShell = new FsShell(conf);
    Path path = methodPath();
    String testDir = path.toString();

    fs.mkdirs(path);
    ContractTestUtils.touch(fs, new Path(path, "emptyFile"));
    byte[] block = ContractTestUtils.dataset(1024, 0, 255);
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
    Configuration conf = getConfiguration();
    S3AFileSystem fs = getFileSystem();
    FsShell fsShell = new FsShell(conf);
    Path path = methodPath();
    String testDir = path.toString();

    fs.mkdirs(path);
    ContractTestUtils.touch(fs, new Path(path, "emptyFile"));

    ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setErr(new PrintStream(err));

    // permission
    // Even if some commands are runnable, but it has no effect on S3 object
    assertEquals("Should run chgrp success", 0,
        fsShell.run(new String[] {"-chgrp", "hadoop", testDir + "/emptyFile"}));

    assertEquals("Should run chmod success", 0,
        fsShell.run(new String[] {"-chmod", "400", testDir + "/emptyFile"}));

    assertEquals("Should run chown success", 0,
        fsShell.run(new String[] {"-chown", "hadoop", testDir + "/emptyFile"}));

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
    Configuration conf = getConfiguration();
    S3AFileSystem fs = getFileSystem();
    FsShell fsShell = new FsShell(conf);
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
    Configuration conf = getConfiguration();
    S3AFileSystem fs = getFileSystem();
    FsShell fsShell = new FsShell(conf);
    Path path = methodPath();
    String testDir = path.toString();

    fs.mkdirs(path);

    // Set replication factor command will success but has no effect on the object
    assertEquals("Should return zero when set replication factor", 0,
        fsShell.run(new String[] {"-setrep", "2", testDir}));

    assertEquals("Should return zero when set replication factor and wait to finish", 0,
        fsShell.run(new String[] {"-setrep", "-w", "2", testDir}));
  }
}
