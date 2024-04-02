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
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.decodeBytes;
import static org.apache.hadoop.test.GenericTestUtils.getTempPath;

/**
 * Test of hadoop fs shell against S3A.
 */
public class ITestS3AFsShell extends AbstractS3ATestBase {

  // block size for generated test file in byte
  private static final int BLOCK_SIZE = 1024;

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

  private int shellRun(String... args) {
    return fsShell.run(args);
  }

  @Test
  public void testFsShellDirectoryOperations() throws IOException {
    Path testDir = methodPath();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));

    assertEquals("Should create directory success", 0, shellRun("-mkdir", testDir.toString()));
    assertTrue("Directory should exist", fs.getFileStatus(testDir).isDirectory());

    assertEquals("Should recursively create directory success", 0,
        shellRun("-mkdir", "-p", testDir + "/subdir1/subdir2"));
    assertTrue("Directory should exist",
        fs.getFileStatus(new Path(testDir, "subdir1/subdir2")).isDirectory());

    // create a new bucket with hadoop fs will return error file exists
    // because innerGetFileStatus return root directory status without a probe
    String newBucketName =
        "hadoop-fs-shell-test-" + ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    assertNotEquals("Should not be able to create new bucket", 0,
        shellRun("-mkdir", "s3a://" + newBucketName + "/"));
    assertTrue(err.toString().contains("mkdir: `s3a://" + newBucketName + "/': File exists"));
    err.reset();

    assertEquals("Should list directory success", 0, shellRun("-ls", testDir.toString()));
    assertTrue("Should found one item", out.toString().contains("Found 1 items"));
    assertTrue("Should print file list to stdout", out.toString().contains(testDir + "/subdir1"));
    out.reset();

    assertEquals("Should list directory as a plain file success", 0,
        shellRun("-ls", "-d", testDir.toString()));
    assertTrue("Should print directory path to stdout",
        out.toString().contains(testDir.toString()));
    out.reset();

    assertNotEquals("Should fail when list with display erasure coding policy flag", 0,
        shellRun("-ls", "-e", testDir.toString()));
    assertTrue(err.toString()
        .contains("FileSystem " + fs.getUri().toString() + " does not support Erasure Coding"));
    err.reset();

    assertEquals("Should recursively list directory success", 0,
        shellRun("-ls", "-R", testDir.toString()));
    assertTrue("Should print file list to stdout", out.toString().contains(testDir.toString()));
    assertTrue("Should print file list to stdout", out.toString().contains(testDir + "/subdir1"));
    assertTrue("Should print file list to stdout",
        out.toString().contains(testDir + "/subdir1/subdir2"));
    out.reset();

    assertEquals("Should delete directory success", 0,
        shellRun("-rmdir", testDir + "/subdir1/subdir2"));
    assertFalse("Directory should not exist", fs.exists(new Path(testDir, "subdir1/subdir2")));

    assertNotEquals("Should not be able to delete non-empty directory", 0,
        shellRun("-rmdir", testDir.toString()));
    assertTrue(err.toString().contains("Directory is not empty"));
    assertTrue("Directory should exist", fs.exists(testDir));
    err.reset();

    assertEquals("Should recursively delete directory success", 0,
        shellRun("-rm", "-r", testDir.toString()));
    assertFalse("Directory should not exist", fs.exists(testDir));

    assertNotEquals("Should not be able to delete root directory", 0,
        shellRun("-rm", "-r", "-f", fs.getUri().toString() + "/"));
    assertTrue(err.toString().contains("S3A: Cannot delete the root directory"));
    err.reset();
  }

  @Test
  public void testFsShellFileOperations() throws IOException {
    Path testDir = methodPath();

    fs.mkdirs(testDir);
    fs.mkdirs(new Path(testDir, "subdir"));
    ContractTestUtils.touch(fs, new Path(testDir, "emptyFile"));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));

    long lastModifiedTime;
    long modifiedTime;

    // test working with remote files
    assertEquals("Should touch file success", 0, shellRun("-touch", testDir + "/touchFile"));
    assertTrue("File should exist", fs.getFileStatus(new Path(testDir, "touchFile")).isFile());

    lastModifiedTime = fs.getFileStatus(new Path(testDir, "touchFile")).getModificationTime();
    assertEquals("Should touch with specific timestamp success", 0,
        shellRun("-touch", "-t", "20180809:230000", testDir + "/touchFile"));
    modifiedTime = fs.getFileStatus(new Path(testDir, "touchFile")).getModificationTime();
    assertEquals("Object modification time can't be updated", lastModifiedTime, modifiedTime);

    assertEquals("Should create zero length file success", 0,
        shellRun("-touchz", testDir + "/touchzFile"));
    assertTrue("File should exist", fs.getFileStatus(new Path(testDir, "touchzFile")).isFile());

    assertEquals("Should copy a file success", 0,
        shellRun("-cp", testDir + "/touchFile", testDir + "/copiedFile"));
    assertTrue("File should exist", fs.getFileStatus(new Path(testDir, "copiedFile")).isFile());

    assertEquals("Should copy a file with preserve flag success", 0,
        shellRun("-cp", "-p", testDir + "/touchFile", testDir + "/copiedFile2"));
    assertTrue("File should exist", fs.getFileStatus(new Path(testDir, "copiedFile2")).isFile());

    lastModifiedTime = fs.getFileStatus(new Path(testDir, "copiedFile")).getModificationTime();
    assertEquals("Should copy and overwrite a file success", 0,
        shellRun("-cp", "-f", testDir + "/touchFile", testDir + "/copiedFile"));
    modifiedTime = fs.getFileStatus(new Path(testDir, "copiedFile")).getModificationTime();
    assertTrue("File should exist", fs.getFileStatus(new Path(testDir, "copiedFile")).isFile());
    assertNotEquals("Modification time should be updated from overwrite", lastModifiedTime,
        modifiedTime);

    assertNotEquals("Should fail when copy multiple files but destination is not a directory", 0,
        shellRun("-cp", testDir + "/touchFile", testDir + "/touchzFile", testDir + "/emptyFile"));
    assertTrue(err.toString().contains("/emptyFile': Is not a directory"));
    err.reset();

    assertEquals("Should copy multiple files success", 0,
        shellRun("-cp", testDir + "/touchFile", testDir + "/touchzFile", testDir + "/subdir"));
    assertTrue("File should exist",
        fs.getFileStatus(new Path(testDir, "subdir/touchFile")).isFile());
    assertTrue("File should exist",
        fs.getFileStatus(new Path(testDir, "subdir/touchzFile")).isFile());

    assertEquals("Should move a file success", 0,
        shellRun("-mv", testDir + "/copiedFile", testDir + "/movedFile"));
    assertFalse("Source file should not exist after moved",
        fs.exists(new Path(testDir, "copiedFile")));
    assertTrue("File should exist", fs.getFileStatus(new Path(testDir, "movedFile")).isFile());

    assertEquals("Should delete a file success", 0, shellRun("-rm", testDir + "/movedFile"));
    assertTrue("Should display deleted file",
        out.toString().contains("Deleted " + testDir + "/movedFile"));
    assertFalse("File should not exist", fs.exists(new Path(testDir, "movedFile")));
    out.reset();

    assertEquals("Should delete multiple files success", 0,
        shellRun("-rm", testDir + "/subdir/touchFile", testDir + "/subdir/touchzFile"));
    assertTrue("Should display deleted file",
        out.toString().contains("Deleted " + testDir + "/subdir/touchFile"));
    assertTrue("Should display deleted file",
        out.toString().contains("Deleted " + testDir + "/subdir/touchzFile"));
    assertFalse("File should not exist", fs.exists(new Path(testDir, "subdir/touchFile")));
    assertFalse("File should not exist", fs.exists(new Path(testDir, "subdir/touchzFile")));
    out.reset();

    assertNotEquals("Should fail when move multiple files but destination is not a directory", 0,
        shellRun("-mv", testDir + "/touchFile", testDir + "/touchzFile", testDir + "/emptyFile"));
    assertTrue(err.toString().contains("/emptyFile': Is not a directory"));
    err.reset();

    assertEquals("Should move multiple files success", 0,
        shellRun("-mv", testDir + "/touchFile", testDir + "/touchzFile", testDir + "/subdir"));
    assertFalse("Source file should not exist after moved",
        fs.exists(new Path(testDir, "touchFile")));
    assertFalse("Source file should not exist after moved",
        fs.exists(new Path(testDir, "touchzFile")));
    assertTrue("File should exist",
        fs.getFileStatus(new Path(testDir, "subdir/touchFile")).isFile());
    assertTrue("File should exist",
        fs.getFileStatus(new Path(testDir, "subdir/touchzFile")).isFile());

    assertEquals("Should delete files from trash success", 0,
        shellRun("-expunge", "-immediate", "-fs", fs.getUri().toString() + "/"));

    assertEquals("Should list against a file success", 0,
        shellRun("-ls", testDir + "/subdir/touchFile"));
    assertTrue("Should print file path to stdout",
        out.toString().contains(testDir + "/subdir/touchFile"));
    out.reset();

    assertEquals("Should print matched files and directories by name", 0,
        shellRun("-find", testDir.toString(), "-name", "*touch*", "-print"));
    assertTrue("Should display matched file",
        out.toString().contains(testDir + "/subdir/touchFile"));
    assertTrue("Should display matched file",
        out.toString().contains(testDir + "/subdir/touchzFile"));
    out.reset();

    assertEquals("Should display file checksum", 0,
        shellRun("-checksum", testDir + "/subdir/touchFile"));
    assertTrue("-checksum is not implemented, expected NONE", out.toString().contains("NONE"));
    out.reset();
  }

  @Test
  public void testFsShellStreamConcatOperations() throws IOException {
    Path testDir = methodPath();

    fs.mkdirs(testDir);
    ContractTestUtils.touch(fs, new Path(testDir, "emptyFile"));
    int fileLen = BLOCK_SIZE * 2;
    byte[] data = ContractTestUtils.dataset(fileLen, 0, 255);
    ContractTestUtils.createFile(fs, new Path(testDir, "testFile"), true, data);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));

    assertEquals("Should copy source file to stdout", 0, shellRun("-cat", testDir + "/testFile"));
    assertArrayEquals("-cat output doesn't match the original data", out.toByteArray(), data);
    out.reset();

    assertEquals("Should display first kilobyte of file", 0,
        shellRun("-head", testDir + "/testFile"));
    assertEquals("-head returned " + out.size() + " bytes data, expected 1KB", 1024, out.size());
    assertArrayEquals("Tail output doesn't match the original data",
        Arrays.copyOfRange(data, 0, 1024), out.toByteArray());
    out.reset();
    out.reset();

    assertEquals("Should display last kilobyte of file", 0,
        shellRun("-tail", testDir + "/testFile"));
    assertEquals("-tail returned " + out.size() + " bytes data, expected 1KB", 1024, out.size());
    assertArrayEquals("Tail output doesn't match the original data",
        Arrays.copyOfRange(data, data.length - 1024, data.length), out.toByteArray());
    out.reset();

    assertEquals("Should display file in text format", 0, shellRun("-text", testDir + "/testFile"));
    assertArrayEquals("-text output doesn't match the original data", out.toByteArray(), data);
    out.reset();

    assertNotEquals("Should fail on concat", 0,
        shellRun("-concat", testDir + "/emptyFile", testDir + "/testFile", testDir + "/testFile"));
    assertTrue(err.toString().contains("Not implemented by the S3AFileSystem"));
    err.reset();

    assertNotEquals("Should fail on truncate", 0,
        shellRun("-truncate", "-w", "1", testDir + "/testFile"));
    assertTrue(err.toString().contains("Not implemented by the S3AFileSystem"));
    err.reset();

    assertNotEquals("Should fail on appendToFile", 0,
        shellRun("-appendToFile", "-", testDir + "/testFile"));
    assertTrue(err.toString().contains("Append is not supported by S3AFileSystem"));
    err.reset();
  }

  @Test
  public void testFsShellFileTransferOperations() throws IOException {
    Path testDir = methodPath();

    fs.mkdirs(testDir);
    int fileLen = BLOCK_SIZE;
    byte[] data = ContractTestUtils.dataset(fileLen, 0, 255);
    ContractTestUtils.createFile(fs, new Path(testDir, "testFile"), true, data);

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
          shellRun("-put", localSrcFile.toString(), testDir + "/putFile"));
      assertTrue("File should exist", fs.getFileStatus(new Path(testDir, "putFile")).isFile());

      assertEquals("Should put and overwrite file to S3 success", 0,
          shellRun("-put", "-f", localSrcFile.toString(), testDir + "/putFile"));
      assertTrue("File should exist", fs.getFileStatus(new Path(testDir, "putFile")).isFile());

      assertEquals("Should put file to S3 success with preserve flag", 0,
          shellRun("-put", "-p", localSrcFile.toString(), testDir + "/putFile-p"));
      assertTrue("File should exist", fs.getFileStatus(new Path(testDir, "putFile-p")).isFile());

      assertEquals("Should put file to S3 success with specific thread count", 0,
          shellRun("-put", "-t", "2", localSrcFile.toString(), testDir + "/putFile-t"));
      assertTrue("File should exist", fs.getFileStatus(new Path(testDir, "putFile-t")).isFile());

      assertEquals("Should put file to S3 success with lazily persist option", 0,
          shellRun("-put", "-l", localSrcFile.toString(), testDir + "/putFile-l"));
      assertTrue("File should exist", fs.getFileStatus(new Path(testDir, "putFile-l")).isFile());

      assertEquals("Should put file to S3 success with skip temporary file creation option", 0,
          shellRun("-put", "-d", localSrcFile.toString(), testDir + "/putFile-d"));
      assertTrue("File should exist", fs.getFileStatus(new Path(testDir, "putFile-d")).isFile());

      assertEquals("Should move file to S3 success", 0,
          shellRun("-moveFromLocal", localSrcFile.toString(), testDir + "/movedLocalFile"));
      assertTrue("File should exist",
          fs.getFileStatus(new Path(testDir, "movedLocalFile")).isFile());

      assertEquals("Should copy file from S3 to local", 0,
          shellRun("-get", testDir + "/testFile", localDstPath + "/testGet"));
      assertTrue("File should exist locally",
          lfs.getFileStatus(new Path(localDstPath, "testGet")).isFile());

      assertEquals("Should copy file from S3 and overwrite to local", 0,
          shellRun("-get", "-f", testDir + "/testFile", localDstPath + "/testGet"));
      assertTrue("File should exist locally",
          lfs.getFileStatus(new Path(localDstPath, "testGet")).isFile());

      assertEquals("Should copy file from S3 to local with ignore crc option", 0,
          shellRun("-get", "-ignoreCrc", testDir + "/testFile",
              localDstPath + "/testGet-ignoreCrc"));
      assertTrue("File should exist locally",
          lfs.getFileStatus(new Path(localDstPath, "testGet-ignoreCrc")).isFile());

      Path getWithCrcFile = new Path(localDstPath, "testGet-crc");
      assertEquals("Should copy file from S3 to local with crc option", 0,
          shellRun("-get", "-crc", testDir + "/testFile", getWithCrcFile.toString()));
      assertTrue("File should exist locally", lfs.getFileStatus(getWithCrcFile).isFile());
      assertTrue("Should has checksum file", lfs.exists(lfs.getChecksumFile(getWithCrcFile)));

      Path mergedFile = new Path(localDstPath, "testGetMerge");
      assertEquals("Should merge files under the path and save to local", 0,
          shellRun("-getmerge", "-nl", testDir.toString(), mergedFile.toString()));
      assertTrue("Merged file size should more than an original file",
          lfs.getFileStatus(mergedFile).getLen() > BLOCK_SIZE);

      assertNotEquals("Should error not implemented", 0,
          shellRun("-moveToLocal", testDir + "/testFile", localDstPath + "/testMoveToLocal"));
      assertTrue(err.toString().contains("Option '-moveToLocal' is not implemented yet."));
      err.reset();
    } finally {
      lfs.delete(localTestDir, true);
    }
  }

  @Test
  public void testFsShellStatOperations() throws IOException {
    Path testDir = methodPath();
    Path emptyFile = new Path(testDir, "emptyFile");
    Path nonEmptyFile = new Path(testDir, "nonEmptyFile");

    fs.mkdirs(testDir);
    ContractTestUtils.touch(fs, emptyFile);
    int fileLen = BLOCK_SIZE;
    byte[] data = ContractTestUtils.dataset(fileLen, 0, 255);
    ContractTestUtils.createFile(fs, nonEmptyFile, true, data);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    System.setOut(new PrintStream(out));

    assertEquals("Should display free space", 0, shellRun("-df", testDir.toString()));
    FsStatus fsStatus = fs.getStatus();
    String dfOutput = out.toString();
    assertTrue("-df output should contain capacity",
        dfOutput.contains(Long.toString(fsStatus.getCapacity())));
    assertTrue("-df output should contain used space",
        dfOutput.contains(Long.toString(fsStatus.getUsed())));
    assertTrue("-df output should contain remaining space",
        dfOutput.contains(Long.toString(fsStatus.getRemaining())));
    out.reset();

    assertEquals("Should display sizes of files and directories", 0,
        shellRun("-du", testDir.toString()));
    String duOutput = out.toString();
    FileStatus fileStatus1 = fs.getFileStatus(emptyFile);
    FileStatus fileStatus2 = fs.getFileStatus(nonEmptyFile);
    assertTrue(
        "-du output should contain " + emptyFile.getName() + " with size " + fileStatus1.getLen(),
        duOutput.contains(Long.toString(fileStatus1.getLen())));
    assertTrue("-du output should contain " + nonEmptyFile.getName() + " with size "
        + fileStatus2.getLen(), duOutput.contains(Long.toString(fileStatus2.getLen())));
    out.reset();

    assertEquals("Should count directories, files and bytes under the path", 0,
        shellRun("-count", testDir.toString()));
    String[] countResult = out.toString().trim().split("\\s+");
    assertEquals("Expected output to has four columns", 4, countResult.length);
    assertEquals("Directory count is " + countResult[0] + ", expected 1", "1", countResult[0]);
    assertEquals("File count is " + countResult[1] + ", expected 2", "2", countResult[1]);
    assertEquals("Byte count is " + countResult[2] + ", expected 1024", "1024", countResult[2]);
    out.reset();

    assertEquals("Should print statistics", 0,
        shellRun("-stat", "\"type:%F perm:%a %u:%g size:%b mtime:%y atime:%x name:%n\"",
            nonEmptyFile.toString()));
    SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
    FileStatus fileStatus = fs.getFileStatus(nonEmptyFile);
    String modifiedTime = fmt.format(new Date(fileStatus.getModificationTime()));
    String accessTime = fmt.format(new Date(fileStatus.getAccessTime()));
    assertEquals("-stat output doesn't match pattern",
        "\"type:regular file perm:666 " + fileStatus.getOwner() + ":" + fileStatus.getGroup()
            + " size:" + fileStatus.getLen() + " mtime:" + modifiedTime + " atime:" + accessTime
            + " name:" + nonEmptyFile.getName() + "\"\n", out.toString());
    out.reset();

    assertEquals("Should return zero for a directory", 0,
        shellRun("-test", "-d", testDir.toString()));

    assertNotEquals("Should return non-zero for a file", 0,
        shellRun("-test", "-d", testDir + "/emptyFile"));

    assertEquals("Should return zero when the path exists", 0,
        shellRun("-test", "-e", testDir.toString()));

    assertNotEquals("Should return non-zero when the path doesn't exist", 0,
        shellRun("-test", "-e", testDir + "/notExistPath"));

    assertEquals("Should return zero for a file", 0,
        shellRun("-test", "-f", testDir + "/emptyFile"));

    assertNotEquals("Should return non-zero for a directory", 0,
        shellRun("-test", "-f", testDir.toString()));

    assertEquals("Should return zero for a non-empty path", 0,
        shellRun("-test", "-s", testDir + "/nonEmptyFile"));

    assertNotEquals("Should return non-zero for an empty path", 0,
        shellRun("-test", "-s", testDir.toString()));

    assertEquals("Should return zero when path exists and write permission is granted", 0,
        shellRun("-test", "-w", testDir.toString()));

    assertEquals("Should return zero when path exists and read permission is granted", 0,
        shellRun("-test", "-r", testDir.toString()));

    assertEquals("Should return zero for a zero length file", 0,
        shellRun("-test", "-z", testDir + "/emptyFile"));
  }

  @Test
  public void testFsShellPermissionOperations() throws IOException {
    Path testDir = methodPath();

    fs.mkdirs(testDir);
    Path emptyFile = new Path(testDir, "emptyFile");
    ContractTestUtils.touch(fs, emptyFile);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));

    // permission
    // Even if some commands are runnable, but it has no effect on S3 object
    assertEquals("Should run chgrp success", 0,
        shellRun("-chgrp", "hadoopaws", testDir + "/emptyFile"));
    assertNotEquals("File group does not change", "hadoopaws",
        fs.getFileStatus(emptyFile).getGroup());

    assertEquals("Should run chmod success", 0, shellRun("-chmod", "400", testDir + "/emptyFile"));
    assertEquals("File permission always be rw-rw-rw-", "rw-rw-rw-",
        fs.getFileStatus(emptyFile).getPermission().toString());

    assertEquals("Should run chown success", 0,
        shellRun("-chown", "hadoopaws", testDir + "/emptyFile"));
    assertNotEquals("File owner does not change", "hadoopaws",
        fs.getFileStatus(emptyFile).getOwner());

    assertEquals("Should run getfacl success", 0, shellRun("-getfacl", testDir + "/emptyFile"));
    S3AFileStatus fileStatus = (S3AFileStatus) fs.getFileStatus(new Path(testDir, "emptyFile"));
    String getfaclOutput = out.toString();
    assertTrue("getfacl output doesn't contain owner name=" + fileStatus.getOwner(),
        getfaclOutput.contains("owner: " + fileStatus.getOwner()));
    assertTrue("getfacl output doesn't contain group name=" + fileStatus.getGroup(),
        getfaclOutput.contains("group: " + fileStatus.getGroup()));
    assertTrue("getfacl output doesn't contain user permission",
        getfaclOutput.contains("user::rw-"));
    assertTrue("getfacl output doesn't contain group permission",
        getfaclOutput.contains("group::rw-"));
    assertTrue("getfacl output doesn't contain other permission",
        getfaclOutput.contains("other::rw-"));
    out.reset();

    assertEquals("Should run getfattr success", 0,
        shellRun("-getfattr", "-d", testDir + "/emptyFile"));
    Map<String, byte[]> xAttrs = fs.getXAttrs(new Path(testDir, "emptyFile"));
    String getfattrOutput = out.toString();
    for (String key : xAttrs.keySet()) {
      String expectedString = key + "=\"" + decodeBytes(xAttrs.get(key)) + "\"";
      assertTrue("Expected attribute " + key + " from getfattr output",
          getfattrOutput.contains(expectedString));
    }
    out.reset();

    assertNotEquals("Should fail on setfacl", 0,
        shellRun("-setfacl", "-m", "user:hadoop:rw-", testDir + "/emptyFile"));
    assertTrue(err.toString().contains("S3AFileSystem doesn't support modifyAclEntries"));
    err.reset();

    assertNotEquals("Should fail on setfattr", 0,
        shellRun("-setfattr", "-n", "user.myAttr", "-v", "myValue", testDir + "/emptyFile"));
    assertTrue(err.toString().contains("S3AFileSystem doesn't support setXAttr"));
    err.reset();
  }

  @Test
  public void testFsShellSnapshotOperations() throws IOException {
    Path testDir = methodPath();

    fs.mkdirs(testDir);

    ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setErr(new PrintStream(err));

    assertNotEquals("Should fail on createSnapshot", 0,
        shellRun("-createSnapshot", testDir.toString(), "snapshot1"));
    assertTrue(err.toString().contains("S3AFileSystem doesn't support createSnapshot"));
    err.reset();

    assertNotEquals("Should fail on deleteSnapshot", 0,
        shellRun("-deleteSnapshot", testDir.toString(), "snapshot1"));
    assertTrue(err.toString().contains("S3AFileSystem doesn't support deleteSnapshot"));
    err.reset();

    assertNotEquals("Should fail on renameSnapshot", 0,
        shellRun("-renameSnapshot", testDir.toString(), "snapshot1", "snapshot2"));
    assertTrue(err.toString().contains("S3AFileSystem doesn't support renameSnapshot"));
    err.reset();
  }

  @Test
  public void testFsShellReplicationOperations() throws IOException {
    Path testDir = methodPath();

    fs.mkdirs(testDir);

    // Set replication factor command will success but has no effect on the object
    assertEquals("Should return zero when set replication factor", 0,
        shellRun("-setrep", "2", testDir.toString()));
    assertEquals("S3AFileSystem replication factor is always 1", 1,
        fs.getFileStatus(testDir).getReplication());

    assertEquals("Should return zero when set replication factor and wait to finish", 0,
        shellRun("-setrep", "-w", "2", testDir.toString()));
    assertEquals("S3AFileSystem replication factor is always 1", 1,
        fs.getFileStatus(testDir).getReplication());
  }
}
