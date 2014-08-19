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

package org.apache.hadoop.fs.azure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/*
 * Tests the Native Azure file system (WASB) against an actual blob store if
 * provided in the environment.
 * Subclasses implement createTestAccount() to hit local&mock storage with the same test code.
 * 
 * For hand-testing: remove "abstract" keyword and copy in an implementation of createTestAccount
 * from one of the subclasses
 */
public abstract class NativeAzureFileSystemBaseTest {

  private FileSystem fs;
  private AzureBlobStorageTestAccount testAccount;
  private final long modifiedTimeErrorMargin = 5 * 1000; // Give it +/-5 seconds

  protected abstract AzureBlobStorageTestAccount createTestAccount()
      throws Exception;

  @Before
  public void setUp() throws Exception {
    testAccount = createTestAccount();
    if (testAccount != null) {
      fs = testAccount.getFileSystem();
    }
    assumeNotNull(testAccount);
  }

  @After
  public void tearDown() throws Exception {
    if (testAccount != null) {
      testAccount.cleanup();
      testAccount = null;
      fs = null;
    }
  }

  @Test
  public void testCheckingNonExistentOneLetterFile() throws Exception {
    assertFalse(fs.exists(new Path("/a")));
  }

  @Test
  public void testStoreRetrieveFile() throws Exception {
    Path testFile = new Path("unit-test-file");
    writeString(testFile, "Testing");
    assertTrue(fs.exists(testFile));
    FileStatus status = fs.getFileStatus(testFile);
    assertNotNull(status);
    // By default, files should be have masked permissions
    // that grant RW to user, and R to group/other
    assertEquals(new FsPermission((short) 0644), status.getPermission());
    assertEquals("Testing", readString(testFile));
    fs.delete(testFile, true);
  }

  @Test
  public void testStoreDeleteFolder() throws Exception {
    Path testFolder = new Path("storeDeleteFolder");
    assertFalse(fs.exists(testFolder));
    assertTrue(fs.mkdirs(testFolder));
    assertTrue(fs.exists(testFolder));
    FileStatus status = fs.getFileStatus(testFolder);
    assertNotNull(status);
    assertTrue(status.isDirectory());
    // By default, directories should be have masked permissions
    // that grant RWX to user, and RX to group/other
    assertEquals(new FsPermission((short) 0755), status.getPermission());
    Path innerFile = new Path(testFolder, "innerFile");
    assertTrue(fs.createNewFile(innerFile));
    assertTrue(fs.exists(innerFile));
    assertTrue(fs.delete(testFolder, true));
    assertFalse(fs.exists(innerFile));
    assertFalse(fs.exists(testFolder));
  }

  @Test
  public void testFileOwnership() throws Exception {
    Path testFile = new Path("ownershipTestFile");
    writeString(testFile, "Testing");
    testOwnership(testFile);
  }

  @Test
  public void testFolderOwnership() throws Exception {
    Path testFolder = new Path("ownershipTestFolder");
    fs.mkdirs(testFolder);
    testOwnership(testFolder);
  }

  private void testOwnership(Path pathUnderTest) throws IOException {
    FileStatus ret = fs.getFileStatus(pathUnderTest);
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    assertEquals(ret.getOwner(), currentUser.getShortUserName());
    fs.delete(pathUnderTest, true);
  }

  private static FsPermission ignoreStickyBit(FsPermission original) {
    return new FsPermission(original.getUserAction(),
        original.getGroupAction(), original.getOtherAction());
  }

  // When FsPermission applies a UMask, it loses sticky bit information.
  // And since we always apply UMask, we should ignore whether the sticky
  // bit is equal or not.
  private static void assertEqualsIgnoreStickyBit(FsPermission expected,
      FsPermission actual) {
    assertEquals(ignoreStickyBit(expected), ignoreStickyBit(actual));
  }

  @Test
  public void testFilePermissions() throws Exception {
    Path testFile = new Path("permissionTestFile");
    FsPermission permission = FsPermission.createImmutable((short) 644);
    createEmptyFile(testFile, permission);
    FileStatus ret = fs.getFileStatus(testFile);
    assertEqualsIgnoreStickyBit(permission, ret.getPermission());
    fs.delete(testFile, true);
  }

  @Test
  public void testFolderPermissions() throws Exception {
    Path testFolder = new Path("permissionTestFolder");
    FsPermission permission = FsPermission.createImmutable((short) 644);
    fs.mkdirs(testFolder, permission);
    FileStatus ret = fs.getFileStatus(testFolder);
    assertEqualsIgnoreStickyBit(permission, ret.getPermission());
    fs.delete(testFolder, true);
  }

  @Test
  public void testDeepFileCreation() throws Exception {
    Path testFile = new Path("deep/file/creation/test");
    FsPermission permission = FsPermission.createImmutable((short) 644);
    createEmptyFile(testFile, permission);
    assertTrue(fs.exists(testFile));
    assertTrue(fs.exists(new Path("deep")));
    assertTrue(fs.exists(new Path("deep/file/creation")));
    FileStatus ret = fs.getFileStatus(new Path("deep/file"));
    assertTrue(ret.isDirectory());
    assertEqualsIgnoreStickyBit(permission, ret.getPermission());
    assertTrue(fs.delete(new Path("deep"), true));
    assertFalse(fs.exists(testFile));

    // An alternative test scenario would've been to delete the file first,
    // and then check for the existence of the upper folders still. But that
    // doesn't actually work as expected right now.
  }

  private static enum RenameVariation {
    NormalFileName, SourceInAFolder, SourceWithSpace, SourceWithPlusAndPercent
  }

  @Test
  public void testRename() throws Exception {
    for (RenameVariation variation : RenameVariation.values()) {
      System.out.printf("Rename variation: %s\n", variation);
      Path originalFile;
      switch (variation) {
      case NormalFileName:
        originalFile = new Path("fileToRename");
        break;
      case SourceInAFolder:
        originalFile = new Path("file/to/rename");
        break;
      case SourceWithSpace:
        originalFile = new Path("file to rename");
        break;
      case SourceWithPlusAndPercent:
        originalFile = new Path("file+to%rename");
        break;
      default:
        throw new Exception("Unknown variation");
      }
      Path destinationFile = new Path("file/resting/destination");
      assertTrue(fs.createNewFile(originalFile));
      assertTrue(fs.exists(originalFile));
      assertFalse(fs.rename(originalFile, destinationFile)); // Parent directory
      // doesn't exist
      assertTrue(fs.mkdirs(destinationFile.getParent()));
      assertTrue(fs.rename(originalFile, destinationFile));
      assertTrue(fs.exists(destinationFile));
      assertFalse(fs.exists(originalFile));
      fs.delete(destinationFile.getParent(), true);
    }
  }

  @Test
  public void testRenameImplicitFolder() throws Exception {
    Path testFile = new Path("deep/file/rename/test");
    FsPermission permission = FsPermission.createImmutable((short) 644);
    createEmptyFile(testFile, permission);
    assertTrue(fs.rename(new Path("deep/file"), new Path("deep/renamed")));
    assertFalse(fs.exists(testFile));
    FileStatus newStatus = fs
        .getFileStatus(new Path("deep/renamed/rename/test"));
    assertNotNull(newStatus);
    assertEqualsIgnoreStickyBit(permission, newStatus.getPermission());
    assertTrue(fs.delete(new Path("deep"), true));
  }

  private static enum RenameFolderVariation {
    CreateFolderAndInnerFile, CreateJustInnerFile, CreateJustFolder
  }

  @Test
  public void testRenameFolder() throws Exception {
    for (RenameFolderVariation variation : RenameFolderVariation.values()) {
      Path originalFolder = new Path("folderToRename");
      if (variation != RenameFolderVariation.CreateJustInnerFile){
        assertTrue(fs.mkdirs(originalFolder));
      }
      Path innerFile = new Path(originalFolder, "innerFile");
      if (variation != RenameFolderVariation.CreateJustFolder){
        assertTrue(fs.createNewFile(innerFile));
      }
      Path destination = new Path("renamedFolder");
      assertTrue(fs.rename(originalFolder, destination));
      assertTrue(fs.exists(destination));
      if (variation != RenameFolderVariation.CreateJustFolder){
        assertTrue(fs.exists(new Path(destination, innerFile.getName())));
      }
      assertFalse(fs.exists(originalFolder));
      assertFalse(fs.exists(innerFile));
      fs.delete(destination, true);
    }
  }

  @Test
  public void testCopyFromLocalFileSystem() throws Exception {
    Path localFilePath = new Path(System.getProperty("test.build.data",
        "azure_test"));
    FileSystem localFs = FileSystem.get(new Configuration());
    localFs.delete(localFilePath, true);
    try {
      writeString(localFs, localFilePath, "Testing");
      Path dstPath = new Path("copiedFromLocal");
      assertTrue(FileUtil.copy(localFs, localFilePath, fs, dstPath, false,
          fs.getConf()));
      assertTrue(fs.exists(dstPath));
      assertEquals("Testing", readString(fs, dstPath));
      fs.delete(dstPath, true);
    } finally {
      localFs.delete(localFilePath, true);
    }
  }

  @Test
  public void testListDirectory() throws Exception {
    Path rootFolder = new Path("testingList");
    assertTrue(fs.mkdirs(rootFolder));
    FileStatus[] listed = fs.listStatus(rootFolder);
    assertEquals(0, listed.length);
    Path innerFolder = new Path(rootFolder, "inner");
    assertTrue(fs.mkdirs(innerFolder));
    listed = fs.listStatus(rootFolder);
    assertEquals(1, listed.length);
    assertTrue(listed[0].isDirectory());
    Path innerFile = new Path(innerFolder, "innerFile");
    writeString(innerFile, "testing");
    listed = fs.listStatus(rootFolder);
    assertEquals(1, listed.length);
    assertTrue(listed[0].isDirectory());
    listed = fs.listStatus(innerFolder);
    assertEquals(1, listed.length);
    assertFalse(listed[0].isDirectory());
    assertTrue(fs.delete(rootFolder, true));
  }

  @Test
  public void testStatistics() throws Exception {
    FileSystem.clearStatistics();
    FileSystem.Statistics stats = FileSystem.getStatistics("wasb",
        NativeAzureFileSystem.class);
    assertEquals(0, stats.getBytesRead());
    assertEquals(0, stats.getBytesWritten());
    Path newFile = new Path("testStats");
    writeString(newFile, "12345678");
    assertEquals(8, stats.getBytesWritten());
    assertEquals(0, stats.getBytesRead());
    String readBack = readString(newFile);
    assertEquals("12345678", readBack);
    assertEquals(8, stats.getBytesRead());
    assertEquals(8, stats.getBytesWritten());
    assertTrue(fs.delete(newFile, true));
    assertEquals(8, stats.getBytesRead());
    assertEquals(8, stats.getBytesWritten());
  }

  @Test
  public void testUriEncoding() throws Exception {
    fs.create(new Path("p/t%5Fe")).close();
    FileStatus[] listing = fs.listStatus(new Path("p"));
    assertEquals(1, listing.length);
    assertEquals("t%5Fe", listing[0].getPath().getName());
    assertTrue(fs.rename(new Path("p"), new Path("q")));
    assertTrue(fs.delete(new Path("q"), true));
  }

  @Test
  public void testUriEncodingMoreComplexCharacters() throws Exception {
    // Create a file name with URI reserved characters, plus the percent
    String fileName = "!#$'()*;=[]%";
    String directoryName = "*;=[]%!#$'()";
    fs.create(new Path(directoryName, fileName)).close();
    FileStatus[] listing = fs.listStatus(new Path(directoryName));
    assertEquals(1, listing.length);
    assertEquals(fileName, listing[0].getPath().getName());
    FileStatus status = fs.getFileStatus(new Path(directoryName, fileName));
    assertEquals(fileName, status.getPath().getName());
    InputStream stream = fs.open(new Path(directoryName, fileName));
    assertNotNull(stream);
    stream.close();
    assertTrue(fs.delete(new Path(directoryName, fileName), true));
    assertTrue(fs.delete(new Path(directoryName), true));
  }

  @Test
  public void testReadingDirectoryAsFile() throws Exception {
    Path dir = new Path("/x");
    assertTrue(fs.mkdirs(dir));
    try {
      fs.open(dir).close();
      assertTrue("Should've thrown", false);
    } catch (FileNotFoundException ex) {
      assertEquals("/x is a directory not a file.", ex.getMessage());
    }
  }

  @Test
  public void testCreatingFileOverDirectory() throws Exception {
    Path dir = new Path("/x");
    assertTrue(fs.mkdirs(dir));
    try {
      fs.create(dir).close();
      assertTrue("Should've thrown", false);
    } catch (IOException ex) {
      assertEquals("Cannot create file /x; already exists as a directory.",
          ex.getMessage());
    }
  }

  @Test
  public void testSetPermissionOnFile() throws Exception {
    Path newFile = new Path("testPermission");
    OutputStream output = fs.create(newFile);
    output.write(13);
    output.close();
    FsPermission newPermission = new FsPermission((short) 0700);
    fs.setPermission(newFile, newPermission);
    FileStatus newStatus = fs.getFileStatus(newFile);
    assertNotNull(newStatus);
    assertEquals(newPermission, newStatus.getPermission());
    assertEquals("supergroup", newStatus.getGroup());
    assertEquals(UserGroupInformation.getCurrentUser().getShortUserName(),
        newStatus.getOwner());
    assertEquals(1, newStatus.getLen());
  }

  @Test
  public void testSetPermissionOnFolder() throws Exception {
    Path newFolder = new Path("testPermission");
    assertTrue(fs.mkdirs(newFolder));
    FsPermission newPermission = new FsPermission((short) 0600);
    fs.setPermission(newFolder, newPermission);
    FileStatus newStatus = fs.getFileStatus(newFolder);
    assertNotNull(newStatus);
    assertEquals(newPermission, newStatus.getPermission());
    assertTrue(newStatus.isDirectory());
  }

  @Test
  public void testSetOwnerOnFile() throws Exception {
    Path newFile = new Path("testOwner");
    OutputStream output = fs.create(newFile);
    output.write(13);
    output.close();
    fs.setOwner(newFile, "newUser", null);
    FileStatus newStatus = fs.getFileStatus(newFile);
    assertNotNull(newStatus);
    assertEquals("newUser", newStatus.getOwner());
    assertEquals("supergroup", newStatus.getGroup());
    assertEquals(1, newStatus.getLen());
    fs.setOwner(newFile, null, "newGroup");
    newStatus = fs.getFileStatus(newFile);
    assertNotNull(newStatus);
    assertEquals("newUser", newStatus.getOwner());
    assertEquals("newGroup", newStatus.getGroup());
  }

  @Test
  public void testSetOwnerOnFolder() throws Exception {
    Path newFolder = new Path("testOwner");
    assertTrue(fs.mkdirs(newFolder));
    fs.setOwner(newFolder, "newUser", null);
    FileStatus newStatus = fs.getFileStatus(newFolder);
    assertNotNull(newStatus);
    assertEquals("newUser", newStatus.getOwner());
    assertTrue(newStatus.isDirectory());
  }

  @Test
  public void testModifiedTimeForFile() throws Exception {
    Path testFile = new Path("testFile");
    fs.create(testFile).close();
    testModifiedTime(testFile);
  }

  @Test
  public void testModifiedTimeForFolder() throws Exception {
    Path testFolder = new Path("testFolder");
    assertTrue(fs.mkdirs(testFolder));
    testModifiedTime(testFolder);
  }

  @Test
  public void testFolderLastModifiedTime() throws Exception {
    Path parentFolder = new Path("testFolder");
    Path innerFile = new Path(parentFolder, "innerfile");
    assertTrue(fs.mkdirs(parentFolder));

    // Create file
    long lastModifiedTime = fs.getFileStatus(parentFolder)
        .getModificationTime();
    // Wait at least the error margin
    Thread.sleep(modifiedTimeErrorMargin + 1);
    assertTrue(fs.createNewFile(innerFile));
    // The parent folder last modified time should have changed because we
    // create an inner file.
    assertFalse(testModifiedTime(parentFolder, lastModifiedTime));
    testModifiedTime(parentFolder);

    // Rename file
    lastModifiedTime = fs.getFileStatus(parentFolder).getModificationTime();
    Path destFolder = new Path("testDestFolder");
    assertTrue(fs.mkdirs(destFolder));
    long destLastModifiedTime = fs.getFileStatus(destFolder)
        .getModificationTime();
    Thread.sleep(modifiedTimeErrorMargin + 1);
    Path destFile = new Path(destFolder, "innerfile");
    assertTrue(fs.rename(innerFile, destFile));
    // Both source and destination folder last modified time should have changed
    // because of renaming.
    assertFalse(testModifiedTime(parentFolder, lastModifiedTime));
    assertFalse(testModifiedTime(destFolder, destLastModifiedTime));
    testModifiedTime(parentFolder);
    testModifiedTime(destFolder);

    // Delete file
    destLastModifiedTime = fs.getFileStatus(destFolder).getModificationTime();
    // Wait at least the error margin
    Thread.sleep(modifiedTimeErrorMargin + 1);
    fs.delete(destFile, false);
    // The parent folder last modified time should have changed because we
    // delete an inner file.
    assertFalse(testModifiedTime(destFolder, destLastModifiedTime));
    testModifiedTime(destFolder);
  }

  @Test
  public void testListSlash() throws Exception {
    Path testFolder = new Path("/testFolder");
    Path testFile = new Path(testFolder, "testFile");
    assertTrue(fs.mkdirs(testFolder));
    assertTrue(fs.createNewFile(testFile));
    FileStatus status = fs.getFileStatus(new Path("/testFolder/."));
    assertNotNull(status);
  }

  @Test
  public void testCloseFileSystemTwice() throws Exception {
    //make sure close() can be called multiple times without doing any harm
    fs.close();
    fs.close();
  }

  private boolean testModifiedTime(Path testPath, long time) throws Exception {
    FileStatus fileStatus = fs.getFileStatus(testPath);
    final long errorMargin = modifiedTimeErrorMargin;
    long lastModified = fileStatus.getModificationTime();
    return (lastModified > (time - errorMargin) && lastModified < (time + errorMargin));
  }

  private void testModifiedTime(Path testPath) throws Exception {
    Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    long currentUtcTime = utc.getTime().getTime();
    FileStatus fileStatus = fs.getFileStatus(testPath);
    assertTrue("Modification time "
        + new Date(fileStatus.getModificationTime()) + " is not close to now: "
        + utc.getTime(), testModifiedTime(testPath, currentUtcTime));
  }

   private void createEmptyFile(Path testFile, FsPermission permission)
      throws IOException {
    FSDataOutputStream outputStream = fs.create(testFile, permission, true,
        4096, (short) 1, 1024, null);
    outputStream.close();
  }

  private String readString(Path testFile) throws IOException {
    return readString(fs, testFile);
  }

  private String readString(FileSystem fs, Path testFile) throws IOException {
    FSDataInputStream inputStream = fs.open(testFile);
    String ret = readString(inputStream);
    inputStream.close();
    return ret;
  }

  private String readString(FSDataInputStream inputStream) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(
        inputStream));
    final int BUFFER_SIZE = 1024;
    char[] buffer = new char[BUFFER_SIZE];
    int count = reader.read(buffer, 0, BUFFER_SIZE);
    if (count >= BUFFER_SIZE) {
      throw new IOException("Exceeded buffer size");
    }
    inputStream.close();
    return new String(buffer, 0, count);
  }

  private void writeString(Path path, String value) throws IOException {
    writeString(fs, path, value);
  }

  private void writeString(FileSystem fs, Path path, String value)
      throws IOException {
    FSDataOutputStream outputStream = fs.create(path, true);
    writeString(outputStream, value);
    outputStream.close();
  }

  private void writeString(FSDataOutputStream outputStream, String value)
      throws IOException {
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
        outputStream));
    writer.write(value);
    writer.close();
  }
}
