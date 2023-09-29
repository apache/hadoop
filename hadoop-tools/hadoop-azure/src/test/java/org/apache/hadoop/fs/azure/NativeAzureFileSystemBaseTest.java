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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem.FolderRenamePending;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.readStringFromFile;
import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.writeStringToFile;
import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.writeStringToStream;
import static org.apache.hadoop.test.GenericTestUtils.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/*
 * Tests the Native Azure file system (WASB) against an actual blob store if
 * provided in the environment.
 * Subclasses implement createTestAccount() to hit local&mock storage with the same test code.
 * 
 * For hand-testing: remove "abstract" keyword and copy in an implementation of createTestAccount
 * from one of the subclasses
 */
public abstract class NativeAzureFileSystemBaseTest
    extends AbstractWasbTestBase {

  private final long modifiedTimeErrorMargin = 5 * 1000; // Give it +/-5 seconds

  private static final short READ_WRITE_PERMISSIONS = 644;
  private static final EnumSet<XAttrSetFlag> CREATE_FLAG = EnumSet.of(XAttrSetFlag.CREATE);
  private static final EnumSet<XAttrSetFlag> REPLACE_FLAG = EnumSet.of(XAttrSetFlag.REPLACE);

  public static final Logger LOG = LoggerFactory.getLogger(NativeAzureFileSystemBaseTest.class);
  protected NativeAzureFileSystem fs;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    fs = getFileSystem();
  }

  /**
   * Assert that a path does not exist.
   *
   * @param message message to include in the assertion failure message
   * @param path path in the filesystem
   * @throws IOException IO problems
   */
  public void assertPathDoesNotExist(String message,
      Path path) throws IOException {
    ContractTestUtils.assertPathDoesNotExist(fs, message, path);
  }

  /**
   * Assert that a path exists.
   *
   * @param message message to include in the assertion failure message
   * @param path path in the filesystem
   * @throws IOException IO problems
   */
  public void assertPathExists(String message,
      Path path) throws IOException {
    ContractTestUtils.assertPathExists(fs, message, path);
  }

  @Test
  public void testCheckingNonExistentOneLetterFile() throws Exception {
    assertPathDoesNotExist("one letter file", new Path("/a"));
  }

  @Test
  public void testStoreRetrieveFile() throws Exception {
    Path testFile = methodPath();
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
  public void testSetGetXAttr() throws Exception {
    byte[] attributeValue1 = "hi".getBytes(StandardCharsets.UTF_8);
    byte[] attributeValue2 = "你好".getBytes(StandardCharsets.UTF_8);
    String attributeName1 = "user.asciiAttribute";
    String attributeName2 = "user.unicodeAttribute";
    Path testFile = methodPath();

    // after creating a file, the xAttr should not be present
    createEmptyFile(testFile, FsPermission.createImmutable(READ_WRITE_PERMISSIONS));
    assertNull(fs.getXAttr(testFile, attributeName1));

    // after setting the xAttr on the file, the value should be retrievable
    fs.setXAttr(testFile, attributeName1, attributeValue1);
    assertArrayEquals(attributeValue1, fs.getXAttr(testFile, attributeName1));

    // after setting a second xAttr on the file, the first xAttr values should not be overwritten
    fs.setXAttr(testFile, attributeName2, attributeValue2);
    assertArrayEquals(attributeValue1, fs.getXAttr(testFile, attributeName1));
    assertArrayEquals(attributeValue2, fs.getXAttr(testFile, attributeName2));
  }

  @Test
  public void testSetGetXAttrCreateReplace() throws Exception {
    byte[] attributeValue = "one".getBytes(StandardCharsets.UTF_8);
    String attributeName = "user.someAttribute";
    Path testFile = methodPath();

    // after creating a file, it must be possible to create a new xAttr
    createEmptyFile(testFile, FsPermission.createImmutable(READ_WRITE_PERMISSIONS));
    fs.setXAttr(testFile, attributeName, attributeValue, CREATE_FLAG);
    assertArrayEquals(attributeValue, fs.getXAttr(testFile, attributeName));

    // however after the xAttr is created, creating it again must fail
    intercept(IOException.class, () -> fs.setXAttr(testFile, attributeName, attributeValue, CREATE_FLAG));
  }

  @Test
  public void testSetGetXAttrReplace() throws Exception {
    byte[] attributeValue1 = "one".getBytes(StandardCharsets.UTF_8);
    byte[] attributeValue2 = "two".getBytes(StandardCharsets.UTF_8);
    String attributeName = "user.someAttribute";
    Path testFile = methodPath();

    // after creating a file, it must not be possible to replace an xAttr
    createEmptyFile(testFile, FsPermission.createImmutable(READ_WRITE_PERMISSIONS));
    intercept(IOException.class, () -> fs.setXAttr(testFile, attributeName, attributeValue1, REPLACE_FLAG));

    // however after the xAttr is created, replacing it must succeed
    fs.setXAttr(testFile, attributeName, attributeValue1, CREATE_FLAG);
    fs.setXAttr(testFile, attributeName, attributeValue2, REPLACE_FLAG);
    assertArrayEquals(attributeValue2, fs.getXAttr(testFile, attributeName));
  }

  @Test
  public void testStoreDeleteFolder() throws Exception {
    Path testFolder = methodPath();
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
    assertPathExists("inner file", innerFile);
    assertTrue(fs.delete(testFolder, true));
    assertPathDoesNotExist("inner file", innerFile);
    assertPathDoesNotExist("testFolder", testFolder);
  }

  @Test
  public void testFileOwnership() throws Exception {
    Path testFile = methodPath();
    writeString(testFile, "Testing");
    testOwnership(testFile);
  }

  @Test
  public void testFolderOwnership() throws Exception {
    Path testFolder = methodPath();
    fs.mkdirs(testFolder);
    testOwnership(testFolder);
  }

  private void testOwnership(Path pathUnderTest) throws IOException {
    FileStatus ret = fs.getFileStatus(pathUnderTest);
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    assertTrue(ret.getOwner().equals(currentUser.getShortUserName()));
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
    Path testFile = methodPath();
    FsPermission permission = FsPermission.createImmutable((short) 644);
    createEmptyFile(testFile, permission);
    FileStatus ret = fs.getFileStatus(testFile);
    assertEqualsIgnoreStickyBit(permission, ret.getPermission());
    fs.delete(testFile, true);
  }

  @Test
  public void testFolderPermissions() throws Exception {
    Path testFolder = methodPath();
    FsPermission permission = FsPermission.createImmutable((short) 644);
    fs.mkdirs(testFolder, permission);
    FileStatus ret = fs.getFileStatus(testFolder);
    assertEqualsIgnoreStickyBit(permission, ret.getPermission());
    fs.delete(testFolder, true);
  }

  void testDeepFileCreationBase(String testFilePath, String firstDirPath, String middleDirPath,
          short permissionShort, short umaskedPermissionShort) throws Exception  {
    Path testFile = new Path(testFilePath);
    Path firstDir = new Path(firstDirPath);
    Path middleDir = new Path(middleDirPath);
    FsPermission permission = FsPermission.createImmutable(permissionShort);
    FsPermission umaskedPermission = FsPermission.createImmutable(umaskedPermissionShort);

    createEmptyFile(testFile, permission);
    FsPermission rootPerm = fs.getFileStatus(firstDir.getParent()).getPermission();
    FsPermission inheritPerm = FsPermission.createImmutable((short)(rootPerm.toShort() | 0300));
    assertPathExists("test file", testFile);
    assertPathExists("firstDir", firstDir);
    assertPathExists("middleDir", middleDir);
    // verify that the indirectly created directory inherited its permissions from the root directory
    FileStatus directoryStatus = fs.getFileStatus(middleDir);
    assertTrue(directoryStatus.isDirectory());
    assertEqualsIgnoreStickyBit(inheritPerm, directoryStatus.getPermission());
    // verify that the file itself has the permissions as specified
    FileStatus fileStatus = fs.getFileStatus(testFile);
    assertFalse(fileStatus.isDirectory());
    assertEqualsIgnoreStickyBit(umaskedPermission, fileStatus.getPermission());
    assertTrue(fs.delete(firstDir, true));
    assertPathDoesNotExist("deleted file", testFile);

    // An alternative test scenario would've been to delete the file first,
    // and then check for the existence of the upper folders still. But that
    // doesn't actually work as expected right now.
  }

  @Test
  public void testDeepFileCreation() throws Exception {
    // normal permissions in user home
    testDeepFileCreationBase("deep/file/creation/test", "deep", "deep/file/creation", (short)0644, (short)0644);
    // extra permissions in user home. umask will change the actual permissions.
    testDeepFileCreationBase("deep/file/creation/test", "deep", "deep/file/creation", (short)0777, (short)0755);
    // normal permissions in root
    testDeepFileCreationBase("/deep/file/creation/test", "/deep", "/deep/file/creation", (short)0644, (short)0644);
    // less permissions in root
    testDeepFileCreationBase("/deep/file/creation/test", "/deep", "/deep/file/creation", (short)0700, (short)0700);
    // one indirectly created directory in root
    testDeepFileCreationBase("/deep/file", "/deep", "/deep", (short)0644, (short)0644);
    // one indirectly created directory in user home
    testDeepFileCreationBase("deep/file", "deep", "deep", (short)0644, (short)0644);
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
      boolean result = fs.rename(originalFile, destinationFile);
      assertTrue(result);
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
    boolean renameResult = fs.rename(new Path("deep/file"), new Path("deep/renamed"));
    assertTrue(renameResult);
    assertFalse(fs.exists(testFile));
    FileStatus newStatus = fs.getFileStatus(new Path("deep/renamed/rename/test"));
    assertNotNull(newStatus);
    assertEqualsIgnoreStickyBit(permission, newStatus.getPermission());
    assertTrue(fs.delete(new Path("deep"), true));
  }

  private enum RenameFolderVariation {
    CreateFolderAndInnerFile, CreateJustInnerFile, CreateJustFolder
  }

  @Test
  public void testRenameFolder() throws Exception {
    for (RenameFolderVariation variation : RenameFolderVariation.values()) {
      Path originalFolder = new Path("folderToRename");
      if (variation != RenameFolderVariation.CreateJustInnerFile) {
        assertTrue(fs.mkdirs(originalFolder));
      }
      Path innerFile = new Path(originalFolder, "innerFile");
      Path innerFile2 = new Path(originalFolder, "innerFile2");
      if (variation != RenameFolderVariation.CreateJustFolder) {
        assertTrue(fs.createNewFile(innerFile));
        assertTrue(fs.createNewFile(innerFile2));
      }
      Path destination = new Path("renamedFolder");
      assertTrue(fs.rename(originalFolder, destination));
      assertTrue(fs.exists(destination));
      if (variation != RenameFolderVariation.CreateJustFolder) {
        assertTrue(fs.exists(new Path(destination, innerFile.getName())));
        assertTrue(fs.exists(new Path(destination, innerFile2.getName())));
      }
      assertFalse(fs.exists(originalFolder));
      assertFalse(fs.exists(innerFile));
      assertFalse(fs.exists(innerFile2));
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
      writeStringToFile(localFs, localFilePath, "Testing");
      Path dstPath = methodPath();
      assertTrue(FileUtil.copy(localFs, localFilePath, fs, dstPath, false,
          fs.getConf()));
      assertPathExists("coied from local", dstPath);
      assertEquals("Testing", readStringFromFile(fs, dstPath));
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
  public void testChineseCharacters() throws Exception {
    // Create a file and a folder with Chinese (non-ASCI) characters
    String chinese = "" + '\u963f' + '\u4db5';
    String fileName = "filename" + chinese;
    String directoryName = chinese;
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
  public void testChineseCharactersFolderRename() throws Exception {
    // Create a file and a folder with Chinese (non-ASCI) characters
    String chinese = "" + '\u963f' + '\u4db5';
    String fileName = "filename" + chinese;
    String srcDirectoryName = chinese;
    String targetDirectoryName = "target" + chinese;
    fs.create(new Path(srcDirectoryName, fileName)).close();
    fs.rename(new Path(srcDirectoryName), new Path(targetDirectoryName));
    FileStatus[] listing = fs.listStatus(new Path(targetDirectoryName));
    assertEquals(1, listing.length);
    assertEquals(fileName, listing[0].getPath().getName());
    FileStatus status = fs.getFileStatus(new Path(targetDirectoryName, fileName));
    assertEquals(fileName, status.getPath().getName());
    assertTrue(fs.delete(new Path(targetDirectoryName, fileName), true));
    assertTrue(fs.delete(new Path(targetDirectoryName), true));
  }

  @Test
  public void testReadingDirectoryAsFile() throws Exception {
    Path dir = methodPath();
    assertTrue(fs.mkdirs(dir));
    try {
      fs.open(dir).close();
      assertTrue("Should've thrown", false);
    } catch (FileNotFoundException ex) {
      assertExceptionContains("a directory not a file.", ex);
    }
  }

  @Test
  public void testCreatingFileOverDirectory() throws Exception {
    Path dir = methodPath();
    assertTrue(fs.mkdirs(dir));
    try {
      fs.create(dir).close();
      assertTrue("Should've thrown", false);
    } catch (IOException ex) {
      assertExceptionContains("Cannot create file", ex);
      assertExceptionContains("already exists as a directory", ex);
    }
  }

  @Test
  public void testInputStreamReadWithZeroSizeBuffer() throws Exception {
    Path newFile = methodPath();
    OutputStream output = fs.create(newFile);
    output.write(10);
    output.close();

    InputStream input = fs.open(newFile);
    int result = input.read(new byte[2], 0, 0);
    assertEquals(0, result);
  }

  @Test
  public void testInputStreamReadWithBufferReturnsMinusOneOnEof() throws Exception {
    Path newFile = methodPath();
    OutputStream output = fs.create(newFile);
    output.write(10);
    output.close();

    // Read first byte back
    InputStream input = fs.open(newFile);
    byte[] buff = new byte[1];
    int result = input.read(buff, 0, 1);
    assertEquals(1, result);
    assertEquals(10, buff[0]);

    // Issue another read and make sure it returns -1
    buff[0] = 2;
    result = input.read(buff, 0, 1);
    assertEquals(-1, result);
    // Buffer is intact
    assertEquals(2, buff[0]);
  }

  @Test
  public void testInputStreamReadWithBufferReturnsMinusOneOnEofForLargeBuffer() throws Exception {
    Path newFile = methodPath();
    OutputStream output = fs.create(newFile);
    byte[] outputBuff = new byte[97331];
    for(int i = 0; i < outputBuff.length; ++i) {
      outputBuff[i] = (byte)(Math.random() * 255);
    }
    output.write(outputBuff);
    output.close();

    // Read the content of the file
    InputStream input = fs.open(newFile);
    byte[] buff = new byte[131072];
    int result = input.read(buff, 0, buff.length);
    assertEquals(outputBuff.length, result);
    for(int i = 0; i < outputBuff.length; ++i) {
      assertEquals(outputBuff[i], buff[i]);
    }

    // Issue another read and make sure it returns -1
    buff = new byte[131072];
    result = input.read(buff, 0, buff.length);
    assertEquals(-1, result);
  }

  @Test
  public void testInputStreamReadIntReturnsMinusOneOnEof() throws Exception {
    Path newFile = methodPath();
    OutputStream output = fs.create(newFile);
    output.write(10);
    output.close();

    // Read first byte back
    InputStream input = fs.open(newFile);
    int value = input.read();
    assertEquals(10, value);

    // Issue another read and make sure it returns -1
    value = input.read();
    assertEquals(-1, value);
  }

  @Test
  public void testSetPermissionOnFile() throws Exception {
    Path newFile = methodPath();
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

    // Don't check the file length for page blobs. Only block blobs
    // provide the actual length of bytes written.
    if (!(this instanceof ITestNativeAzureFSPageBlobLive)) {
      assertEquals(1, newStatus.getLen());
    }
  }

  @Test
  public void testSetPermissionOnFolder() throws Exception {
    Path newFolder = methodPath();
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
    Path newFile = methodPath();
    OutputStream output = fs.create(newFile);
    output.write(13);
    output.close();
    fs.setOwner(newFile, "newUser", null);
    FileStatus newStatus = fs.getFileStatus(newFile);
    assertNotNull(newStatus);
    assertEquals("newUser", newStatus.getOwner());
    assertEquals("supergroup", newStatus.getGroup());

    // File length is only reported to be the size of bytes written to the file for block blobs.
    // So only check it for block blobs, not page blobs.
    if (!(this instanceof ITestNativeAzureFSPageBlobLive)) {
      assertEquals(1, newStatus.getLen());
    }
    fs.setOwner(newFile, null, "newGroup");
    newStatus = fs.getFileStatus(newFile);
    assertNotNull(newStatus);
    assertEquals("newUser", newStatus.getOwner());
    assertEquals("newGroup", newStatus.getGroup());
  }

  @Test
  public void testSetOwnerOnFolder() throws Exception {
    Path newFolder = methodPath();
    assertTrue(fs.mkdirs(newFolder));
    fs.setOwner(newFolder, "newUser", null);
    FileStatus newStatus = fs.getFileStatus(newFolder);
    assertNotNull(newStatus);
    assertEquals("newUser", newStatus.getOwner());
    assertTrue(newStatus.isDirectory());
  }

  @Test
  public void testModifiedTimeForFile() throws Exception {
    Path testFile = methodPath();
    fs.create(testFile).close();
    testModifiedTime(testFile);
  }

  @Test
  public void testModifiedTimeForFolder() throws Exception {
    Path testFolder = methodPath();
    assertTrue(fs.mkdirs(testFolder));
    testModifiedTime(testFolder);
  }

  @Test
  public void testFolderLastModifiedTime() throws Exception {
    Path parentFolder = methodPath();
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

  /**
   * Verify we can get file status of a directory with various forms of
   * the directory file name, including the nonstandard but legal form
   * ending in "/.". Check that we're getting status for a directory.
   */
  @Test
  public void testListSlash() throws Exception {
    Path testFolder = new Path("/testFolder");
    Path testFile = new Path(testFolder, "testFile");
    assertTrue(fs.mkdirs(testFolder));
    assertTrue(fs.createNewFile(testFile));
    FileStatus status;
    status = fs.getFileStatus(new Path("/testFolder"));
    assertTrue(status.isDirectory());
    status = fs.getFileStatus(new Path("/testFolder/"));
    assertTrue(status.isDirectory());
    status = fs.getFileStatus(new Path("/testFolder/."));
    assertTrue(status.isDirectory());
  }

  @Test
  public void testCannotCreatePageBlobByDefault() throws Exception {

    // Verify that the page blob directory list configuration setting
    // is not set in the default configuration.
    Configuration conf = new Configuration();
    String[] rawPageBlobDirs =
        conf.getStrings(AzureNativeFileSystemStore.KEY_PAGE_BLOB_DIRECTORIES);
    assertTrue(rawPageBlobDirs == null);
  }

  /*
   * Set up a situation where a folder rename is partway finished.
   * Then apply redo to finish the rename.
   *
   * The original source folder *would* have had contents
   * folderToRename  (0 byte dummy file for directory)
   * folderToRename/innerFile
   * folderToRename/innerFile2
   *
   * The actual source folder (after partial rename and failure)
   *
   * folderToRename
   * folderToRename/innerFile2
   *
   * The actual target folder (after partial rename and failure)
   *
   * renamedFolder
   * renamedFolder/innerFile
   */
  @Test
  public void testRedoRenameFolder() throws IOException {
    // create original folder
    String srcKey = "folderToRename";
    Path originalFolder = new Path(srcKey);
    assertTrue(fs.mkdirs(originalFolder));
    Path innerFile = new Path(originalFolder, "innerFile");
    assertTrue(fs.createNewFile(innerFile));
    Path innerFile2 = new Path(originalFolder, "innerFile2");
    assertTrue(fs.createNewFile(innerFile2));

    String dstKey = "renamedFolder";

    // propose (but don't do) the rename
    Path home = fs.getHomeDirectory();
    String relativeHomeDir = getRelativePath(home.toString());
    NativeAzureFileSystem.FolderRenamePending pending =
        new NativeAzureFileSystem.FolderRenamePending(
            relativeHomeDir + "/" + srcKey,
            relativeHomeDir + "/" + dstKey, null,
            (NativeAzureFileSystem) fs);

    // get the rename pending file contents
    String renameDescription = pending.makeRenamePendingFileContents();

    // Remove one file from source folder to simulate a partially done
    // rename operation.
    assertTrue(fs.delete(innerFile, false));

    // Create the destination folder with just one file in it, again
    // to simulate a partially done rename.
    Path destination = new Path(dstKey);
    Path innerDest = new Path(destination, "innerFile");
    assertTrue(fs.createNewFile(innerDest));

    // Create a rename-pending file and write rename information to it.
    final String renamePendingStr = "folderToRename-RenamePending.json";
    Path renamePendingFile = new Path(renamePendingStr);
    FSDataOutputStream out = fs.create(renamePendingFile, true);
    assertTrue(out != null);
    writeStringToStream(out, renameDescription);

    // Redo the rename operation based on the contents of the -RenamePending.json file.
    // Trigger the redo by checking for existence of the original folder. It must appear
    // to not exist.
    assertFalse(fs.exists(originalFolder));

    // Verify that the target is there, and the source is gone.
    assertTrue(fs.exists(destination));
    assertTrue(fs.exists(new Path(destination, innerFile.getName())));
    assertTrue(fs.exists(new Path(destination, innerFile2.getName())));
    assertFalse(fs.exists(originalFolder));
    assertFalse(fs.exists(innerFile));
    assertFalse(fs.exists(innerFile2));

    // Verify that there's no RenamePending file left.
    assertFalse(fs.exists(renamePendingFile));

    // Verify that we can list the target directory.
    FileStatus[] listed = fs.listStatus(destination);
    assertEquals(2, listed.length);

    // List the home directory and show the contents is a directory.
    Path root = fs.getHomeDirectory();
    listed = fs.listStatus(root);
    assertEquals(1, listed.length);
    assertTrue(listed[0].isDirectory());
  }

  /**
   * If there is a folder to be renamed inside a parent folder,
   * then when you list the parent folder, you should only see
   * the final result, after the rename.
   */
  @Test
  public void testRedoRenameFolderInFolderListing() throws IOException {

    // create original folder
    String parent = "parent";
    Path parentFolder = new Path(parent);
    assertTrue(fs.mkdirs(parentFolder));
    Path inner = new Path(parentFolder, "innerFolder");
    assertTrue(fs.mkdirs(inner));
    Path inner2 = new Path(parentFolder, "innerFolder2");
    assertTrue(fs.mkdirs(inner2));
    Path innerFile = new Path(inner2, "file");
    assertTrue(fs.createNewFile(innerFile));

    Path inner2renamed = new Path(parentFolder, "innerFolder2Renamed");

    // propose (but don't do) the rename of innerFolder2
    Path home = fs.getHomeDirectory();
    String relativeHomeDir = getRelativePath(home.toString());
    NativeAzureFileSystem.FolderRenamePending pending =
        new NativeAzureFileSystem.FolderRenamePending(
            relativeHomeDir + "/" + inner2,
            relativeHomeDir + "/" + inner2renamed, null,
            (NativeAzureFileSystem) fs);

    // Create a rename-pending file and write rename information to it.
    final String renamePendingStr = inner2 + FolderRenamePending.SUFFIX;
    Path renamePendingFile = new Path(renamePendingStr);
    FSDataOutputStream out = fs.create(renamePendingFile, true);
    assertTrue(out != null);
    writeStringToStream(out, pending.makeRenamePendingFileContents());

    // Redo the rename operation based on the contents of the
    // -RenamePending.json file. Trigger the redo by checking for existence of
    // the original folder. It must appear to not exist.
    FileStatus[] listed = fs.listStatus(parentFolder);
    assertEquals(2, listed.length);
    assertTrue(listed[0].isDirectory());
    assertTrue(listed[1].isDirectory());

    // The rename pending file is not a directory, so at this point we know the
    // redo has been done.
    assertFalse(fs.exists(inner2)); // verify original folder is gone
    assertTrue(fs.exists(inner2renamed)); // verify the target is there
    assertTrue(fs.exists(new Path(inner2renamed, "file")));
  }

  /**
   * There is a nested folder and file under the folder to be renamed
   * and the process crashes after the nested folder has been renamed but not the file.
   * then when you list the parent folder, pending renames should be redone
   * Apache jira HADOOP-12780
   */
  @Test
  public void testRedoRenameFolderRenameInProgress() throws IOException {

    // create original folder
    String parent = "parent";
    Path parentFolder = new Path(parent);
    assertTrue(fs.mkdirs(parentFolder));
    Path folderToBeRenamed = new Path(parentFolder, "folderToBeRenamed");
    assertTrue(fs.mkdirs(folderToBeRenamed));
    String innerFolderName = "innerFolder";
    Path inner = new Path(folderToBeRenamed, innerFolderName);
    assertTrue(fs.mkdirs(inner));
    String innerFileName = "file";
    Path innerFile = new Path(inner, innerFileName);
    assertTrue(fs.createNewFile(innerFile));

    Path renamedFolder = new Path(parentFolder, "renamedFolder");

    // propose (but don't do) the rename of innerFolder2
    Path home = fs.getHomeDirectory();
    String relativeHomeDir = getRelativePath(home.toString());
    NativeAzureFileSystem.FolderRenamePending pending =
        new NativeAzureFileSystem.FolderRenamePending(
            relativeHomeDir + "/" + folderToBeRenamed,
            relativeHomeDir + "/" + renamedFolder, null,
            (NativeAzureFileSystem) fs);

    // Create a rename-pending file and write rename information to it.
    final String renamePendingStr = folderToBeRenamed + FolderRenamePending.SUFFIX;
    Path renamePendingFile = new Path(renamePendingStr);
    FSDataOutputStream out = fs.create(renamePendingFile, true);
    assertTrue(out != null);
    writeStringToStream(out, pending.makeRenamePendingFileContents());

    // Rename inner folder to simulate the scenario where rename has started and
    // only one directory has been renamed but not the files under it
    ((NativeAzureFileSystem) fs).getStoreInterface().rename(
        relativeHomeDir + "/" +inner, relativeHomeDir + "/" +renamedFolder + "/" + innerFolderName , true, null);

    // Instead of using fs.exist use store.explicitFileExists because fs.exist will return true
    // even if directory has been renamed, but there are still file under that directory
    assertFalse(((NativeAzureFileSystem) fs).getStoreInterface().
        explicitFileExists(relativeHomeDir + "/" + inner)); // verify the explicit inner folder is gone
    assertTrue(((NativeAzureFileSystem) fs).getStoreInterface().
        explicitFileExists(relativeHomeDir + "/" + innerFile)); // verify inner file is present

    // Redo the rename operation based on the contents of the
    // -RenamePending.json file. Trigger the redo by checking for existence of
    // the original folder. It must appear to not exist.
    FileStatus[] listed = fs.listStatus(parentFolder);
    assertEquals(1, listed.length);
    assertTrue(listed[0].isDirectory());

    // The rename pending file is not a directory, so at this point we know the
    // redo has been done.
    assertFalse(fs.exists(inner)); // verify original folder is gone
    assertFalse(fs.exists(innerFile)); // verify original file is gone
    assertTrue(fs.exists(renamedFolder)); // verify the target is there
    assertTrue(fs.exists(new Path(renamedFolder, innerFolderName + "/" + innerFileName)));
  }

  /**
   * Test the situation when the rename metadata file is empty
   * i.e. it is created but not written yet. In that case in next rename
   * this empty file should be deleted. As zero byte metadata file means
   * rename has not started yet. This is to emulate the scenario where
   * the process crashes just after creating rename metadata file.
   *  We had a bug (HADOOP-12678) that in that case listing used to fail and
   * hbase master did not use to come up
   */
  @Test
  public void testRedoRenameFolderInFolderListingWithZeroByteRenameMetadata()
      throws IOException {
    // create original folder
    String parent = "parent";
    Path parentFolder = new Path(parent);
    assertTrue(fs.mkdirs(parentFolder));
    Path inner = new Path(parentFolder, "innerFolder");
    assertTrue(fs.mkdirs(inner));
    Path inner2 = new Path(parentFolder, "innerFolder2");
    assertTrue(fs.mkdirs(inner2));
    Path innerFile = new Path(inner2, "file");
    assertTrue(fs.createNewFile(innerFile));

    Path inner2renamed = new Path(parentFolder, "innerFolder2Renamed");

    // Create an empty rename-pending file
    final String renamePendingStr = inner2 + FolderRenamePending.SUFFIX;
    Path renamePendingFile = new Path(renamePendingStr);
    FSDataOutputStream out = fs.create(renamePendingFile, true);
    assertTrue(out != null);
    out.close();

    // Redo the rename operation based on the contents of the
    // -RenamePending.json file. Trigger the redo by listing
    // the parent folder. It should not throw and it should
    // delete empty rename pending file
    FileStatus[] listed = fs.listStatus(parentFolder);
    assertEquals(2, listed.length);
    assertTrue(listed[0].isDirectory());
    assertTrue(listed[1].isDirectory());
    assertFalse(fs.exists(renamePendingFile));

    // Verify that even if rename pending file is deleted,
    // deletion should handle that
    Path home = fs.getHomeDirectory();
    String relativeHomeDir = getRelativePath(home.toString());
    NativeAzureFileSystem.FolderRenamePending pending =
            new NativeAzureFileSystem.FolderRenamePending(
                relativeHomeDir + "/" + inner2,
                relativeHomeDir + "/" + inner2renamed, null,
                (NativeAzureFileSystem) fs);
    pending.deleteRenamePendingFile(fs, renamePendingFile);

    assertTrue(fs.exists(inner2)); // verify original folder is there
    assertFalse(fs.exists(inner2renamed)); // verify the target is not there
  }

  /**
   * Test the situation where a rename pending file exists but the rename
   * is really done. This could happen if the rename process died just
   * before deleting the rename pending file. It exercises a non-standard
   * code path in redo().
   */
  @Test
  public void testRenameRedoFolderAlreadyDone() throws IOException {
    // create only destination folder
    String orig = "originalFolder";
    String dest = "renamedFolder";
    Path destPath = new Path(dest);
    assertTrue(fs.mkdirs(destPath));

    // propose (but don't do) the rename of innerFolder2
    Path home = fs.getHomeDirectory();
    String relativeHomeDir = getRelativePath(home.toString());
    NativeAzureFileSystem.FolderRenamePending pending =
        new NativeAzureFileSystem.FolderRenamePending(
            relativeHomeDir + "/" + orig,
            relativeHomeDir + "/" + dest, null,
            (NativeAzureFileSystem) fs);

    // Create a rename-pending file and write rename information to it.
    final String renamePendingStr = orig + FolderRenamePending.SUFFIX;
    Path renamePendingFile = new Path(renamePendingStr);
    FSDataOutputStream out = fs.create(renamePendingFile, true);
    assertTrue(out != null);
    writeStringToStream(out, pending.makeRenamePendingFileContents());

    try {
      pending.redo();
    } catch (Exception e) {
      fail();
    }

    // Make sure rename pending file is gone.
    FileStatus[] listed = fs.listStatus(new Path("/"));
    assertEquals("Pending directory still found", 1, listed.length);
    assertTrue(listed[0].isDirectory());
  }

  @Test
  public void testRedoFolderRenameAll() throws IllegalArgumentException, IOException {
    {
      FileFolder original = new FileFolder("folderToRename");
      original.add("innerFile").add("innerFile2");
      FileFolder partialSrc = original.copy();
      FileFolder partialDst = original.copy();
      partialDst.setName("renamedFolder");
      partialSrc.setPresent(0, false);
      partialDst.setPresent(1, false);

      testRenameRedoFolderSituation(original, partialSrc, partialDst);
    }
    {
      FileFolder original = new FileFolder("folderToRename");
      original.add("file1").add("file2").add("file3");
      FileFolder partialSrc = original.copy();
      FileFolder partialDst = original.copy();
      partialDst.setName("renamedFolder");

      // Set up this state before the redo:
      // folderToRename: file1       file3
      // renamedFolder:  file1 file2
      // This gives code coverage for all 3 expected cases for individual file
      // redo.
      partialSrc.setPresent(1, false);
      partialDst.setPresent(2, false);

      testRenameRedoFolderSituation(original, partialSrc, partialDst);
    }
    {
      // Simulate a situation with folder with a large number of files in it.
      // For the first half of the files, they will be in the destination
      // but not the source. For the second half, they will be in the source
      // but not the destination. There will be one file in the middle that is
      // in both source and destination. Then trigger redo and verify.
      // For testing larger folder sizes, manually change this, temporarily, and
      // edit the SIZE value.
      final int SIZE = 5;
      assertTrue(SIZE >= 3);
      // Try a lot of files in the folder.
      FileFolder original = new FileFolder("folderToRename");
      for (int i = 0; i < SIZE; i++) {
        original.add("file" + Integer.toString(i));
      }
      FileFolder partialSrc = original.copy();
      FileFolder partialDst = original.copy();
      partialDst.setName("renamedFolder");
      for (int i = 0; i < SIZE; i++) {
        partialSrc.setPresent(i, i >= SIZE / 2);
        partialDst.setPresent(i, i <= SIZE / 2);
      }

      testRenameRedoFolderSituation(original, partialSrc, partialDst);
    }
    {
      // Do a nested folder, like so:
      // folderToRename:
      //   nestedFolder: a, b, c
      //   p
      //   q
      //
      // Then delete file 'a' from the source and add it to destination.
      // Then trigger redo.

      FileFolder original = new FileFolder("folderToRename");
      FileFolder nested = new FileFolder("nestedFolder");
      nested.add("a").add("b").add("c");
      original.add(nested).add("p").add("q");

      FileFolder partialSrc = original.copy();
      FileFolder partialDst = original.copy();
      partialDst.setName("renamedFolder");

      // logically remove 'a' from source
      partialSrc.getMember(0).setPresent(0, false);

      // logically eliminate b, c from destination
      partialDst.getMember(0).setPresent(1, false);
      partialDst.getMember(0).setPresent(2, false);

      testRenameRedoFolderSituation(original, partialSrc, partialDst);
    }
  }

  private void testRenameRedoFolderSituation(
      FileFolder fullSrc,
      FileFolder partialSrc,
      FileFolder partialDst) throws IllegalArgumentException, IOException {

    // make file folder tree for source
    fullSrc.create();

    // set up rename pending file
    fullSrc.makeRenamePending(partialDst);

    // prune away some files (as marked) from source to simulate partial rename
    partialSrc.prune();

    // Create only the files indicated for the destination to indicate a partial rename.
    partialDst.create();

    // trigger redo
    assertFalse(fullSrc.exists());

    // verify correct results
    partialDst.verifyExists();
    fullSrc.verifyGone();

    // delete the new folder to leave no garbage behind
    fs.delete(new Path(partialDst.getName()), true);
  }

  // Mock up of a generalized folder (which can also be a leaf-level file)
  // for rename redo testing.
  private class FileFolder {
    private String name;

    // For rename testing, indicates whether an expected
    // file is present in the source or target folder.
    private boolean present;
    ArrayList<FileFolder> members; // Null if a leaf file, otherwise not null.

    // Make a new, empty folder (not a regular leaf file).
    public FileFolder(String name) {
      this.name = name;
      this.present = true;
      members = new ArrayList<FileFolder>();
    }

    public FileFolder getMember(int i) {
      return members.get(i);
    }

    // Verify a folder and all its contents are gone. This is only to
    // be called on the root of a FileFolder.
    public void verifyGone() throws IllegalArgumentException, IOException {
      assertFalse(fs.exists(new Path(name)));
      assertTrue(isFolder());
      verifyGone(new Path(name), members);
    }

    private void verifyGone(Path prefix, ArrayList<FileFolder> members2) throws IOException {
      for (FileFolder f : members2) {
        f.verifyGone(prefix);
      }
    }

    private void verifyGone(Path prefix) throws IOException {
      assertFalse(fs.exists(new Path(prefix, name)));
      if (isLeaf()) {
        return;
      }
      for (FileFolder f : members) {
        f.verifyGone(new Path(prefix, name));
      }
    }

    public void verifyExists() throws IllegalArgumentException, IOException {

      // verify the root is present
      assertTrue(fs.exists(new Path(name)));
      assertTrue(isFolder());

      // check the members
      verifyExists(new Path(name), members);
    }

    private void verifyExists(Path prefix, ArrayList<FileFolder> members2) throws IOException {
      for (FileFolder f : members2) {
        f.verifyExists(prefix);
      }
    }

    private void verifyExists(Path prefix) throws IOException {

      // verify this file/folder is present
      assertTrue(fs.exists(new Path(prefix, name)));

      // verify members are present
      if (isLeaf()) {
        return;
      }

      for (FileFolder f : members) {
        f.verifyExists(new Path(prefix, name));
      }
    }

    public boolean exists() throws IOException {
      return fs.exists(new Path(name));
    }

    // Make a rename pending file for the situation where we rename
    // this object (the source) to the specified destination.
    public void makeRenamePending(FileFolder dst) throws IOException {

      // Propose (but don't do) the rename.
      Path home = fs.getHomeDirectory();
      String relativeHomeDir = getRelativePath(home.toString());
      NativeAzureFileSystem.FolderRenamePending pending =
          new NativeAzureFileSystem.FolderRenamePending(
              relativeHomeDir + "/" + this.getName(),
              relativeHomeDir + "/" + dst.getName(), null,
              (NativeAzureFileSystem) fs);

      // Get the rename pending file contents.
      String renameDescription = pending.makeRenamePendingFileContents();

      // Create a rename-pending file and write rename information to it.
      final String renamePendingStr = this.getName() + "-RenamePending.json";
      Path renamePendingFile = new Path(renamePendingStr);
      FSDataOutputStream out = fs.create(renamePendingFile, true);
      assertTrue(out != null);
      writeStringToStream(out, renameDescription);
    }

    // set whether a child is present or not
    public void setPresent(int i, boolean b) {
      members.get(i).setPresent(b);
    }

    // Make an uninitialized folder
    private FileFolder() {
      this.present = true;
    }

    public void setPresent(boolean value) {
      present = value;
    }

    public FileFolder makeLeaf(String name) {
      FileFolder f = new FileFolder();
      f.setName(name);
      return f;
    }

    void setName(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public boolean isLeaf() {
      return members == null;
    }

    public boolean isFolder() {
      return members != null;
    }

    FileFolder add(FileFolder folder) {
      members.add(folder);
      return this;
    }

    // Add a leaf file (by convention, if you pass a string argument, you get a leaf).
    FileFolder add(String file) {
      FileFolder leaf = makeLeaf(file);
      members.add(leaf);
      return this;
    }

    public FileFolder copy() {
      if (isLeaf()) {
        return makeLeaf(name);
      } else {
        FileFolder f = new FileFolder(name);
        for (FileFolder member : members) {
          f.add(member.copy());
        }
        return f;
      }
    }

    // Create the folder structure. Return true on success, or else false.
    public void create() throws IllegalArgumentException, IOException {
      create(null);
    }

    private void create(Path prefix) throws IllegalArgumentException, IOException {
      if (isFolder()) {
        if (present) {
          assertTrue(fs.mkdirs(makePath(prefix, name)));
        }
        create(makePath(prefix, name), members);
      } else if (isLeaf()) {
        if (present) {
          assertTrue(fs.createNewFile(makePath(prefix, name)));
        }
      } else {
        assertTrue("The object must be a (leaf) file or a folder.", false);
      }
    }

    private void create(Path prefix, ArrayList<FileFolder> members2) throws IllegalArgumentException, IOException {
      for (FileFolder f : members2) {
        f.create(prefix);
      }
    }

    private Path makePath(Path prefix, String name) {
      if (prefix == null) {
        return new Path(name);
      } else {
        return new Path(prefix, name);
      }
    }

    // Remove the files marked as not present.
    public void prune() throws IOException {
      prune(null);
    }

    private void prune(Path prefix) throws IOException {
      Path path = null;
      if (prefix == null) {
        path = new Path(name);
      } else {
        path = new Path(prefix, name);
      }
      if (isLeaf() && !present) {
        assertTrue(fs.delete(path, false));
      } else if (isFolder() && !present) {
        assertTrue(fs.delete(path, true));
      } else if (isFolder()) {
        for (FileFolder f : members) {
          f.prune(path);
        }
      }
    }
  }

  private String getRelativePath(String path) {
    // example input: wasb://wasbtests-ehans-1404322046279@ehans9.blob.core.windows.net/user/ehans/folderToRename
    // example result: user/ehans/folderToRename

    // Find the third / position and return input substring after that.
    int slashCount = 0; // number of slashes so far
    int i;
    for (i = 0; i < path.length(); i++) {
      if (path.charAt(i) == '/') {
        slashCount++;
        if (slashCount == 3) {
          return path.substring(i + 1, path.length());
        }
      }
    }
    throw new RuntimeException("Incorrect path prefix -- expected wasb://.../...");
  }

  @Test
  public void testCloseFileSystemTwice() throws Exception {
    //make sure close() can be called multiple times without doing any harm
    fs.close();
    fs.close();
  }

  // Test the available() method for the input stream returned by fs.open().
  // This works for both page and block blobs.
  int FILE_SIZE = 4 * 1024 * 1024 + 1; // Make this 1 bigger than internal
                                       // buffer used in BlobInputStream
                                       // to exercise that case.
  int MAX_STRIDE = FILE_SIZE + 1;
  Path PATH = new Path("/available.dat");
  @Test
  public void testAvailable() throws IOException {

    // write FILE_SIZE bytes to page blob
    FSDataOutputStream out = fs.create(PATH);
    byte[] data = new byte[FILE_SIZE];
    Arrays.fill(data, (byte) 5);
    out.write(data, 0, FILE_SIZE);
    out.close();

    // Test available() for different read sizes
    verifyAvailable(1);
    verifyAvailable(100);
    verifyAvailable(5000);
    verifyAvailable(FILE_SIZE);
    verifyAvailable(MAX_STRIDE);

    fs.delete(PATH, false);
  }

  // Verify that available() for the input stream is always >= 1 unless we've
  // consumed all the input, and then it is 0. This is to match expectations by
  // HBase which were set based on behavior of DFSInputStream.available().
  private void verifyAvailable(int readStride) throws IOException {
    FSDataInputStream in = fs.open(PATH);
    try {
      byte[] inputBuffer = new byte[MAX_STRIDE];
      int position = 0;
      int bytesRead = 0;
      while(bytesRead != FILE_SIZE) {
        bytesRead += in.read(inputBuffer, position, readStride);
        int available = in.available();
        if (bytesRead < FILE_SIZE) {
          if (available < 1) {
            fail(String.format(
                  "expected available > 0 but got: "
                      + "position = %d, bytesRead = %d, in.available() = %d",
                  position, bytesRead, available));
          }
        }
      }
      int available = in.available();
      assertTrue(available == 0);
    } finally {
      in.close();
    }
  }

  @Test
  public void testGetFileSizeFromListing() throws IOException {
    Path path = new Path("file.dat");
    final int PAGE_SIZE = 512;
    final int FILE_SIZE = PAGE_SIZE + 1;

    // write FILE_SIZE bytes to page blob
    FSDataOutputStream out = fs.create(path);
    byte[] data = new byte[FILE_SIZE];
    Arrays.fill(data, (byte) 5);
    out.write(data, 0, FILE_SIZE);
    out.close();

    // list the file to get its properties
    FileStatus[] status = fs.listStatus(path);
    assertEquals(1, status.length);

    // The file length should report the number of bytes
    // written for either page or block blobs (subclasses
    // of this test class will exercise both).
    assertEquals(FILE_SIZE, status[0].getLen());
  }

  private boolean testModifiedTime(Path testPath, long time) throws Exception {
    FileStatus fileStatus = fs.getFileStatus(testPath);
    final long errorMargin = modifiedTimeErrorMargin;
    long lastModified = fileStatus.getModificationTime();
    return (lastModified > (time - errorMargin) && lastModified < (time + errorMargin));
  }

  @Test
  public void testCreateNonRecursive() throws Exception {
    Path testFolder = new Path("/testFolder");
    Path testFile = new Path(testFolder, "testFile");
    try {
      fs.createNonRecursive(testFile, true, 1024, (short)1, 1024, null);
      assertTrue("Should've thrown", false);
    } catch (FileNotFoundException e) {
    }
    fs.mkdirs(testFolder);
    fs.createNonRecursive(testFile, true, 1024, (short)1, 1024, null)
      .close();
    assertTrue(fs.exists(testFile));
  }

  public void testFileEndingInDot() throws Exception {
    Path testFolder = new Path("/testFolder.");
    Path testFile = new Path(testFolder, "testFile.");
    assertTrue(fs.mkdirs(testFolder));
    assertTrue(fs.createNewFile(testFile));
    assertTrue(fs.exists(testFile));
    FileStatus[] listed = fs.listStatus(testFolder);
    assertEquals(1, listed.length);
    assertEquals("testFile.", listed[0].getPath().getName());
  }
  private void testModifiedTime(Path testPath) throws Exception {
    Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    long currentUtcTime = utc.getTime().getTime();
    FileStatus fileStatus = fs.getFileStatus(testPath);
    final long errorMargin = 60 * 1000; // Give it +/-60 seconds
    assertTrue("Modification time " +
        new Date(fileStatus.getModificationTime()) + " is not close to now: " +
        utc.getTime(),
        fileStatus.getModificationTime() > (currentUtcTime - errorMargin) &&
        fileStatus.getModificationTime() < (currentUtcTime + errorMargin));
  }

  private void createEmptyFile(Path testFile, FsPermission permission)
      throws IOException {
    FSDataOutputStream outputStream = fs.create(testFile, permission, true,
        4096, (short) 1, 1024, null);
    outputStream.close();
  }

  private String readString(Path testFile) throws IOException {
    return readStringFromFile(fs, testFile);
  }


  private void writeString(Path path, String value) throws IOException {
    writeStringToFile(fs, path, value);
  }

  @Test
  // Acquire and free a Lease object. Wait for more than the lease
  // timeout, to make sure the lease renews itself.
  public void testSelfRenewingLease() throws IllegalArgumentException, IOException,
    InterruptedException, StorageException {

    SelfRenewingLease lease;
    final String FILE_KEY = "file";
    fs.create(new Path(FILE_KEY));
    NativeAzureFileSystem nfs = (NativeAzureFileSystem) fs;
    String fullKey = nfs.pathToKey(nfs.makeAbsolute(new Path(FILE_KEY)));
    AzureNativeFileSystemStore store = nfs.getStore();
    lease = store.acquireLease(fullKey);
    assertTrue(lease.getLeaseID() != null);

    // The sleep time for the keep-alive thread is 40 seconds, so sleep just
    // a little beyond that, to make sure the keep-alive thread wakes up
    // and renews the lease.
    Thread.sleep(42000);
    lease.free();

    // Check that the lease is really freed.
    CloudBlob blob = lease.getCloudBlob();

    // Try to acquire it again, using direct Azure blob access.
    // If that succeeds, then the lease was already freed.
    String differentLeaseID = null;
    try {
      differentLeaseID = blob.acquireLease(15, null);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Caught exception trying to directly re-acquire lease from Azure");
    } finally {
      assertTrue(differentLeaseID != null);
      AccessCondition accessCondition = AccessCondition.generateEmptyCondition();
      accessCondition.setLeaseID(differentLeaseID);
      blob.releaseLease(accessCondition);
    }
  }

  @Test
  // Acquire a SelfRenewingLease object. Wait for more than the lease
  // timeout, to make sure the lease renews itself. Delete the file.
  // That will automatically free the lease.
  // (that should work without any failures).
  public void testSelfRenewingLeaseFileDelete()
      throws IllegalArgumentException, IOException,
        InterruptedException, StorageException {

    SelfRenewingLease lease;
    final String FILE_KEY = "file";
    final Path path = new Path(FILE_KEY);
    fs.create(path);
    NativeAzureFileSystem nfs = (NativeAzureFileSystem) fs;
    String fullKey = nfs.pathToKey(nfs.makeAbsolute(path));
    lease = nfs.getStore().acquireLease(fullKey);
    assertTrue(lease.getLeaseID() != null);

    // The sleep time for the keep-alive thread is 40 seconds, so sleep just
    // a little beyond that, to make sure the keep-alive thread wakes up
    // and renews the lease.
    Thread.sleep(42000);

    nfs.getStore().delete(fullKey, lease);

    // Check that the file is really gone and the lease is freed.
    assertTrue(!fs.exists(path));
    assertTrue(lease.isFreed());
  }

  // Variables to check assertions in next test.
  private long firstEndTime;
  private long secondStartTime;

  // Create two threads. One will get a lease on a file.
  // The second one will try to get the lease and thus block.
  // Then the first one will free the lease and the second
  // one will get it and proceed.
  @Test
  public void testLeaseAsDistributedLock() throws IllegalArgumentException,
      IOException {
    final String LEASE_LOCK_FILE_KEY = "file";
    fs.create(new Path(LEASE_LOCK_FILE_KEY));
    NativeAzureFileSystem nfs = (NativeAzureFileSystem) fs;
    String fullKey = nfs.pathToKey(nfs.makeAbsolute(new Path(LEASE_LOCK_FILE_KEY)));

    Thread first = new Thread(new LeaseLockAction("first-thread", fullKey));
    first.start();
    Thread second = new Thread(new LeaseLockAction("second-thread", fullKey));
    second.start();
    try {

      // Wait for the two  threads to finish.
      first.join();
      second.join();
      assertTrue(firstEndTime < secondStartTime);
    } catch (InterruptedException e) {
      fail("Unable to wait for threads to finish");
      Thread.currentThread().interrupt();
    }
  }

  private class LeaseLockAction implements Runnable {
    private String name;
    private String key;

    LeaseLockAction(String name, String key) {
      this.name = name;
      this.key = key;
    }

    @Override
    public void run() {
      LOG.info("starting thread " + name);
      SelfRenewingLease lease = null;
      NativeAzureFileSystem nfs = (NativeAzureFileSystem) fs;

      if (name.equals("first-thread")) {
        try {
          lease = nfs.getStore().acquireLease(key);
          LOG.info(name + " acquired lease " + lease.getLeaseID());
        } catch (AzureException e) {
          assertTrue("Unanticipated exception", false);
        }
        assertTrue(lease != null);
        try {

          // Sleep long enough for the lease to renew once.
          Thread.sleep(SelfRenewingLease.LEASE_RENEWAL_PERIOD + 2000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        try {
          firstEndTime = System.currentTimeMillis();
          lease.free();
          LOG.info(name + " freed lease " + lease.getLeaseID());
        } catch (StorageException e) {
          fail("Unanticipated exception");
        }
      } else if (name.equals("second-thread")) {
        try {

          // sleep 2 sec to let first thread get ahead of this one
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        try {
          LOG.info(name + " before getting lease");
          lease = nfs.getStore().acquireLease(key);
          secondStartTime = System.currentTimeMillis();
          LOG.info(name + " acquired lease " + lease.getLeaseID());
        } catch (AzureException e) {
          assertTrue("Unanticipated exception", false);
        }
        assertTrue(lease != null);
        try {
          lease.free();
          LOG.info(name + " freed lease " + lease.getLeaseID());
        } catch (StorageException e) {
          assertTrue("Unanticipated exception", false);
        }
      } else {
        fail("Unknown thread name");
      }

      LOG.info(name + " is exiting.");
    }

  }
}
