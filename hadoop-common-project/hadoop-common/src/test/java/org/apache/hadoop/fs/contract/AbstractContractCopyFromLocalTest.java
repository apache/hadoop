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

package org.apache.hadoop.fs.contract;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public abstract class AbstractContractCopyFromLocalTest extends
    AbstractFSContractTestBase {

  private static final Charset ASCII = StandardCharsets.US_ASCII;
  private File file;

  @Override
  public void teardown() throws Exception {
    super.teardown();
    if (file != null) {
      file.delete();
    }
  }

  @Test
  public void testCopyEmptyFile() throws Throwable {
    file = File.createTempFile("test", ".txt");
    Path dest = copyFromLocal(file, true);
    assertPathExists("uploaded file not found", dest);
  }

  @Test
  public void testCopyFile() throws Throwable {
    String message = "hello";
    file = createTempFile(message);
    Path dest = copyFromLocal(file, true);

    assertPathExists("uploaded file not found", dest);
    assertTrue("source file deleted", Files.exists(file.toPath()));

    FileSystem fs = getFileSystem();
    FileStatus status = fs.getFileStatus(dest);
    assertEquals("File length not equal " + status,
        message.getBytes(ASCII).length, status.getLen());
    assertFileTextEquals(dest, message);
  }

  @Test
  public void testCopyFileNoOverwrite() throws Throwable {
    file = createTempFile("hello");
    copyFromLocal(file, true);
    intercept(PathExistsException.class,
        () -> copyFromLocal(file, false));
  }

  @Test
  public void testCopyFileOverwrite() throws Throwable {
    file = createTempFile("hello");
    Path dest = copyFromLocal(file, true);
    String updated = "updated";
    FileUtils.write(file, updated, ASCII);
    copyFromLocal(file, true);
    assertFileTextEquals(dest, updated);
  }

  @Test
  public void testCopyMissingFile() throws Throwable {
    describe("Copying a file that's not there must fail.");
    file = createTempFile("test");
    file.delete();
    // first upload to create
    intercept(FileNotFoundException.class, "",
        () -> copyFromLocal(file, true));
  }

  @Test
  public void testSourceIsFileAndDelSrcTrue() throws Throwable {
    describe("Source is a file delSrc flag is set to true");

    file = createTempFile("test");
    copyFromLocal(file, false, true);

    assertFalse("Source file not deleted", Files.exists(file.toPath()));
  }

  @Test
  public void testSourceIsFileAndDestinationIsDirectory() throws Throwable {
    describe("Source is a file and destination is a directory. " +
        "File must be copied inside the directory.");

    file = createTempFile("test");
    Path source = new Path(file.toURI());
    FileSystem fs = getFileSystem();
    File dir = createTempDirectory("test");
    Path destination = fileToPath(dir);

    // Make sure there's nothing already existing at destination
    fs.delete(destination, false);
    mkdirs(destination);
    fs.copyFromLocalFile(source, destination);

    Path expectedFile = path(dir.getName() + "/" + source.getName());
    assertPathExists("File not copied into directory", expectedFile);
  }

  @Test
  public void testSourceIsFileAndDestinationIsNonExistentDirectory()
      throws Throwable {
    describe("Source is a file and destination directory does not exist. " +
        "Copy operation must still work.");

    file = createTempFile("test");
    Path source = new Path(file.toURI());
    FileSystem fs = getFileSystem();

    File dir = createTempDirectory("test");
    Path destination = fileToPath(dir);
    fs.delete(destination, false);
    assertPathDoesNotExist("Destination not deleted", destination);

    fs.copyFromLocalFile(source, destination);
    assertPathExists("Destination doesn't exist.", destination);
  }

  @Test
  public void testSrcIsDirWithFilesAndCopySuccessful() throws Throwable {
    describe("Source is a directory with files, copy must copy all" +
        " dir contents to destination");
    String firstChild = "childOne";
    String secondChild = "childTwo";
    File parent = createTempDirectory("parent");
    File root = parent.getParentFile();
    File childFile = createTempFile(parent, firstChild, firstChild);
    File secondChildFile = createTempFile(parent, secondChild, secondChild);

    copyFromLocal(parent, false);

    assertPathExists("Parent directory not copied", fileToPath(parent));
    assertFileTextEquals(fileToPath(childFile, root), firstChild);
    assertFileTextEquals(fileToPath(secondChildFile, root), secondChild);
  }

  @Test
  public void testSrcIsEmptyDirWithCopySuccessful() throws Throwable {
    describe("Source is an empty directory, copy must succeed");
    File source = createTempDirectory("source");
    Path dest = copyFromLocal(source, false);

    assertPathExists("Empty directory not copied", dest);
  }

  @Test
  public void testSrcIsDirWithOverwriteOptions() throws Throwable {
    describe("Source is a directory, destination exists and " +
        "must be overwritten.");

    FileSystem fs = getFileSystem();
    File source = createTempDirectory("source");
    Path sourcePath = new Path(source.toURI());
    String contents = "test file";
    File child = createTempFile(source, "child", contents);

    Path dest = path(source.getName()).getParent();
    fs.copyFromLocalFile(sourcePath, dest);
    intercept(PathExistsException.class,
        () -> fs.copyFromLocalFile(false, false,
            sourcePath, dest));

    String updated = "updated contents";
    FileUtils.write(child, updated, ASCII);
    fs.copyFromLocalFile(sourcePath, dest);

    assertPathExists("Parent directory not copied", fileToPath(source));
    assertFileTextEquals(fileToPath(child, source.getParentFile()),
        updated);
  }

  @Test
  public void testSrcIsDirWithDelSrcOptions() throws Throwable {
    describe("Source is a directory containing a file and delSrc flag is set" +
        ", this must delete the source after the copy.");
    File source = createTempDirectory("source");
    String contents = "child file";
    File child = createTempFile(source, "child", contents);

    copyFromLocal(source, false, true);
    Path dest = fileToPath(child, source.getParentFile());

    assertFalse("Directory not deleted", Files.exists(source.toPath()));
    assertFileTextEquals(dest, contents);
  }

  /*
   * The following path is being created on disk and copied over
   * /parent/ (directory)
   * /parent/test1.txt
   * /parent/child/test.txt
   * /parent/secondChild/ (directory)
   */
  @Test
  public void testCopyTreeDirectoryWithoutDelete() throws Throwable {
    File srcDir = createTempDirectory("parent");
    File childDir = createTempDirectory(srcDir, "child");
    File secondChild = createTempDirectory(srcDir, "secondChild");
    File parentFile = createTempFile(srcDir, "test1", ".txt");
    File childFile = createTempFile(childDir, "test2", ".txt");

    copyFromLocal(srcDir, false, false);
    File root = srcDir.getParentFile();

    assertPathExists("Parent directory not found",
        fileToPath(srcDir));
    assertPathExists("Child directory not found",
        fileToPath(childDir, root));
    assertPathExists("Second child directory not found",
        fileToPath(secondChild, root));
    assertPathExists("Parent file not found",
        fileToPath(parentFile, root));
    assertPathExists("Child file not found",
        fileToPath(childFile, root));
  }

  @Test
  public void testCopyDirectoryWithDelete() throws Throwable {
    java.nio.file.Path srcDir = Files.createTempDirectory("parent");
    Files.createTempFile(srcDir, "test1", ".txt");

    Path src = new Path(srcDir.toUri());
    Path dst = path(srcDir.getFileName().toString());
    getFileSystem().copyFromLocalFile(true, true, src, dst);

    assertFalse("Source directory was not deleted",
        Files.exists(srcDir));
  }

  @Test
  public void testSourceIsDirectoryAndDestinationIsFile() throws Throwable {
    describe("Source is a directory and destination is a file must fail");

    File file = createTempFile("local");
    File source = createTempDirectory("srcDir");
    Path destination = copyFromLocal(file, false);
    Path sourcePath = new Path(source.toURI());

    intercept(FileAlreadyExistsException.class,
        () -> getFileSystem().copyFromLocalFile(false, true,
            sourcePath, destination));
  }

  protected Path fileToPath(File file) throws IOException {
    return path(file.getName());
  }

  protected Path fileToPath(File file, File parent) throws IOException {
    return path(parent
        .toPath()
        .relativize(file.toPath())
        .toString());
  }

  protected File createTempDirectory(String name) throws IOException {
    return Files.createTempDirectory(name).toFile();
  }

  protected Path copyFromLocal(File srcFile, boolean overwrite) throws
      IOException {
    return copyFromLocal(srcFile, overwrite, false);
  }

  protected Path copyFromLocal(File srcFile, boolean overwrite, boolean delSrc)
      throws IOException {
    Path src = new Path(srcFile.toURI());
    Path dst = path(srcFile.getName());
    getFileSystem().copyFromLocalFile(delSrc, overwrite, src, dst);
    return dst;
  }

  /**
   * Create a temp file with some text.
   * @param text text for the file
   * @return the file
   * @throws IOException on a failure
   */
  protected File createTempFile(String text) throws IOException {
    File f = File.createTempFile("test", ".txt");
    FileUtils.write(f, text, ASCII);
    return f;
  }

  protected File createTempFile(File parent, String name, String text)
      throws IOException {
    File f = File.createTempFile(name, ".txt", parent);
    FileUtils.write(f, text, ASCII);
    return f;
  }

  protected File createTempDirectory(File parent, String name)
      throws IOException {
    return Files.createTempDirectory(parent.toPath(), name).toFile();
  }

  private void assertFileTextEquals(Path path, String expected)
      throws IOException {
    assertEquals("Wrong data in " + path,
        expected, IOUtils.toString(getFileSystem().open(path), ASCII));
  }
}
