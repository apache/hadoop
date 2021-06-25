/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;

import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.impl.CopyFromLocalOperation;
import org.apache.hadoop.fs.s3a.impl.StatusProbeEnum;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test {@link S3AFileSystem#copyFromLocalFile(boolean, boolean, Path, Path)}.
 * Some of the tests have been disabled pending a fix for HADOOP-15932 and
 * recursive directory copying; the test cases themselves may be obsolete.
 */
public class ITestS3ACopyFromLocalFile extends AbstractS3ATestBase {
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
    Path dest = upload(file, true);
    assertPathExists("uploaded file", dest);
  }

  @Test
  public void testCopyFile() throws Throwable {
    String message = "hello";
    file = createTempFile(message);
    Path dest = upload(file, true);
    assertPathExists("uploaded file not found", dest);
    S3AFileSystem fs = getFileSystem();
    FileStatus status = fs.getFileStatus(dest);
    assertEquals("File length of " + status,
        message.getBytes(ASCII).length, status.getLen());
    assertFileTextEquals(dest, message);
  }

  public void assertFileTextEquals(Path path, String expected)
      throws IOException {
    assertEquals("Wrong data in " + path,
        expected, IOUtils.toString(getFileSystem().open(path), ASCII));
  }

  @Test
  public void testCopyFileNoOverwrite() throws Throwable {
    file = createTempFile("hello");
    Path dest = upload(file, true);
    // HADOOP-15932: the exception type changes here
    intercept(PathExistsException.class,
        () -> upload(file, false));
  }

  @Test
  public void testCopyFileOverwrite() throws Throwable {
    file = createTempFile("hello");
    Path dest = upload(file, true);
    String updated = "updated";
    FileUtils.write(file, updated, ASCII);
    upload(file, true);
    assertFileTextEquals(dest, updated);
  }

  @Test
  public void testCopyFileNoOverwriteDirectory() throws Throwable {
    file = createTempFile("hello");
    Path dest = upload(file, true);
    S3AFileSystem fs = getFileSystem();
    fs.delete(dest, false);
    fs.mkdirs(dest);
    intercept(PathExistsException.class,
        () -> upload(file, true));
  }

  @Test
  public void testCopyMissingFile() throws Throwable {
    file = File.createTempFile("test", ".txt");
    file.delete();
    // first upload to create
    intercept(FileNotFoundException.class, "",
        () -> upload(file, true));
  }

  @Test
  public void testImplementationTemporary() throws Throwable {
    java.nio.file.Path srcDir = Files.createTempDirectory("parent");
    java.nio.file.Path childDir = Files.createTempDirectory(srcDir, "child");
    java.nio.file.Path secondChild = Files.createTempDirectory(srcDir, "secondChild");
    java.nio.file.Path parentFile = Files.createTempFile(srcDir, "test1", ".txt");
    java.nio.file.Path childFile = Files.createTempFile(childDir, "test2", ".txt");

    Path src = new Path(srcDir.toUri());
    Path dst = path(srcDir.getFileName().toString());

    S3AFileSystem fileSystem = getFileSystem();
    fileSystem.copyFromLocalFile(true, true, src, dst);

    java.nio.file.Path parent = srcDir.getParent();

    assertPathExists("Parent directory", srcDir, parent);
    assertPathExists("Child directory", childDir, parent);
    assertPathExists("Parent file", parentFile, parent);
    assertPathExists("Child file", childFile, parent);

  }

  /*
   * The following path is being created on disk and copied over
   * /parent/ (trailing slash to make it clear it's  a directory
   * /parent/test1.txt
   * /parent/child/test.txt
   */
  @Test
  public void testCopyTreeDirectoryWithoutDelete() throws Throwable {
    java.nio.file.Path srcDir = Files.createTempDirectory("parent");
    java.nio.file.Path childDir = Files.createTempDirectory(srcDir, "child");
    java.nio.file.Path parentFile = Files.createTempFile(srcDir, "test1", ".txt");
    java.nio.file.Path childFile = Files.createTempFile(childDir, "test2", ".txt");

    Path src = new Path(srcDir.toUri());
    Path dst = path(srcDir.getFileName().toString());
    getFileSystem().copyFromLocalFile(false, true, src, dst);

    java.nio.file.Path parent = srcDir.getParent();

    assertPathExists("Parent directory", srcDir, parent);
    assertPathExists("Child directory", childDir, parent);
    assertPathExists("Parent file", parentFile, parent);
    assertPathExists("Child file", childFile, parent);

    if (!Files.exists(srcDir)) {
      throw new Exception("Folder was deleted when it shouldn't have!");
    }
  }

  @Test
  public void testCopyDirectoryWithDelete() throws Throwable {
    java.nio.file.Path srcDir = Files.createTempDirectory("parent");
    Files.createTempFile(srcDir, "test1", ".txt");

    Path src = new Path(srcDir.toUri());
    Path dst = path(srcDir.getFileName().toString());
    getFileSystem().copyFromLocalFile(true, true, src, dst);

    if (Files.exists(srcDir)) {
        throw new Exception("Source directory should've been deleted");
    }
  }

  @Test
  public void testLocalFilesOnly() throws Throwable {
    Path dst = path("testLocalFilesOnly");
    intercept(IllegalArgumentException.class,
        () -> {
          getFileSystem().copyFromLocalFile(false, true, dst, dst);
          return "copy successful";
        });
  }

  public Path upload(File srcFile, boolean overwrite) throws IOException {
    Path src = new Path(srcFile.toURI());
    Path dst = path(srcFile.getName());
    getFileSystem().copyFromLocalFile(false, overwrite, src, dst);
    return dst;
  }

  /**
   * Create a temp file with some text.
   * @param text text for the file
   * @return the file
   * @throws IOException on a failure
   */
  public File createTempFile(String text) throws IOException {
    File f = File.createTempFile("test", ".txt");
    FileUtils.write(f, text, ASCII);
    return f;
  }

  private void assertPathExists(String message,
                                java.nio.file.Path toVerify,
                                java.nio.file.Path parent) throws IOException {
    Path qualifiedPath = path(parent.relativize(toVerify).toString());
    assertPathExists(message, qualifiedPath);
  }
}
