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
  @Ignore("HADOOP-15932")
  public void testCopyFileNoOverwriteDirectory() throws Throwable {
    file = createTempFile("hello");
    Path dest = upload(file, true);
    S3AFileSystem fs = getFileSystem();
    fs.delete(dest, false);
    fs.mkdirs(dest);
    intercept(FileAlreadyExistsException.class,
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
  @Ignore("HADOOP-15932")
  public void testCopyDirectoryFile() throws Throwable {
    file = File.createTempFile("test", ".txt");
    // first upload to create
    intercept(FileNotFoundException.class, "Not a file",
        () -> upload(file.getParentFile(), true));
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
}
