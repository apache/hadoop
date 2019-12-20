/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * File utilities for tests.
 * Provides methods that can create, delete files or directories
 * in a temp directory, or any specified directory.
 */
public class FileUtilitiesForTests {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileUtilitiesForTests.class);
  private String tempDir;
  private List<File> cleanupFiles;

  public void setup() {
    cleanupFiles = Lists.newArrayList();
    tempDir = System.getProperty("java.io.tmpdir");
  }

  public void teardown() throws IOException {
    LOG.info("About to clean up files: " + cleanupFiles);
    List<File> dirs = Lists.newArrayList();
    for (File cleanupFile : cleanupFiles) {
      if (cleanupFile.isDirectory()) {
        dirs.add(cleanupFile);
      } else {
        deleteFile(cleanupFile);
      }
    }

    for (File dir : dirs) {
      deleteFile(dir);
    }
  }

  public File createFileInTempDir(String filename) throws IOException {
    File file = new File(tempDir, new Path(filename).getName());
    createFile(file);
    return file;
  }

  public File createDirInTempDir(String dirName) {
    File file = new File(tempDir, new Path(dirName).getName());
    createDirectory(file);
    return file;
  }

  public File createFileInDir(Path dir, String filename) throws IOException {
    File dirTmp = new File(dir.toUri().getPath());
    if (!dirTmp.exists()) {
      createDirectory(dirTmp);
    }
    File file =
        new File(dir.toUri().getPath() + "/" + new Path(filename).getName());
    createFile(file);
    return file;
  }

  public File createFileInDir(File dir, String filename) throws IOException {
    if (!dir.exists()) {
      createDirectory(dir);
    }
    File file = new File(dir, filename);
    createFile(file);
    return file;
  }

  public File createDirectory(Path parent, String dirname) {
    File dir =
        new File(parent.toUri().getPath() + "/" + new Path(dirname).getName());
    createDirectory(dir);
    return dir;
  }

  public File createDirectory(File parent, String dirname) {
    File dir =
        new File(parent.getPath() + "/" + new Path(dirname).getName());
    createDirectory(dir);
    return dir;
  }

  private void createDirectory(File dir) {
    boolean result = dir.mkdir();
    assertTrue("Failed to create directory " + dir.getAbsolutePath(), result);
    assertTrue("Directory does not exist: " + dir.getAbsolutePath(),
        dir.exists());
    this.cleanupFiles.add(dir);
  }

  private void createFile(File file) throws IOException {
    boolean result = file.createNewFile();
    assertTrue("Failed to create file " + file.getAbsolutePath(), result);
    assertTrue("File does not exist: " + file.getAbsolutePath(), file.exists());
    this.cleanupFiles.add(file);
  }

  private static void deleteFile(File file) throws IOException {
    if (file.isDirectory()) {
      LOG.info("Removing directory: " + file.getAbsolutePath());
      FileUtils.deleteDirectory(file);
    }

    if (file.exists()) {
      LOG.info("Removing file: " + file.getAbsolutePath());
      boolean result = file.delete();
      assertTrue("Deletion of file " + file.getAbsolutePath()
          + " was not successful!", result);
    }
  }

  public File getTempFileWithName(String filename) {
    return new File(tempDir + "/" + new Path(filename).getName());
  }

  public static File getFilename(Path parent, String filename) {
    return new File(
        parent.toUri().getPath() + "/" + new Path(filename).getName());
  }

  public void addTrackedFile(File file) {
    this.cleanupFiles.add(file);
  }
}
