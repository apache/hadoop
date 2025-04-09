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
package org.apache.hadoop.mapred.gridmix;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

/**
 * Test the basic functionality of PseudoLocalFs
 */
public class TestPseudoLocalFs {

  /**
   * Test if a file on PseudoLocalFs of a specific size can be opened and read.
   * Validate the size of the data read.
   * Test the read methods of {@link PseudoLocalFs.RandomInputStream}.
   * @throws Exception
   */
  @Test
  public void testPseudoLocalFsFileSize() throws Exception {
    long fileSize = 10000;
    Path path = PseudoLocalFs.generateFilePath("myPsedoFile", fileSize);
    PseudoLocalFs pfs = new PseudoLocalFs();
    pfs.create(path);

    // Read 1 byte at a time and validate file size.
    InputStream in = pfs.open(path, 0);
    long totalSize = 0;

    while (in.read() >= 0) {
      ++totalSize;
    }
    in.close();
    assertEquals(fileSize, totalSize, "File size mismatch with read().");

    // Read data from PseudoLocalFs-based file into buffer to
    // validate read(byte[]) and file size.
    in = pfs.open(path, 0);
    totalSize = 0;
    byte[] b = new byte[1024];
    int bytesRead = in.read(b);
    while (bytesRead >= 0) {
      totalSize += bytesRead;
      bytesRead = in.read(b);
    }
    assertEquals(fileSize, totalSize, "File size mismatch with read(byte[]).");
  }

  /**
   * Validate if file status is obtained for correctly formed file paths on
   * PseudoLocalFs and also verify if appropriate exception is thrown for
   * invalid file paths.
   * @param pfs Pseudo Local File System
   * @param path file path for which getFileStatus() is to be called
   * @param shouldSucceed <code>true</code> if getFileStatus() should succeed
   * @throws IOException
   */
  private void validateGetFileStatus(FileSystem pfs, Path path,
      boolean shouldSucceed) throws IOException {
    boolean expectedExceptionSeen = false;
    FileStatus stat = null;
    try {
      stat = pfs.getFileStatus(path);
    } catch(FileNotFoundException e) {
      expectedExceptionSeen = true;
    }
    if (shouldSucceed) {
      assertFalse(expectedExceptionSeen,
          "getFileStatus() has thrown Exception for valid file name " + path);
      assertNotNull(stat, "Missing file status for a valid file.");

      // validate fileSize
      String[] parts = path.toUri().getPath().split("\\.");
      long expectedFileSize = Long.parseLong(parts[parts.length - 1]);
      assertEquals(expectedFileSize, stat.getLen(), "Invalid file size.");
    } else {
      assertTrue(expectedExceptionSeen,
          "getFileStatus() did not throw Exception for invalid file "
          + " name " + path);
    }
  }

  /**
   * Validate if file creation succeeds for correctly formed file paths on
   * PseudoLocalFs and also verify if appropriate exception is thrown for
   * invalid file paths.
   * @param pfs Pseudo Local File System
   * @param path file path for which create() is to be called
   * @param shouldSucceed <code>true</code> if create() should succeed
   * @throws IOException
   */
  private void validateCreate(FileSystem pfs, Path path,
      boolean shouldSucceed) throws IOException {
    boolean expectedExceptionSeen = false;
    try {
      pfs.create(path);
    } catch(IOException e) {
      expectedExceptionSeen = true;
    }
    if (shouldSucceed) {
      assertFalse(expectedExceptionSeen,
          "create() has thrown Exception for valid file name "
          + path);
    } else {
      assertTrue(expectedExceptionSeen,
          "create() did not throw Exception for invalid file name "
          + path);
    }
  }

  /**
   * Validate if opening of file succeeds for correctly formed file paths on
   * PseudoLocalFs and also verify if appropriate exception is thrown for
   * invalid file paths.
   * @param pfs Pseudo Local File System
   * @param path file path for which open() is to be called
   * @param shouldSucceed <code>true</code> if open() should succeed
   * @throws IOException
   */
  private void validateOpen(FileSystem pfs, Path path,
      boolean shouldSucceed) throws IOException {
    boolean expectedExceptionSeen = false;
    try {
      pfs.open(path);
    } catch(IOException e) {
      expectedExceptionSeen = true;
    }
    if (shouldSucceed) {
      assertFalse(expectedExceptionSeen,
          "open() has thrown Exception for valid file name "
          + path);
    } else {
      assertTrue(expectedExceptionSeen,
          "open() did not throw Exception for invalid file name "
          + path);
    }
  }

  /**
   * Validate if exists() returns <code>true</code> for correctly formed file
   * paths on PseudoLocalFs and returns <code>false</code> for improperly
   * formed file paths.
   * @param pfs Pseudo Local File System
   * @param path file path for which exists() is to be called
   * @param shouldSucceed expected return value of exists(&lt;path&gt;)
   * @throws IOException
   */
  private void validateExists(FileSystem pfs, Path path,
      boolean shouldSucceed) throws IOException {
    boolean ret = pfs.exists(path);
    if (shouldSucceed) {
      assertTrue(ret, "exists() returned false for valid file name " + path);
    } else {
      assertFalse(ret, "exists() returned true for invalid file name " + path);
    }
  }

  /**
   *  Test Pseudo Local File System methods like getFileStatus(), create(),
   *  open(), exists() for <li> valid file paths and <li> invalid file paths.
   * @throws IOException
   */
  @Test
  public void testPseudoLocalFsFileNames() throws IOException {
    PseudoLocalFs pfs = new PseudoLocalFs();
    Configuration conf = new Configuration();
    conf.setClass("fs.pseudo.impl", PseudoLocalFs.class, FileSystem.class);

    Path path = new Path("pseudo:///myPsedoFile.1234");
    FileSystem testFs = path.getFileSystem(conf);
    assertEquals(pfs.getUri().getScheme(), testFs.getUri().getScheme(),
        "Failed to obtain a pseudo local file system object from path");

    // Validate PseudoLocalFS operations on URI of some other file system
    path = new Path("file:///myPsedoFile.12345");
    validateGetFileStatus(pfs, path, false);
    validateCreate(pfs, path, false);
    validateOpen(pfs, path, false);
    validateExists(pfs, path, false);

    path = new Path("pseudo:///myPsedoFile");//.<fileSize> missing
    validateGetFileStatus(pfs, path, false);
    validateCreate(pfs, path, false);
    validateOpen(pfs, path, false);
    validateExists(pfs, path, false);

    // thing after final '.' is not a number
    path = new Path("pseudo:///myPsedoFile.txt");
    validateGetFileStatus(pfs, path, false);
    validateCreate(pfs, path, false);
    validateOpen(pfs, path, false);
    validateExists(pfs, path, false);

    // Generate valid file name(relative path) and validate operations on it
    long fileSize = 231456;
    path = PseudoLocalFs.generateFilePath("my.Psedo.File", fileSize);
    // Validate the above generateFilePath()
    assertEquals(fileSize, pfs.validateFileNameFormat(path), "generateFilePath() failed.");

    validateGetFileStatus(pfs, path, true);
    validateCreate(pfs, path, true);
    validateOpen(pfs, path, true);
    validateExists(pfs, path, true);

    // Validate operations on valid qualified path
    path = new Path("myPsedoFile.1237");
    path = pfs.makeQualified(path);
    validateGetFileStatus(pfs, path, true);
    validateCreate(pfs, path, true);
    validateOpen(pfs, path, true);
    validateExists(pfs, path, true);
  }
}
