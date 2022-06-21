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

package org.apache.hadoop.fs.s3a.audit;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * MergerTest will implement different tests on Merger class methods.
 */
public class TestS3AAuditLogMerger {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestS3AAuditLogMerger.class);

  private final S3AAuditLogMerger s3AAuditLogMerger = new S3AAuditLogMerger();

  /**
   * Sample directories and files to test.
   */
  private final File auditLogFile = new File("AuditLogFile");
  private File file1;
  private File file2;
  private File file3;
  private File dir1;
  private File dir2;

  /**
   * Testing the mergeFiles method in Merger class.
   * by passing a sample directory which contains files with some content in it.
   * and checks if files in a directory are merged into single file.
   */
  @Test
  public void testMergeFiles() throws IOException {
    dir1 = Files.createTempDirectory("sampleFilesDirectory").toFile();
    file1 = File.createTempFile("sampleFile1", ".txt", dir1);
    file2 = File.createTempFile("sampleFile2", ".txt", dir1);
    file3 = File.createTempFile("sampleFile3", ".txt", dir1);
    try (FileWriter fw = new FileWriter(file1);
        FileWriter fw1 = new FileWriter(file2);
        FileWriter fw2 = new FileWriter(file3)) {
      fw.write("abcd");
      fw1.write("efgh");
      fw2.write("ijkl");
    }
    s3AAuditLogMerger.mergeFiles(dir1.getPath());
    String str =
        new String(Files.readAllBytes(Paths.get(auditLogFile.getPath())));
    //File content of each audit log file in merged audit log file are
    // divided by '\n'.
    // Performing assertions will be easy by replacing '\n' with ''
    String fileText = str.replace("\n", "");
    assertTrue("the string 'abcd' should be in the merged file",
        fileText.contains("abcd"));
    assertTrue("the string 'efgh' should be in the merged file",
        fileText.contains("efgh"));
    assertTrue("the string 'ijkl' should be in the merged file",
        fileText.contains("ijkl"));
  }

  /**
   * Testing the merged file.
   * by passing different directories which contains files with some content.
   * in it and checks if the file is overwritten by new file contents.
   */
  @Test
  public void testMergedFile() throws IOException {
    //Testing the merged file with contents of first directory
    dir1 = Files.createTempDirectory("sampleFilesDirectory").toFile();
    file1 = File.createTempFile("sampleFile1", ".txt", dir1);
    file2 = File.createTempFile("sampleFile2", ".txt", dir1);
    try (FileWriter fw = new FileWriter(file1);
        FileWriter fw1 = new FileWriter(file2)) {
      fw.write("abcd");
      fw1.write("efgh");
    }
    s3AAuditLogMerger.mergeFiles(dir1.getPath());
    String str =
        new String(Files.readAllBytes(Paths.get(auditLogFile.getPath())));
    //File content of each audit log file in merged audit log file are
    // divided by '\n'.
    // Performing assertions will be easy by replacing '\n' with ''
    String fileText = str.replace("\n", "");
    assertTrue("the string 'abcd' should be in the merged file",
        fileText.contains("abcd"));
    assertTrue("the string 'efgh' should be in the merged file",
        fileText.contains("efgh"));
    assertFalse("the string 'ijkl' should not be in the merged file",
        fileText.contains("ijkl"));

    //Testing the merged file with contents of second directory
    dir2 = Files.createTempDirectory("sampleFilesDirectory1").toFile();
    file3 = File.createTempFile("sampleFile3", ".txt", dir2);
    try (FileWriter fw2 = new FileWriter(file3)) {
      fw2.write("ijkl");
    }
    s3AAuditLogMerger.mergeFiles(dir2.getPath());
    String str1 =
        new String(Files.readAllBytes(Paths.get(auditLogFile.getPath())));
    //File content of each audit log file in merged audit log file are
    // divided by '\n'.
    // Performing assertions will be easy by replacing '\n' with ''
    String fileText1 = str1.replace("\n", "");
    assertFalse("the string 'abcd' should not be in the merged file",
        fileText1.contains("abcd"));
    assertFalse("the string 'efgh' should not be in the merged file",
        fileText1.contains("efgh"));
    assertTrue("the string 'ijkl' should be in the merged file",
        fileText1.contains("ijkl"));
  }

  /**
   * Testing the mergeFiles method in Merger class.
   * by passing an empty directory and checks if merged file is created or not.
   */
  @Test
  public void testMergeFilesEmptyDir() throws IOException {
    dir1 = Files.createTempDirectory("emptyFilesDirectory").toFile();
    if (auditLogFile.exists()) {
      LOG.info("AuditLogFile already exists and we are deleting it here");
      if (auditLogFile.delete()) {
        LOG.debug("AuditLogFile deleted");
      }
    }
    s3AAuditLogMerger.mergeFiles(dir1.getPath());
    assertFalse(
        "This AuditLogFile shouldn't exist if input directory is empty ",
        auditLogFile.exists());
  }

  /**
   * Delete all the sample directories and sample files after all tests.
   */
  @After
  public void tearDown() throws Exception {
    if (auditLogFile.exists()) {
      auditLogFile.delete();
    }
    if (file1 != null) {
      file1.delete();
    }
    if (file2 != null) {
      file2.delete();
    }
    if (file3 != null) {
      file3.delete();
    }
    if (dir1 != null) {
      dir1.delete();
    }
    if (dir2 != null) {
      dir2.delete();
    }
  }
}
