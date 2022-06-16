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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * MergerTest will implement different tests on Merger class methods.
 */
public class TestS3AAuditLogMerger {

  private final Logger logger = LoggerFactory.getLogger(TestS3AAuditLogMerger.class);

  private final S3AAuditLogMerger s3AAuditLogMerger = new S3AAuditLogMerger();

  /**
   * sample directories and files to test.
   */
  private final File auditLogFile = new File("AuditLogFile");
  private final File sampleDirectory = new File("sampleFilesDirectory");
  private final File emptyDirectory = new File("emptyFilesDirectory");
  private final File firstSampleFile =
      new File("sampleFilesDirectory", "sampleFile1.txt");
  private final File secondSampleFile =
      new File("sampleFilesDirectory", "sampleFile2.txt");
  private final File thirdSampleFile =
      new File("sampleFilesDirectory", "sampleFile3.txt");

  /**
   * creates the sample directories and files before each test.
   *
   * @throws IOException on failure
   */
  @Before
  public void setUp() throws IOException {
    boolean sampleDirCreation = sampleDirectory.mkdir();
    boolean emptyDirCreation = emptyDirectory.mkdir();
    if (sampleDirCreation && emptyDirCreation) {
      try (FileWriter fw = new FileWriter(firstSampleFile,
          StandardCharsets.UTF_8);
          FileWriter fw1 = new FileWriter(secondSampleFile,
              StandardCharsets.UTF_8);
          FileWriter fw2 = new FileWriter(thirdSampleFile,
              StandardCharsets.UTF_8)) {
        fw.write("abcd");
        fw1.write("efgh");
        fw2.write("ijkl");
      }
    }
  }

  /**
   * mergeFilesTest() will test the mergeFiles() method in Merger class.
   * by passing a sample directory which contains files with some content in it
   * and checks if files in a directory are merged into single file
   *
   * @throws IOException on any failure
   */
  @Test
  public void mergeFilesTest() throws IOException {
    s3AAuditLogMerger.mergeFiles(sampleDirectory.getPath());
    String str =
        new String(Files.readAllBytes(Paths.get(auditLogFile.getPath())));
    String fileText = str.replace("\n", "");
    assertTrue("the string 'abcd' should be in the merged file",
        fileText.contains("abcd"));
    assertTrue("the string 'efgh' should be in the merged file",
        fileText.contains("efgh"));
    assertTrue("the string 'ijkl' should be in the merged file",
        fileText.contains("ijkl"));
  }

  /**
   * mergeFilesTestEmpty() will test the mergeFiles().
   * by passing an empty directory and checks if merged file is created or not
   *
   * @throws IOException on any failure
   */
  @Test
  public void mergeFilesTestEmpty() throws IOException {
    if (auditLogFile.exists()) {
      logger.info("AuditLogFile already exists and we are deleting it here");
      if (auditLogFile.delete()) {
        logger.debug("AuditLogFile deleted");
      }
    }
    s3AAuditLogMerger.mergeFiles(emptyDirectory.getPath());
    assertFalse("This AuditLogFile shouldn't exist if input directory is empty ",
        auditLogFile.exists());
  }

  /**
   * delete all the sample directories and files after all tests.
   *
   * @throws Exception on any failure
   */
  @After
  public void tearDown() throws Exception {
    if (auditLogFile.delete() && firstSampleFile.delete()
        && secondSampleFile.delete() &&
        thirdSampleFile.delete() && sampleDirectory.delete()
        && emptyDirectory.delete()) {
      logger.debug("sample files regarding testing deleted");
    }
  }
}
