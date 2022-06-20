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

  private static final Logger LOG =
      LoggerFactory.getLogger(TestS3AAuditLogMerger.class);

  private final S3AAuditLogMerger s3AAuditLogMerger = new S3AAuditLogMerger();

  /**
   * Sample directories and files to test.
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
   * Creates the sample directories and files before each test.
   */
  @Before
  public void setUp() throws IOException {
    boolean sampleDirCreation = sampleDirectory.mkdir();
    boolean emptyDirCreation = emptyDirectory.mkdir();
    if (sampleDirCreation && emptyDirCreation) {
      try (FileWriter fw = new FileWriter(firstSampleFile);
          FileWriter fw1 = new FileWriter(secondSampleFile);
          FileWriter fw2 = new FileWriter(thirdSampleFile)) {
        fw.write("abcd");
        fw1.write("efgh");
        fw2.write("ijkl");
      }
    }
  }

  /**
   * Testing the mergeFiles method in Merger class.
   * by passing a sample directory which contains files with some content in it.
   * and checks if files in a directory are merged into single file.
   */
  @Test
  public void testMergeFiles() throws IOException {
    s3AAuditLogMerger.mergeFiles(sampleDirectory.getPath());
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
   * Testing the mergeFiles method in Merger class.
   * by passing an empty directory and checks if merged file is created or not.
   */
  @Test
  public void testMergeFilesEmptyDir() throws IOException {
    if (auditLogFile.exists()) {
      LOG.info("AuditLogFile already exists and we are deleting it here");
      if (auditLogFile.delete()) {
        LOG.debug("AuditLogFile deleted");
      }
    }
    s3AAuditLogMerger.mergeFiles(emptyDirectory.getPath());
    assertFalse("This AuditLogFile shouldn't exist if input directory is empty ",
        auditLogFile.exists());
  }

  /**
   * Delete all the sample directories and sample files after all tests.
   */
  @After
  public void tearDown() throws Exception {
    if (firstSampleFile.delete() && secondSampleFile.delete()
        && thirdSampleFile.delete() && sampleDirectory.delete()
        && emptyDirectory.delete() && auditLogFile.delete()) {
      LOG.debug("sample files regarding testing deleted");
    }
  }
}
