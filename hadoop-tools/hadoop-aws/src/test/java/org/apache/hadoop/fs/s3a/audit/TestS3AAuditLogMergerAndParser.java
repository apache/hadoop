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
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.audit.mapreduce.S3AAuditLogMergerAndParser;

/**
 * This will implement different tests on S3AAuditLogMergerAndParser class.
 */
public class TestS3AAuditLogMergerAndParser extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestS3AAuditLogMergerAndParser.class);

  /**
   * A real log entry.
   * This is derived from a real log entry on a test run.
   * If this needs to be updated, please do it from a real log.
   * Splitting this up across lines has a tendency to break things, so
   * be careful making changes.
   */
  static final String SAMPLE_LOG_ENTRY =
      "183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4000000"
          + " bucket-london"
          + " [13/May/2021:11:26:06 +0000]"
          + " 109.157.171.174"
          + " arn:aws:iam::152813717700:user/dev"
          + " M7ZB7C4RTKXJKTM9"
          + " REST.PUT.OBJECT"
          + " fork-0001/test/testParseBrokenCSVFile"
          + " \"PUT /fork-0001/test/testParseBrokenCSVFile HTTP/1.1\""
          + " 200"
          + " -"
          + " -"
          + " 794"
          + " 55"
          + " 17"
          + " \"https://audit.example.org/hadoop/1/op_create/"
          + "e8ede3c7-8506-4a43-8268-fe8fcbb510a4-00000278/"
          + "?op=op_create"
          + "&p1=fork-0001/test/testParseBrokenCSVFile"
          + "&pr=alice"
          + "&ps=2eac5a04-2153-48db-896a-09bc9a2fd132"
          + "&id=e8ede3c7-8506-4a43-8268-fe8fcbb510a4-00000278&t0=154"
          + "&fs=e8ede3c7-8506-4a43-8268-fe8fcbb510a4&t1=156&"
          + "ts=1620905165700\""
          + " \"Hadoop 3.4.0-SNAPSHOT, java/1.8.0_282 vendor/AdoptOpenJDK\""
          + " -"
          + " TrIqtEYGWAwvu0h1N9WJKyoqM0TyHUaY+ZZBwP2yNf2qQp1Z/0="
          + " SigV4"
          + " ECDHE-RSA-AES128-GCM-SHA256"
          + " AuthHeader"
          + " bucket-london.s3.eu-west-2.amazonaws.com"
          + " TLSv1.2" + "\n";

  static final String SAMPLE_LOG_ENTRY_1 =
      "01234567890123456789"
          + " bucket-london1"
          + " [13/May/2021:11:26:06 +0000]"
          + " 109.157.171.174"
          + " arn:aws:iam::152813717700:user/dev"
          + " M7ZB7C4RTKXJKTM9"
          + " REST.PUT.OBJECT"
          + " fork-0001/test/testParseBrokenCSVFile"
          + " \"PUT /fork-0001/test/testParseBrokenCSVFile HTTP/1.1\""
          + " 200"
          + " -"
          + " -"
          + " 794"
          + " 55"
          + " 17"
          + " \"https://audit.example.org/hadoop/1/op_create/"
          + "e8ede3c7-8506-4a43-8268-fe8fcbb510a4-00000278/"
          + "?op=op_create"
          + "&p1=fork-0001/test/testParseBrokenCSVFile"
          + "&pr=alice"
          + "&ps=2eac5a04-2153-48db-896a-09bc9a2fd132"
          + "&id=e8ede3c7-8506-4a43-8268-fe8fcbb510a4-00000278&t0=154"
          + "&fs=e8ede3c7-8506-4a43-8268-fe8fcbb510a4&t1=156&"
          + "ts=1620905165700\""
          + " \"Hadoop 3.4.0-SNAPSHOT, java/1.8.0_282 vendor/AdoptOpenJDK\""
          + " -"
          + " TrIqtEYGWAwvu0h1N9WJKyoqM0TyHUaY+ZZBwP2yNf2qQp1Z/0="
          + " SigV4"
          + " ECDHE-RSA-AES128-GCM-SHA256"
          + " AuthHeader"
          + " bucket-london.s3.eu-west-2.amazonaws.com"
          + " TLSv1.2" + "\n";

  /**
   * A real referrer header entry.
   * This is derived from a real log entry on a test run.
   * If this needs to be updated, please do it from a real log.
   * Splitting this up across lines has a tendency to break things, so
   * be careful making changes.
   */
  private final String sampleReferrerHeader =
      "\"https://audit.example.org/hadoop/1/op_create/"
          + "e8ede3c7-8506-4a43-8268-fe8fcbb510a4-00000278/?"
          + "op=op_create"
          + "&p1=fork-0001/test/testParseBrokenCSVFile"
          + "&pr=alice"
          + "&ps=2eac5a04-2153-48db-896a-09bc9a2fd132"
          + "&id=e8ede3c7-8506-4a43-8268-fe8fcbb510a4-00000278&t0=154"
          + "&fs=e8ede3c7-8506-4a43-8268-fe8fcbb510a4&t1=156"
          + "&ts=1620905165700\"";

  /**
   * Sample directories and files to test.
   */
  private File sampleFile;
  private File sampleDir;
  private File sampleDestDir;

  private final S3AAuditLogMergerAndParser s3AAuditLogMergerAndParser =
      new S3AAuditLogMergerAndParser();

  /**
   * Testing parseAuditLog method in parser class by passing sample audit log
   * entry and checks if the log is parsed correctly.
   */
  @Test
  public void testParseAuditLog() {
    Map<String, String> parseAuditLogResult =
        s3AAuditLogMergerAndParser.parseAuditLog(SAMPLE_LOG_ENTRY);
    assertNotNull("the result of parseAuditLogResult should be not null",
        parseAuditLogResult);
    //verifying the bucket from parsed audit log
    assertEquals("the expected and actual results should be same",
        "bucket-london", parseAuditLogResult.get("bucket"));
    //verifying the remoteip from parsed audit log
    assertEquals("the expected and actual results should be same",
        "109.157.171.174", parseAuditLogResult.get("remoteip"));
  }

  /**
   * Testing parseAuditLog method in parser class by passing empty string and
   * null and checks if the result is empty.
   */
  @Test
  public void testParseAuditLogEmptyAndNull() {
    Map<String, String> parseAuditLogResultEmpty =
        s3AAuditLogMergerAndParser.parseAuditLog("");
    assertTrue("the returned list should be empty for this test",
        parseAuditLogResultEmpty.isEmpty());
    Map<String, String> parseAuditLogResultNull =
        s3AAuditLogMergerAndParser.parseAuditLog(null);
    assertTrue("the returned list should be empty for this test",
        parseAuditLogResultNull.isEmpty());
  }

  /**
   * Testing parseReferrerHeader method in parser class by passing
   * sample referrer header taken from sample audit log and checks if the
   * referrer header is parsed correctly.
   */
  @Test
  public void testParseReferrerHeader() {
    Map<String, String> parseReferrerHeaderResult =
        s3AAuditLogMergerAndParser.parseReferrerHeader(sampleReferrerHeader);
    assertNotNull("the result of parseReferrerHeaderResult should be not null",
        parseReferrerHeaderResult);
    //verifying the path 'p1' from parsed referrer header
    assertEquals("the expected and actual results should be same",
        "fork-0001/test/testParseBrokenCSVFile",
        parseReferrerHeaderResult.get("p1"));
    //verifying the principal 'pr' from parsed referrer header
    assertEquals("the expected and actual results should be same", "alice",
        parseReferrerHeaderResult.get("pr"));
  }

  /**
   * Testing parseReferrerHeader method in parser class by passing empty
   * string and null string and checks if the result is empty.
   */
  @Test
  public void testParseReferrerHeaderEmptyAndNull() {
    Map<String, String> parseReferrerHeaderResultEmpty =
        s3AAuditLogMergerAndParser.parseReferrerHeader("");
    assertTrue("the returned list should be empty for this test",
        parseReferrerHeaderResultEmpty.isEmpty());
    Map<String, String> parseReferrerHeaderResultNull =
        s3AAuditLogMergerAndParser.parseReferrerHeader(null);
    assertTrue("the returned list should be empty for this test",
        parseReferrerHeaderResultNull.isEmpty());
  }

  /**
   * Testing mergeAndParseAuditLogFiles method by passing filesystem, source
   * and destination paths.
   */
  @Test
  public void testMergeAndParseAuditLogFiles() throws IOException {
    sampleDir = Files.createTempDirectory("sampleDir").toFile();
    sampleFile = File.createTempFile("sampleFile", ".txt", sampleDir);
    try (FileWriter fw = new FileWriter(sampleFile)) {
      fw.write(SAMPLE_LOG_ENTRY);
      fw.write(SAMPLE_LOG_ENTRY_1);
      fw.flush();
    }
    sampleDestDir = Files.createTempDirectory("sampleDestDir").toFile();
    Path logsPath = new Path(sampleDir.toURI());
    Path destPath = new Path(sampleDestDir.toURI());
    FileSystem fileSystem = logsPath.getFileSystem(getConfiguration());
    boolean mergeAndParseResult =
        s3AAuditLogMergerAndParser.mergeAndParseAuditLogFiles(fileSystem,
            logsPath, destPath);
    assertTrue("the result should be true", mergeAndParseResult);
  }

  /**
   * Testing mergeAndParseAuditLogCounter method by passing filesystem,
   * sample files source and destination paths.
   */
  @Test
  public void testMergeAndParseAuditLogCounter() throws IOException {
    sampleDir = Files.createTempDirectory("sampleDir").toFile();
    File firstSampleFile =
        File.createTempFile("sampleFile1", ".txt", sampleDir);
    File secondSampleFile =
        File.createTempFile("sampleFile2", ".txt", sampleDir);
    File thirdSampleFile =
        File.createTempFile("sampleFile3", ".txt", sampleDir);
    try (FileWriter fw = new FileWriter(firstSampleFile);
        FileWriter fw1 = new FileWriter(secondSampleFile);
        FileWriter fw2 = new FileWriter(thirdSampleFile)) {
      fw.write(SAMPLE_LOG_ENTRY);
      fw1.write(SAMPLE_LOG_ENTRY);
      fw2.write(SAMPLE_LOG_ENTRY_1);
    }
    sampleDestDir = Files.createTempDirectory("sampleDestDir").toFile();
    Path logsPath = new Path(sampleDir.toURI());
    Path destPath = new Path(sampleDestDir.toURI());
    FileSystem fileSystem = logsPath.getFileSystem(getConfiguration());
    boolean mergeAndParseResult =
        s3AAuditLogMergerAndParser.mergeAndParseAuditLogFiles(fileSystem,
            logsPath, destPath);
    assertTrue("the result should be true", mergeAndParseResult);

    long noOfAuditLogsParsed = s3AAuditLogMergerAndParser.getAuditLogsParsed();
    assertEquals("the expected and actual results should be same",
        3, noOfAuditLogsParsed);
  }
}
