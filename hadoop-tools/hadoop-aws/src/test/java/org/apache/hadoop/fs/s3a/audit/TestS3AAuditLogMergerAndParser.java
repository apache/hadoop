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

import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.audit.mapreduce.S3AAuditLogMergerAndParser;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.util.Preconditions;

import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_PATH;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_PRINCIPAL;
import static org.apache.hadoop.fs.s3a.audit.mapreduce.S3AAuditLogMergerAndParser.parseAudit;

/**
 * Unit tests on {@link S3AAuditLogMergerAndParser} class.
 */
public class TestS3AAuditLogMergerAndParser extends AbstractHadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestS3AAuditLogMergerAndParser.class);

  @Rule
  public TestName methodName = new TestName();

  @BeforeClass
  public static void nameTestThread() {
    Thread.currentThread().setName("JUnit");
  }

  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit-" + getMethodName());
  }

  protected String getMethodName() {
    return methodName.getMethodName();
  }

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

  private File sampleDir;

  private S3AAuditLogMergerAndParser s3AAuditLogMergerAndParser =
      new S3AAuditLogMergerAndParser(new Configuration(), 1);


  /**
   * Testing parseAuditLog method in parser class by passing sample audit log
   * entry and checks if the log is parsed correctly.
   */
  @Test
  public void testParseAuditLog() {
    Map<String, String> parseAuditLogResult =
        s3AAuditLogMergerAndParser.parseAuditLog(SAMPLE_LOG_ENTRY);
    Assertions.assertThat(parseAuditLogResult)
        .describedAs("the result of parseAuditLogResult of %s", SAMPLE_LOG_ENTRY)
        .isNotNull();

    //verifying the bucket from parsed audit log
    assertFieldValue(parseAuditLogResult, "bucket", "bucket-london");

    //verifying the remoteip from parsed audit log
    assertFieldValue(parseAuditLogResult, "remoteip", "109.157.171.174");
  }

  /**
   * Assert that a field in the parsed audit log matches the expected value.
   * @param parseAuditLogResult parsed audit log
   * @param field field name
   * @param expected expected value
   * @return the ongoing assert
   */
  private static AbstractStringAssert<?> assertFieldValue(final Map<String, String> parseAuditLogResult,
      final String field,
      final String expected) {
    return Assertions.assertThat(parseAuditLogResult.get(field))
        .describedAs("Mismatch in the field %s parsed from the audit", field)
        .isEqualTo(expected);
  }

  /**
   * Testing parseAuditLog method in parser class by passing empty string and
   * null and checks if the result is empty.
   */
  @Test
  public void testParseAuditLogEmptyAndNull() {
    Map<String, String> parseAuditLogResultEmpty =
        s3AAuditLogMergerAndParser.parseAuditLog("");
    Assertions.assertThat(parseAuditLogResultEmpty)
        .describedAs("Audit log map should be empty")
        .isEmpty();
    Map<String, String> parseAuditLogResultNull =
        s3AAuditLogMergerAndParser.parseAuditLog(null);
    Assertions.assertThat(parseAuditLogResultNull)
        .describedAs("Audit log map of null record should be empty")
        .isEmpty();
  }

  /**
   * Testing parseReferrerHeader method in parser class by passing
   * sample referrer header taken from sample audit log and checks if the
   * referrer header is parsed correctly.
   */
  @Test
  public void testParseAudit() {
    Map<String, String> parseReferrerHeaderResult =
        parseAudit(sampleReferrerHeader);
    //verifying the path 'p1' from parsed referrer header
    Assertions.assertThat(parseReferrerHeaderResult)
        .describedAs("Mismatch in the path parsed from the referrer")
        .isNotNull()
        .containsEntry(PARAM_PATH, "fork-0001/test/testParseBrokenCSVFile")
        .describedAs("Referrer header map should contain the principal")
        .containsEntry(PARAM_PRINCIPAL, "alice");

  }

  /**
   * Testing parseReferrerHeader method in parser class by passing empty
   * string and null string and checks if the result is empty.
   */
  @Test
  public void testParseAuditEmptyAndNull() {
    Assertions.assertThat(parseAudit(""))
        .isEmpty();
    Assertions.assertThat(parseAudit(null))
        .isEmpty();
  }

  /**
   * Testing mergeAndParseAuditLogFiles method by passing filesystem, source
   * and destination paths.
   */
  @Test
  public void testMergeAndParseAuditLogFiles() throws IOException {
    // Getting the audit dir and file path from test/resources/
    String auditLogDir = this.getClass().getClassLoader().getResource(
        "TestAuditLogs").toString();
    String auditSingleFile = this.getClass().getClassLoader().getResource(
        "TestAuditLogs/sampleLog1").toString();
    Preconditions.checkArgument(!StringUtils.isAnyBlank(auditLogDir,
        auditSingleFile), String.format("Audit path should not be empty. Check "
            + "test/resources. auditLogDir : %s, auditLogSingleFile :%s",
        auditLogDir, auditSingleFile));
    Path auditDirPath = new Path(auditLogDir);
    Path auditFilePath = new Path(auditSingleFile);

    final Path destPath = tempAvroPath();
    boolean mergeAndParseResult =
        s3AAuditLogMergerAndParser.mergeAndParseAuditLogFiles(
            auditDirPath, destPath);
    Assertions.assertThat(mergeAndParseResult)
        .describedAs("The merge and parse failed for the audit log")
        .isTrue();
    // 36 audit logs with referrer in each of the 2 sample files.
    Assertions.assertThat(s3AAuditLogMergerAndParser.getAuditLogsParsed())
        .describedAs("Mismatch in the number of audit logs parsed")
        .isEqualTo(36 + 36);
    Assertions.assertThat(s3AAuditLogMergerAndParser.getReferrerHeaderLogParsed())
        .describedAs("Mismatch in the number of referrer headers parsed")
        .isEqualTo(36 + 36);
  }

  private Path tempAvroPath() throws IOException {
    File destFile = Files.createTempFile(getMethodName(), ".avro").toFile();
    return new Path(destFile.toURI());
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
    Path logsPath = new Path(sampleDir.toURI());
    Path destPath = tempAvroPath();
    boolean mergeAndParseResult =
        s3AAuditLogMergerAndParser.mergeAndParseAuditLogFiles(
            logsPath, destPath);
    Assertions.assertThat(mergeAndParseResult)
        .describedAs("Merge and parsing of the audit logs files was unsuccessful")
        .isTrue();
    Assertions.assertThat(s3AAuditLogMergerAndParser.getAuditLogsParsed())
        .describedAs("Mismatch in the number of audit logs parsed")
        .isEqualTo(3);
  }
}
