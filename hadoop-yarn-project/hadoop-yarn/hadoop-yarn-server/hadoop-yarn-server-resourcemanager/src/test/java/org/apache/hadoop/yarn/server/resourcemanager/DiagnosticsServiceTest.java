/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager;



import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CommonIssues;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FileContent;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.IssueData;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.IssueType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;


public class DiagnosticsServiceTest {
  private static final String ISSUE_NAME_APP_DIAGNOSTIC = "application_diagnostic";
  private static final String ISSUE_NAME_SCHED_ISSUE =
      "scheduler_related_issue";
  private static final String ISSUE_ARG_APP_ID = "appId";
  private static final String COLON = ":";
  private static final String COMMA = ",";
  private static final String OUTPUT_DIR = "/tmp";

  @Before
  public void setUp() {
    DiagnosticsService.setScriptLocation("src/test/resources/diagnostics" +
        "/diagnostics_collector_test.py");
    handleWindowsRuntime();
  }

  @Test
  public void testListCommonIssues()
      throws Exception {
    CommonIssues commonIssues = DiagnosticsService.listCommonIssues();

    assertEquals(2, commonIssues.getIssueList().size());
    assertIssueEquality(ISSUE_NAME_APP_DIAGNOSTIC,
        Collections.singletonList(ISSUE_ARG_APP_ID),
        commonIssues.getIssueList().get(0));

    assertIssueEquality(ISSUE_NAME_SCHED_ISSUE,
        Collections.emptyList(),
        commonIssues.getIssueList().get(1));
  }

  @Test(expected = IOException.class)
  public void testListCommonIssuesScriptMissing() throws Exception {
    DiagnosticsService.setScriptLocation("/src/invalidLocation/script.py");
    DiagnosticsService.listCommonIssues();
  }

  @Test
  public void testParseIssueTypeValidCases() {
    // valid case: name, no parameters
    String line = ISSUE_NAME_APP_DIAGNOSTIC;

    assertIssueEquality(ISSUE_NAME_APP_DIAGNOSTIC, Collections.emptyList(),
        DiagnosticsService.parseIssueType(line));

    // valid case: name, one parameter
    line = ISSUE_NAME_APP_DIAGNOSTIC + COLON + ISSUE_ARG_APP_ID;

    assertIssueEquality(ISSUE_NAME_APP_DIAGNOSTIC,
        Collections.singletonList(ISSUE_ARG_APP_ID),
        DiagnosticsService.parseIssueType(line));
  }

  @Test
  public void testParseIssueTypeInvalidCases() {
    // invalid case: too many values
    String line = ISSUE_NAME_APP_DIAGNOSTIC + COLON + ISSUE_NAME_APP_DIAGNOSTIC +
        COLON + ISSUE_NAME_APP_DIAGNOSTIC;

    IssueType issueType = DiagnosticsService.parseIssueType(line);
    assertNull(issueType);
  }

  @Test
  public void testCollectIssueDataInvalidCases() throws Exception {
  }

  @Test
  public void testCollectIssueFilesContentNestedDirectories() throws Exception {
    Path root = Files.createTempDirectory("tmp");
    Path applicationDiagnostic = root.resolve("application_diagnostic");
    Files.createDirectories(applicationDiagnostic);
    Path application_info = Files.write(
            applicationDiagnostic.resolve("application_info.txt"),
            "application_1740465819367_0009".getBytes(StandardCharsets.UTF_8)
    );

    IssueData data = DiagnosticsService.collectIssueFilesContent(root.toFile());
    List<FileContent> files = data.getFiles();

    assertEquals(1, files.size());
    assertEquals("application_info.txt", files.get(0).getFilename());
    assertEquals("application_1740465819367_0009", files.get(0).getContent());
  }

  @Test
  public void testCollectIssueFilesContentMissingDir() throws Exception {

  }

  @Test
  public void testCreateProcessBuilderWithArguments() throws Exception {

  }

  private void assertIssueEquality(String expectedIssueName,
                                   List<String> expectedParams,
                                   IssueType actualIssue) {
    assertEquals(expectedIssueName,
        actualIssue.getName());
    assertEquals(expectedParams.size(),
        actualIssue.getParameters().size());
    Assert.assertTrue(CollectionUtils.isEqualCollection(
        expectedParams, actualIssue.getParameters()));
  }

  private void handleWindowsRuntime() {
    if (Shell.WINDOWS) {
      try {
        DiagnosticsService.listCommonIssues();
        fail("On Windows listCommonIssues should throw " +
            "UnsupportedOperationException");
      } catch (Exception e) {
        // Exception is expected
      }
    }
  }
}