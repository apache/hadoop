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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CommonIssues;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FileContent;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.IssueData;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.IssueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DiagnosticsServiceTest {
  private static final String ISSUE_NAME_APP_DIAGNOSTIC =
      "application_diagnostic";
  private static final String ISSUE_NAME_SCHED_ISSUE =
      "scheduler_related_issue";
  private static final String ISSUE_ARG_APP_ID = "appId";
  private static final String COLON = ":";

  /**
   * Stand-in for the real collector script, which is delivered separately.
   * It lists two issue types for "-l". For "-c" it reports the directory
   * passed via "-a" as OUTPUT_DIR, or only an unprefixed path otherwise.
   */
  private static final String STUB_SCRIPT = String.join("\n",
      "import sys",
      "if sys.argv[1] == '-l':",
      "    print('" + ISSUE_NAME_APP_DIAGNOSTIC + COLON + ISSUE_ARG_APP_ID
          + "')",
      "    print('" + ISSUE_NAME_SCHED_ISSUE + "')",
      "elif len(sys.argv) > 4:",
      "    print('collecting into ' + sys.argv[4])",
      "    print('OUTPUT_DIR:' + sys.argv[4])",
      "else:",
      "    print('/tmp/not_an_output_dir')",
      "");

  @TempDir
  private Path tempDir;

  @BeforeEach
  public void setUp() throws IOException {
    Path script = tempDir.resolve("diagnostics_collector_stub.py");
    Files.write(script, STUB_SCRIPT.getBytes(StandardCharsets.UTF_8));
    DiagnosticsService.setScriptLocation(script.toString());
  }

  @AfterEach
  public void tearDown() {
    DiagnosticsService.setScriptLocation(null);
  }

  @Test
  public void testListCommonIssues() throws Exception {
    assumeFalse(Shell.WINDOWS);
    CommonIssues commonIssues = DiagnosticsService.listCommonIssues();

    assertEquals(2, commonIssues.getIssueList().size());
    assertIssueEquality(ISSUE_NAME_APP_DIAGNOSTIC,
        Collections.singletonList(ISSUE_ARG_APP_ID),
        commonIssues.getIssueList().get(0));
    assertIssueEquality(ISSUE_NAME_SCHED_ISSUE,
        Collections.emptyList(),
        commonIssues.getIssueList().get(1));
  }

  @Test
  public void testListCommonIssuesScriptMissing() {
    assumeFalse(Shell.WINDOWS);
    DiagnosticsService.setScriptLocation(
        tempDir.resolve("missing.py").toString());

    assertThrows(IOException.class, DiagnosticsService::listCommonIssues);
  }

  @Test
  public void testListCommonIssuesUnsupportedOnWindows() {
    assumeTrue(Shell.WINDOWS);

    assertThrows(UnsupportedOperationException.class,
        DiagnosticsService::listCommonIssues);
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
    String line = ISSUE_NAME_APP_DIAGNOSTIC + COLON + ISSUE_NAME_APP_DIAGNOSTIC
        + COLON + ISSUE_NAME_APP_DIAGNOSTIC;

    assertNull(DiagnosticsService.parseIssueType(line));
  }

  @Test
  public void testCollectIssueDataNoOutputDirectory() {
    assumeFalse(Shell.WINDOWS);

    assertThrows(IOException.class,
        () -> DiagnosticsService.collectIssueData(ISSUE_NAME_SCHED_ISSUE,
            null));
  }

  @Test
  public void testCollectIssueDataReadsOutputDir() throws Exception {
    assumeFalse(Shell.WINDOWS);
    Path outputDir = tempDir.resolve("output");
    Files.createDirectories(outputDir);
    Files.write(outputDir.resolve("scheduler_info.txt"),
        "queue: root.default".getBytes(StandardCharsets.UTF_8));

    IssueData data = DiagnosticsService.collectIssueData(
        ISSUE_NAME_SCHED_ISSUE,
        Collections.singletonList(outputDir.toString()));
    List<FileContent> files = data.getFiles();

    assertEquals(1, files.size());
    assertEquals("scheduler_info.txt", files.get(0).getFilename());
    assertEquals("queue: root.default", files.get(0).getContent());
  }

  @Test
  public void testCollectIssueFilesContentNestedDirectories()
      throws Exception {
    Path root = tempDir.resolve("output");
    Path applicationDiagnostic = root.resolve(ISSUE_NAME_APP_DIAGNOSTIC);
    Files.createDirectories(applicationDiagnostic);
    Files.write(applicationDiagnostic.resolve("application_info.txt"),
        "application_1740465819367_0009".getBytes(StandardCharsets.UTF_8));

    IssueData data = DiagnosticsService.collectIssueFilesContent(
        root.toFile());
    List<FileContent> files = data.getFiles();

    assertEquals(1, files.size());
    assertEquals("application_info.txt", files.get(0).getFilename());
    assertEquals("application_1740465819367_0009",
        files.get(0).getContent());
  }

  @Test
  public void testCollectIssueFilesContentMissingDir() {
    IssueData data = DiagnosticsService.collectIssueFilesContent(
        tempDir.resolve("does_not_exist").toFile());

    assertTrue(data.getFiles().isEmpty());
  }

  @Test
  public void testCreateProcessBuilderWithArguments() throws Exception {
    List<String> args = Arrays.asList("application_1_0001", "extra");
    ProcessBuilder pb = DiagnosticsService.createProcessBuilder(
        DiagnosticsService.CommandArgument.COMMAND,
        ISSUE_NAME_APP_DIAGNOSTIC, args);

    List<String> command = pb.command();
    assertEquals(Arrays.asList("-c", ISSUE_NAME_APP_DIAGNOSTIC, "-a",
        "application_1_0001", "extra"), command.subList(2, command.size()));
  }

  @Test
  public void testCreateProcessBuilderListIssues() throws Exception {
    ProcessBuilder pb = DiagnosticsService.createProcessBuilder(
        DiagnosticsService.CommandArgument.LIST_ISSUES);

    List<String> command = pb.command();
    assertEquals(Collections.singletonList("-l"),
        command.subList(2, command.size()));
  }

  private void assertIssueEquality(String expectedIssueName,
      List<String> expectedParams, IssueType actualIssue) {
    assertEquals(expectedIssueName, actualIssue.getName());
    assertEquals(expectedParams, actualIssue.getParameters());
  }
}
