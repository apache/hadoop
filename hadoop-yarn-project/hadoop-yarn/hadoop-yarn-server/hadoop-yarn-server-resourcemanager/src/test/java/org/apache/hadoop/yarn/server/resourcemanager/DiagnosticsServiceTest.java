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
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.IssueType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.fail;


public class DiagnosticsServiceTest {
  private static final String ISSUE_NAME_APP_FAILED = "application_failed";
  private static final String ISSUE_NAME_APP_FAILED_NECESSARY_ARGS =
      "application_failed_necessary_args";
  private static final String ISSUE_NAME_APP_HANGING = "application_hanging";
  private static final String ISSUE_NAME_SCHED_ISSUE =
      "scheduler_related_issue";
  private static final String ISSUE_NAME_RM_NM_ISSUE = "rm_nm_start_failure";
  private static final String ISSUE_ARG_APP_ID = "appId";
  private static final String ISSUE_ARG_NODE_ID = "nodeId";
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
  public void testListCommonIssuesValidCaseWithOptionsToBeSkipped()
      throws Exception {
    // The test script contains two invalid options: one with an ambiguous name
    // and one with too many parameters. These should be skipped silently.
    CommonIssues commonIssues = DiagnosticsService.listCommonIssues();

    Assert.assertEquals(4, commonIssues.getIssueList().size());
    assertIssueEquality(ISSUE_NAME_APP_FAILED,
        Collections.singletonList(ISSUE_ARG_APP_ID),
        commonIssues.getIssueList().get(0));

    assertIssueEquality(ISSUE_NAME_APP_HANGING,
        Arrays.asList(ISSUE_ARG_APP_ID, ISSUE_ARG_NODE_ID),
        commonIssues.getIssueList().get(1));

    assertIssueEquality(ISSUE_NAME_SCHED_ISSUE,
        Collections.emptyList(),
        commonIssues.getIssueList().get(2));

    assertIssueEquality(ISSUE_NAME_RM_NM_ISSUE,
        Collections.singletonList(ISSUE_ARG_NODE_ID),
        commonIssues.getIssueList().get(3));
  }

  @Test(expected = IOException.class)
  public void testListCommonIssuesScriptMissing() throws Exception {
    DiagnosticsService.setScriptLocation("/src/invalidLocation/script.py");
    DiagnosticsService.listCommonIssues();
  }

//  @Test
//  public void testCollectIssueDataPathValidOutput() throws Exception {
//    // valid case: the script prints out one directory
//    Assert.assertEquals(OUTPUT_DIR, DiagnosticsService.collectIssueDataPath(
//        ISSUE_NAME_APP_FAILED, null));
//  }
//
//  @Test
//  public void testCollectIssueDataPathValidOutputWhenArgsArePresent()
//      throws Exception {
//    // valid case: appId and nodeId are necessary params and they are present
//    Assert.assertEquals(OUTPUT_DIR, DiagnosticsService.collectIssueDataPath(
//        ISSUE_NAME_APP_FAILED_NECESSARY_ARGS,
//        Arrays.asList(ISSUE_ARG_APP_ID, ISSUE_ARG_NODE_ID)));
//  }
//
//  @Test(expected = IOException.class)
//  public void testCollectIssueDataPathInvalidOutputWhenWrongArgsArePresent()
//      throws Exception {
//    // valid case: appId and nodeId are necessary params but two appIds are
//    // given
//    Assert.assertEquals(OUTPUT_DIR, DiagnosticsService.collectIssueDataPath(
//        ISSUE_NAME_APP_FAILED_NECESSARY_ARGS,
//        Arrays.asList(ISSUE_ARG_APP_ID, ISSUE_ARG_APP_ID)));
//  }
//
//  @Test(expected = IOException.class)
//  public void testCollectIssueDataPathInvalidOutputEmptyDir() throws Exception {
//    // invalid case: the script prints out an empty string as directory
//    // with the correct prefix
//    DiagnosticsService.collectIssueDataPath(ISSUE_NAME_APP_HANGING, null);
//  }
//
//  @Test(expected = IOException.class)
//  public void testCollectIssueDataPathInvalidOutputMissingOutputDir()
//      throws Exception {
//    // invalid case: the script doesn't print out the correct output directory
//    DiagnosticsService.collectIssueDataPath(ISSUE_NAME_SCHED_ISSUE, null);
//  }
//
//  @Test(expected = IOException.class)
//  public void testCollectIssueDataPathInvalidOutputMissingPrints()
//      throws Exception {
//    // invalid case: the script doesn't print out anything
//    DiagnosticsService.collectIssueDataPath(ISSUE_NAME_RM_NM_ISSUE, null);
//  }
//
//  @Test(expected = IOException.class)
//  public void testCollectIssueDataPathScriptMissing() throws Exception {
//    DiagnosticsService.setScriptLocation("/src/invalidLocation/script.py");
//    DiagnosticsService.collectIssueDataPath(ISSUE_NAME_APP_FAILED, null);
//  }

  @Test
  public void testParseIssueTypeValidCases() {
    // valid case: name, no parameters
    String line = ISSUE_NAME_APP_FAILED;

    assertIssueEquality(ISSUE_NAME_APP_FAILED, Collections.emptyList(),
        DiagnosticsService.parseIssueType(line));

    // valid case: name, one parameter
    line = ISSUE_NAME_APP_FAILED + COLON + ISSUE_ARG_APP_ID;

    assertIssueEquality(ISSUE_NAME_APP_FAILED,
        Collections.singletonList(ISSUE_ARG_APP_ID),
        DiagnosticsService.parseIssueType(line));

    // valid case: name, two parameters
    line = ISSUE_NAME_APP_FAILED + COLON + ISSUE_ARG_APP_ID +
        COMMA + ISSUE_ARG_NODE_ID;

    assertIssueEquality(ISSUE_NAME_APP_FAILED,
        Arrays.asList(ISSUE_ARG_APP_ID, ISSUE_ARG_NODE_ID),
        DiagnosticsService.parseIssueType(line));
  }

  @Test
  public void testParseIssueTypeInvalidCases() {
    // invalid case: too many values
    String line = ISSUE_NAME_APP_FAILED + COLON + ISSUE_NAME_APP_FAILED +
        COLON + ISSUE_NAME_APP_FAILED;

    IssueType issueType = DiagnosticsService.parseIssueType(line);
    Assert.assertNull(issueType);
  }

  private void assertIssueEquality(String expectedIssueName,
                                   List<String> expectedParams,
                                   IssueType actualIssue) {
    Assert.assertEquals(expectedIssueName,
        actualIssue.getName());
    Assert.assertEquals(expectedParams.size(),
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