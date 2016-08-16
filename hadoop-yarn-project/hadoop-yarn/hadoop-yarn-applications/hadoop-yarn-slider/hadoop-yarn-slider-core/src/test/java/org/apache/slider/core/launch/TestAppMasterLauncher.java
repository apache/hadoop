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

package org.apache.slider.core.launch;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.client.SliderYarnClientImpl;
import org.apache.slider.common.SliderKeys;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestAppMasterLauncher {
  SliderYarnClientImpl mockYarnClient;
  YarnClientApplication yarnClientApp;
  ApplicationSubmissionContext appSubmissionContext;
  Set<String> tags = Collections.emptySet();
  AppMasterLauncher appMasterLauncher = null;
  boolean isOldApi = true;
  Method rolledLogsIncludeMethod = null;
  Method rolledLogsExcludeMethod = null;

  @Before
  public void initialize() throws Exception {
    mockYarnClient = EasyMock.createNiceMock(SliderYarnClientImpl.class);
    yarnClientApp = EasyMock.createNiceMock(YarnClientApplication.class);
    appSubmissionContext = EasyMock
        .createNiceMock(ApplicationSubmissionContext.class);
    EasyMock.expect(yarnClientApp.getApplicationSubmissionContext())
        .andReturn(appSubmissionContext).once();
    EasyMock.expect(mockYarnClient.createApplication())
        .andReturn(yarnClientApp).once();

    try {
      LogAggregationContext.class.getMethod("newInstance", String.class,
          String.class, String.class, String.class);
      isOldApi = false;
      rolledLogsIncludeMethod = LogAggregationContext.class
          .getMethod("getRolledLogsIncludePattern");
      rolledLogsExcludeMethod = LogAggregationContext.class
          .getMethod("getRolledLogsExcludePattern");
    } catch (Exception e) {
      isOldApi = true;
    }
  }

  /**
   * These tests will probably fail when compiled against hadoop 2.7+. Please
   * refer to SLIDER-810. It has been purposely not modified so that it fails
   * and that someone needs to modify the code in
   * {@code AbstractLauncher#extractLogAggregationContext(Map)}. Comments are
   * provided in that method as to what needs to be done.
   *
   * @throws Exception
   */
  @Test
  public void testExtractLogAggregationContext() throws Exception {
    Map<String, String> options = new HashMap<String, String>();
    options.put(ResourceKeys.YARN_LOG_INCLUDE_PATTERNS,
        " | slider*.txt  |agent.out| |");
    options.put(ResourceKeys.YARN_LOG_EXCLUDE_PATTERNS,
        "command*.json|  agent.log*        |     ");

    EasyMock.replay(mockYarnClient, appSubmissionContext, yarnClientApp);
    appMasterLauncher = new AppMasterLauncher("cl1", SliderKeys.APP_TYPE, null,
        null, mockYarnClient, false, null, options, tags, null);

    // Verify the include/exclude patterns
    String expectedInclude = "slider*.txt|agent.out";
    String expectedExclude = "command*.json|agent.log*";
    assertPatterns(expectedInclude, expectedExclude);

    EasyMock.verify(mockYarnClient, appSubmissionContext, yarnClientApp);

  }

  @Test
  public void testExtractLogAggregationContextEmptyIncludePattern()
      throws Exception {
    Map<String, String> options = new HashMap<String, String>();
    options.put(ResourceKeys.YARN_LOG_INCLUDE_PATTERNS, " ");
    options.put(ResourceKeys.YARN_LOG_EXCLUDE_PATTERNS,
        "command*.json|  agent.log*        |     ");

    EasyMock.replay(mockYarnClient, appSubmissionContext, yarnClientApp);
    appMasterLauncher = new AppMasterLauncher("cl1", SliderKeys.APP_TYPE, null,
        null, mockYarnClient, false, null, options, tags, null);

    // Verify the include/exclude patterns
    String expectedInclude = isOldApi ? "" : ".*";
    String expectedExclude = "command*.json|agent.log*";
    assertPatterns(expectedInclude, expectedExclude);

    EasyMock.verify(mockYarnClient, appSubmissionContext, yarnClientApp);
  }

  @Test
  public void testExtractLogAggregationContextEmptyIncludeAndExcludePattern()
      throws Exception {
    Map<String, String> options = new HashMap<String, String>();
    options.put(ResourceKeys.YARN_LOG_INCLUDE_PATTERNS, "");
    options.put(ResourceKeys.YARN_LOG_EXCLUDE_PATTERNS, "  ");

    EasyMock.replay(mockYarnClient, appSubmissionContext, yarnClientApp);
    appMasterLauncher = new AppMasterLauncher("cl1", SliderKeys.APP_TYPE, null,
        null, mockYarnClient, false, null, options, tags, null);

    // Verify the include/exclude patterns
    String expectedInclude = isOldApi ? "" : ".*";
    String expectedExclude = "";
    assertPatterns(expectedInclude, expectedExclude);

    EasyMock.verify(mockYarnClient, appSubmissionContext, yarnClientApp);
  }

  private void assertPatterns(String expectedIncludePattern,
      String expectedExcludePattern) throws Exception {
    if (isOldApi) {
      Assert.assertEquals(expectedIncludePattern,
          appMasterLauncher.logAggregationContext.getIncludePattern());
      Assert.assertEquals(expectedExcludePattern,
          appMasterLauncher.logAggregationContext.getExcludePattern());
    } else {
      Assert.assertEquals(expectedIncludePattern,
          (String) rolledLogsIncludeMethod
              .invoke(appMasterLauncher.logAggregationContext));
      Assert.assertEquals(expectedExcludePattern,
          (String) rolledLogsExcludeMethod
              .invoke(appMasterLauncher.logAggregationContext));
    }
  }
}
