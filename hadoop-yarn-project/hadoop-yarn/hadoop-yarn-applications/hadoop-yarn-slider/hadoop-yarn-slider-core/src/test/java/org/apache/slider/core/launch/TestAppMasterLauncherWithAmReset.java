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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.util.Records;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.client.SliderYarnClientImpl;
import org.apache.slider.common.SliderKeys;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestAppMasterLauncherWithAmReset {
  SliderYarnClientImpl mockYarnClient;
  YarnClientApplication yarnClientApp;
  ApplicationSubmissionContext appSubmissionContext;
  GetNewApplicationResponse newApp;
  Set<String> tags = Collections.emptySet();
  AppMasterLauncher appMasterLauncher = null;
  boolean isOldApi = true;

  @Before
  public void initialize() throws Exception {
    mockYarnClient = EasyMock.createNiceMock(SliderYarnClientImpl.class);
    yarnClientApp = EasyMock.createNiceMock(YarnClientApplication.class);
    newApp = EasyMock.createNiceMock(GetNewApplicationResponse.class);
    EasyMock.expect(mockYarnClient.createApplication())
        .andReturn(new YarnClientApplication(newApp,
        Records.newRecord(ApplicationSubmissionContext.class)));
  }

  @Test
  public void testExtractYarnResourceManagerAmRetryCountWindowMs() throws
      Exception {
    Map<String, String> options = new HashMap<String, String>();
    final String expectedInterval = Integer.toString (120000);
    options.put(ResourceKeys.YARN_RESOURCEMANAGER_AM_RETRY_COUNT_WINDOW_MS,
        expectedInterval);
    EasyMock.replay(mockYarnClient, yarnClientApp);

    appMasterLauncher = new AppMasterLauncher("am1", SliderKeys.APP_TYPE, null,
        null, mockYarnClient, false, null, options, tags, null);

    ApplicationSubmissionContext ctx = appMasterLauncher.application
        .getApplicationSubmissionContext();
    String retryIntervalWindow = Long.toString(ctx
        .getAttemptFailuresValidityInterval());
    Assert.assertEquals(expectedInterval, retryIntervalWindow);
  }

  @Test
  public void testExtractYarnResourceManagerAmRetryCountWindowMsDefaultValue()
      throws Exception {
    Map<String, String> options = new HashMap<String, String>();
    EasyMock.replay(mockYarnClient, yarnClientApp);

    appMasterLauncher = new AppMasterLauncher("am1", SliderKeys.APP_TYPE, null,
        null, mockYarnClient, false, null, options, tags, null);

    ApplicationSubmissionContext ctx = appMasterLauncher.application
        .getApplicationSubmissionContext();
    long retryIntervalWindow = ctx.getAttemptFailuresValidityInterval();
    Assert.assertEquals(ResourceKeys.DEFAULT_AM_RETRY_COUNT_WINDOW_MS,
        retryIntervalWindow);
  }

}
