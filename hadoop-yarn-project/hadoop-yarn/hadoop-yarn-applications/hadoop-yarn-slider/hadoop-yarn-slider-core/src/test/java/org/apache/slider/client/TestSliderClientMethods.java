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

package org.apache.slider.client;

import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.server.appmaster.model.mock.MockApplicationId;
import org.apache.slider.utils.SliderTestBase;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Test slider client methods.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(SliderUtils.class)
public class TestSliderClientMethods extends SliderTestBase {
  protected static final Logger LOG =
      LoggerFactory.getLogger(TestSliderClientMethods.class);

  static final String AM_ENV = "LD_LIBRARY_PATH";
  static final String PLACEHOLDER_KEY = "${distro.version}";
  static final String PLACEHOLDER_SYSTEM_KEY = "DISTRO_VERSION";
  static final String PLACEHOLDER_VALUE = "1.0.0";
  static final String AM_ENV_2 = "PATH";
  static final String PLACEHOLDER_KEY_2 = "${native.version}";
  static final String PLACEHOLDER_SYSTEM_KEY_2 = "NATIVE_VERSION";
  static final String PLACEHOLDER_VALUE_2 = "2.0.0";

  @Test
  public void testGeneratePlaceholderKeyValueMap() throws Throwable {
    TestSliderClient testSliderClient = new TestSliderClient();

    PowerMock.mockStatic(System.class);
    EasyMock.expect(SliderUtils.getSystemEnv(PLACEHOLDER_SYSTEM_KEY))
      .andReturn(PLACEHOLDER_VALUE).anyTimes();
    PowerMock.replayAll();

    Map<String, String> placeholders = testSliderClient
        .generatePlaceholderKeyValueMap(AM_ENV + "=/usr/lib/" +
            PLACEHOLDER_KEY);
    Assert.assertTrue(placeholders.containsKey(PLACEHOLDER_KEY));
    Assert.assertEquals("Should be equal", PLACEHOLDER_VALUE,
        placeholders.get(PLACEHOLDER_KEY));

    PowerMock.verifyAll();
    LOG.info("Placeholders = {}", placeholders);
  }

  @Test
  public void testSetAmLaunchEnv() throws Throwable {
    TestSliderClient testSliderClient = new TestSliderClient();
    YarnConfiguration conf = SliderUtils.createConfiguration();
    conf.set(SliderXmlConfKeys.KEY_AM_LAUNCH_ENV, AM_ENV + "=/usr/lib/"
        + PLACEHOLDER_KEY);

    PowerMock.mockStatic(System.class);
    EasyMock.expect(SliderUtils.getSystemEnv(PLACEHOLDER_SYSTEM_KEY))
        .andReturn(PLACEHOLDER_VALUE);
    PowerMock.replayAll();

    Map<String, String> amLaunchEnv = testSliderClient.getAmLaunchEnv(conf);
    Assert.assertNotNull(amLaunchEnv);
    Assert.assertNotNull(amLaunchEnv.get(AM_ENV));
    Assert.assertEquals("Should be equal", amLaunchEnv.get(AM_ENV),
        (Shell.WINDOWS ? "%" + AM_ENV + "%;" : "$" + AM_ENV + ":") +
            "/usr/lib/" + PLACEHOLDER_VALUE);

    PowerMock.verifyAll();
    LOG.info("amLaunchEnv = {}", amLaunchEnv);
  }

  @Test
  public void testSetAmLaunchEnvMulti() throws Throwable {
    TestSliderClient testSliderClient = new TestSliderClient();
    YarnConfiguration conf = SliderUtils.createConfiguration();
    conf.set(SliderXmlConfKeys.KEY_AM_LAUNCH_ENV, AM_ENV + "=/usr/lib/"
        + PLACEHOLDER_KEY + "," + AM_ENV_2 + "=/usr/bin/" + PLACEHOLDER_KEY_2);

    PowerMock.mockStatic(System.class);
    EasyMock.expect(SliderUtils.getSystemEnv(PLACEHOLDER_SYSTEM_KEY))
        .andReturn(PLACEHOLDER_VALUE);
    EasyMock.expect(SliderUtils.getSystemEnv(PLACEHOLDER_SYSTEM_KEY_2))
        .andReturn(PLACEHOLDER_VALUE_2);
    PowerMock.replayAll();

    Map<String, String> amLaunchEnv = testSliderClient.getAmLaunchEnv(conf);
    Assert.assertNotNull(amLaunchEnv);
    Assert.assertEquals("Should have 2 envs", amLaunchEnv.size(), 2);
    Assert.assertNotNull(amLaunchEnv.get(AM_ENV));
    Assert.assertEquals("Should be equal", amLaunchEnv.get(AM_ENV),
        (Shell.WINDOWS ? "%" + AM_ENV + "%;" : "$" + AM_ENV + ":") +
            "/usr/lib/" + PLACEHOLDER_VALUE);
    Assert.assertNotNull(amLaunchEnv.get(AM_ENV_2));
    Assert.assertEquals("Should be equal", amLaunchEnv.get(AM_ENV_2),
        (Shell.WINDOWS ? "%" + AM_ENV_2 + "%;" : "$" + AM_ENV_2 + ":") +
            "/usr/bin/" + PLACEHOLDER_VALUE_2);

    PowerMock.verifyAll();
    LOG.info("amLaunchEnv = " + amLaunchEnv);
  }

  static class TestSliderClient extends SliderClient {
    @Override
    public ApplicationId submitApplication(ApplicationSubmissionContext
        context)
        throws YarnException, IOException {
      return new MockApplicationId(1);
    }
  }
}
