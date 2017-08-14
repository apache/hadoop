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

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.service.client.params.Arguments;
import org.apache.hadoop.yarn.service.client.params.ClientArgs;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.main.ServiceLauncher;
import org.apache.slider.utils.SliderTestBase;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * Test bad argument handling.
 */
public class TestClientBasicArgs extends SliderTestBase {

  /**
   * Help should print out help string and then succeed.
   * @throws Throwable
   */
  //@Test
  public void testHelp() throws Throwable {
    ServiceLauncher launcher = launch(SliderClient.class,
                                      SliderUtils.createConfiguration(),
                                      Arrays.asList(ClientArgs.ACTION_HELP));
    assertEquals(0, launcher.getServiceExitCode());
  }

  //@Test
  public void testNoArgs() throws Throwable {
    launchExpectingException(SliderClient.class,
                                        SliderUtils.createConfiguration(),
                                        "Usage: slider COMMAND",
                                        EMPTY_LIST);
  }

  //@Test
  public void testListUnknownRM() throws Throwable {
    try {
      YarnConfiguration conf = SliderUtils.createConfiguration();
      conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,
          1000);
      conf.setLong(YarnConfiguration
          .RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, 1000);
      ServiceLauncher launcher = launch(SliderClient.class,
                                        conf,
                                        Arrays.asList(
                                        ClientArgs.ACTION_LIST,
                                        "cluster",
                                        Arguments.ARG_MANAGER,
                                        "badhost:8888"));
      fail("expected an exception, got a launcher with exit code " +
          launcher.getServiceExitCode());
    } catch (UnknownHostException expected) {
      //expected
    }

  }


}
