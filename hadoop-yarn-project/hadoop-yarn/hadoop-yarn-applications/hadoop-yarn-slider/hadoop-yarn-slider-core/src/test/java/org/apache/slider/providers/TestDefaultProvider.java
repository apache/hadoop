/*
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
package org.apache.slider.providers;

import org.apache.slider.api.resource.Application;
import org.apache.slider.client.SliderClient;
import org.apache.slider.common.params.SliderActions;
import org.apache.slider.core.conf.ExampleAppJson;
import org.apache.slider.core.main.ServiceLauncher;
import org.apache.slider.utils.YarnZKMiniClusterTestBase;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.slider.common.params.Arguments.ARG_APPDEF;

/**
 * Simple end-to-end test.
 */
public class TestDefaultProvider  extends YarnZKMiniClusterTestBase {

  // TODO figure out how to run client commands against minicluster
  // (currently errors out unable to find containing jar of AM for upload)
  @Ignore
  @Test
  public void testDefaultProvider() throws Throwable {
    createMiniCluster("", getConfiguration(), 1, true);
    String appName = "default-1";

    describe("verify default provider");

    String appDef = ExampleAppJson.resourceName(ExampleAppJson
        .DEFAULT_JSON);

    ServiceLauncher<SliderClient> launcher = createOrBuildCluster(
        SliderActions.ACTION_CREATE, appName, Arrays.asList(ARG_APPDEF,
            appDef), true, true);
    SliderClient sliderClient = launcher.getService();
    addToTeardown(sliderClient);

    Application application = sliderClient.actionStatus(appName);
    assertEquals(1L, application.getContainers().size());
  }
}
