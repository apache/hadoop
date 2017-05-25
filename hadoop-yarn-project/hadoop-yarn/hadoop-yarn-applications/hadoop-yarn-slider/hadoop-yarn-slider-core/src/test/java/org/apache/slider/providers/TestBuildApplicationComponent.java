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

import org.apache.slider.api.resource.Component;
import org.apache.slider.client.SliderClient;
import org.apache.slider.common.params.SliderActions;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.conf.ExampleAppJson;
import org.apache.slider.core.main.ServiceLauncher;
import org.apache.slider.util.ServiceApiUtil;
import org.apache.slider.utils.YarnZKMiniClusterTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.slider.common.params.Arguments.ARG_APPDEF;

/**
 * Test for building / resolving components of type APPLICATION.
 */
public class TestBuildApplicationComponent extends YarnZKMiniClusterTestBase {

  private static void checkComponentNames(List<Component> components,
      Set<String> names) {
    assertEquals(names.size(), components.size());
    for (Component comp : components) {
      assertTrue(names.contains(comp.getName()));
    }
  }

  public void buildAndCheckComponents(String appName, String appDef,
      SliderFileSystem sfs, Set<String> names) throws Throwable {
    ServiceLauncher<SliderClient> launcher = createOrBuildCluster(
        SliderActions.ACTION_BUILD, appName, Arrays.asList(ARG_APPDEF,
            ExampleAppJson.resourceName(appDef)), true, false);
    SliderClient sliderClient = launcher.getService();
    addToTeardown(sliderClient);

    // verify the cluster exists
    assertEquals(0, sliderClient.actionExists(appName, false));
    // verify generated conf
    List<Component> components = ServiceApiUtil.getApplicationComponents(sfs,
        appName);
    checkComponentNames(components, names);
  }

  @Test
  public void testExternalComponentBuild() throws Throwable {
    String clustername = createMiniCluster("", getConfiguration(), 1, true);

    describe("verify external components");

    SliderFileSystem sfs = createSliderFileSystem();

    Set<String> nameSet = new HashSet<>();
    nameSet.add("simple");
    nameSet.add("master");
    nameSet.add("worker");

    buildAndCheckComponents("app-1", ExampleAppJson.APP_JSON, sfs,
        nameSet);
    buildAndCheckComponents("external-0", ExampleAppJson
            .EXTERNAL_JSON_0, sfs, nameSet);

    nameSet.add("other");

    buildAndCheckComponents("external-1", ExampleAppJson
        .EXTERNAL_JSON_1, sfs, nameSet);

    nameSet.add("another");

    buildAndCheckComponents("external-2", ExampleAppJson
        .EXTERNAL_JSON_2, sfs, nameSet);

  }

}
