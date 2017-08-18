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


package org.apache.hadoop.yarn.service.servicemonitor;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.service.MockServiceAM;
import org.apache.hadoop.yarn.service.ServiceTestUtils;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Component;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.anyFloat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class TestServiceMonitor extends ServiceTestUtils {

  private File basedir;
  YarnConfiguration conf = new YarnConfiguration();

  @Before
  public void setup() throws Exception {
    basedir = new File("target", "apps");
    if (basedir.exists()) {
      FileUtils.deleteDirectory(basedir);
    } else {
      basedir.mkdirs();
    }
    conf.setLong(InternalKeys.MONITOR_INTERVAL, 2);
  }

  @After
  public void tearDown() throws IOException {
    if (basedir != null) {
      FileUtils.deleteDirectory(basedir);
    }
  }

  // Create compa with 1 container
  // Create compb with 1 container
  // Verify compb dependency satisfied
  // Increase compa to 2 containers
  // Verify compb dependency becomes unsatisfied.
  @Test
  public void testComponentDependency() throws Exception{
    ApplicationId applicationId = ApplicationId.newInstance(123456, 1);
    Application exampleApp = new Application();
    exampleApp.setId(applicationId.toString());
    exampleApp.setName("testComponentDependency");
    exampleApp.addComponent(createComponent("compa", 1, "sleep 1000"));
    Component compb = createComponent("compb", 1, "sleep 1000");

    // Let compb depends on compa;
    compb.setDependencies(Collections.singletonList("compa"));
    exampleApp.addComponent(compb);

    MockServiceAM am = new MockServiceAM(exampleApp);
    am.init(conf);
    am.start();

    // compa ready
    Assert.assertTrue(am.getComponent("compa").areDependenciesReady());
    //compb not ready
    Assert.assertFalse(am.getComponent("compb").areDependenciesReady());

    // feed 1 container to compa,
    am.feedContainerToComp(exampleApp, 1, "compa");
    // waiting for compb's dependencies are satisfied
    am.waitForDependenciesSatisfied("compb");

    // feed 1 container to compb
    am.feedContainerToComp(exampleApp, 2, "compb");
    am.flexComponent("compa", 2);
    am.waitForNumDesiredContainers("compa", 2);

    // compb dependencies not satisfied again.
    Assert.assertFalse(am.getComponent("compb").areDependenciesReady());
    am.stop();
  }
}
