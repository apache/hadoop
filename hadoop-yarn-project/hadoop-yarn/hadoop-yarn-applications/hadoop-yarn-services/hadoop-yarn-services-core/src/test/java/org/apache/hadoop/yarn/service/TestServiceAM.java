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

package org.apache.hadoop.yarn.service;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingCluster;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.registry.client.api.RegistryConstants
    .KEY_REGISTRY_ZK_QUORUM;

public class TestServiceAM extends ServiceTestUtils{

  private File basedir;
  YarnConfiguration conf = new YarnConfiguration();
  TestingCluster zkCluster;

  @Before
  public void setup() throws Exception {
    basedir = new File("target", "apps");
    if (basedir.exists()) {
      FileUtils.deleteDirectory(basedir);
    } else {
      basedir.mkdirs();
    }
    zkCluster = new TestingCluster(1);
    zkCluster.start();
    conf.set(KEY_REGISTRY_ZK_QUORUM, zkCluster.getConnectString());
    System.out.println("ZK cluster: " +  zkCluster.getConnectString());
  }

  @After
  public void tearDown() throws IOException {
    if (basedir != null) {
      FileUtils.deleteDirectory(basedir);
    }
    if (zkCluster != null) {
      zkCluster.stop();
    }
  }

  // Race condition YARN-7486
  // 1. Allocate 1 container to compa and wait it to be started
  // 2. Fail this container, and in the meanwhile allocate the 2nd container.
  // 3. The 2nd container should not be assigned to compa-0 instance, because
  //   the compa-0 instance is not stopped yet.
  // 4. check compa still has the instance in the pending list.
  @Test
  public void testContainerCompleted() throws TimeoutException,
      InterruptedException {
    ApplicationId applicationId = ApplicationId.newInstance(123456, 1);
    Service exampleApp = new Service();
    exampleApp.setId(applicationId.toString());
    exampleApp.setName("testContainerCompleted");
    exampleApp.addComponent(createComponent("compa", 1, "pwd"));

    MockServiceAM am = new MockServiceAM(exampleApp);
    am.init(conf);
    am.start();

    ComponentInstance compa0 = am.getCompInstance("compa", "compa-0");
    // allocate a container
    am.feedContainerToComp(exampleApp, 1, "compa");
    am.waitForCompInstanceState(compa0, ComponentInstanceState.STARTED);

    System.out.println("Fail the container 1");
    // fail the container
    am.feedFailedContainerToComp(exampleApp, 1, "compa");

    // allocate the second container immediately, this container will not be
    // assigned to comp instance
    // because the instance is not yet added to the pending list.
    am.feedContainerToComp(exampleApp, 2, "compa");

    am.waitForCompInstanceState(compa0, ComponentInstanceState.INIT);
    // still 1 pending instance
    Assert.assertEquals(1,
        am.getComponent("compa").getPendingInstances().size());
    am.stop();
  }
}
