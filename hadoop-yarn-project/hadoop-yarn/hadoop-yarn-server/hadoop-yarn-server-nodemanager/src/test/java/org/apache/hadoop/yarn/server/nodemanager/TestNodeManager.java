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

package org.apache.hadoop.yarn.server.nodemanager;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeLabelsProvider;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestNodeManager {

  public static final class InvalidContainerExecutor extends
      DefaultContainerExecutor {
    @Override
    public void init(Context nmContext) throws IOException {
      throw new IOException("dummy executor init called");
    }
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testContainerExecutorInitCall() {
    NodeManager nm = new NodeManager();
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.NM_CONTAINER_EXECUTOR,
        InvalidContainerExecutor.class,
        ContainerExecutor.class);
    try {
      nm.init(conf);
      fail("Init should fail");
    } catch (YarnRuntimeException e) {
      //PASS
      assert(e.getCause().getMessage().contains("dummy executor init called"));
    } finally {
      nm.stop();
    }
  }

  private static int initCalls = 0;
  private static int preCalls = 0;
  private static int postCalls = 0;

  private static class DummyCSTListener1
      implements ContainerStateTransitionListener {
    @Override
    public void init(Context context) {
      initCalls++;
    }

    @Override
    public void preTransition(ContainerImpl op, ContainerState beforeState,
        ContainerEvent eventToBeProcessed) {
      preCalls++;
    }

    @Override
    public void postTransition(ContainerImpl op, ContainerState beforeState,
        ContainerState afterState, ContainerEvent processedEvent) {
      postCalls++;
    }
  }

  private static class DummyCSTListener2
      implements ContainerStateTransitionListener {
    @Override
    public void init(Context context) {
      initCalls++;
    }

    @Override
    public void preTransition(ContainerImpl op, ContainerState beforeState,
        ContainerEvent eventToBeProcessed) {
      preCalls++;
    }

    @Override
    public void postTransition(ContainerImpl op, ContainerState beforeState,
        ContainerState afterState, ContainerEvent processedEvent) {
      postCalls++;
    }
  }

  @Test
  public void testListenerInitialization() throws Exception{
    NodeManager nodeManager = new NodeManager();
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_CONTAINER_STATE_TRANSITION_LISTENERS,
        DummyCSTListener1.class.getName() + ","
            + DummyCSTListener2.class.getName());
    initCalls = 0;
    preCalls = 0;
    postCalls = 0;
    NodeManager.NMContext nmContext =
        nodeManager.createNMContext(null, null, null, false, conf);
    Assert.assertEquals(2, initCalls);
    nmContext.getContainerStateTransitionListener().preTransition(
        null, null, null);
    nmContext.getContainerStateTransitionListener().postTransition(
        null, null, null, null);
    Assert.assertEquals(2, preCalls);
    Assert.assertEquals(2, postCalls);
  }

  @Test
  public void testCreationOfNodeLabelsProviderService()
      throws InterruptedException {
    try {
      NodeManager nodeManager = new NodeManager();
      Configuration conf = new Configuration();
      NodeLabelsProvider labelsProviderService =
          nodeManager.createNodeLabelsProvider(conf);
      Assert
          .assertNull(
              "LabelsProviderService should not be initialized in default configuration",
              labelsProviderService);

      // With valid className
      conf.set(
          YarnConfiguration.NM_NODE_LABELS_PROVIDER_CONFIG,
          "org.apache.hadoop.yarn.server.nodemanager.nodelabels.ConfigurationNodeLabelsProvider");
      labelsProviderService = nodeManager.createNodeLabelsProvider(conf);
      Assert.assertNotNull("LabelsProviderService should be initialized When "
          + "node labels provider class is configured", labelsProviderService);

      // With invalid className
      conf.set(YarnConfiguration.NM_NODE_LABELS_PROVIDER_CONFIG,
          "org.apache.hadoop.yarn.server.nodemanager.NodeManager");
      try {
        labelsProviderService = nodeManager.createNodeLabelsProvider(conf);
        Assert.fail("Expected to throw IOException on Invalid configuration");
      } catch (IOException e) {
        // exception expected on invalid configuration
      }
      Assert.assertNotNull("LabelsProviderService should be initialized When "
          + "node labels provider class is configured", labelsProviderService);

      // With valid whitelisted configurations
      conf.set(YarnConfiguration.NM_NODE_LABELS_PROVIDER_CONFIG,
          YarnConfiguration.CONFIG_NODE_LABELS_PROVIDER);
      labelsProviderService = nodeManager.createNodeLabelsProvider(conf);
      Assert.assertNotNull("LabelsProviderService should be initialized When "
          + "node labels provider class is configured", labelsProviderService);

    } catch (Exception e) {
      Assert.fail("Exception caught");
      e.printStackTrace();
    }
  }

  /**
   * Test whether NodeManager passes user-provided conf to
   * UserGroupInformation class. If it reads this (incorrect)
   * AuthenticationMethod enum an exception is thrown.
   */
  @Test
  public void testUserProvidedUGIConf() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid attribute value for "
        + CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION
        + " of DUMMYAUTH");
    Configuration dummyConf = new YarnConfiguration();
    dummyConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "DUMMYAUTH");
    NodeManager dummyNodeManager = new NodeManager();
    try {
      dummyNodeManager.init(dummyConf);
    } finally {
      dummyNodeManager.stop();
    }
  }
}
