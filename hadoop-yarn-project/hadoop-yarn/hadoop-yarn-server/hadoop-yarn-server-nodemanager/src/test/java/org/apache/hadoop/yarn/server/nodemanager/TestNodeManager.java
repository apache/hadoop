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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeLabelsProvider;
import org.junit.Assert;
import org.junit.Test;

public class TestNodeManager {

  public static final class InvalidContainerExecutor extends
      DefaultContainerExecutor {
    @Override
    public void init() throws IOException {
      throw new IOException("dummy executor init called");
    }
  }

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
}
