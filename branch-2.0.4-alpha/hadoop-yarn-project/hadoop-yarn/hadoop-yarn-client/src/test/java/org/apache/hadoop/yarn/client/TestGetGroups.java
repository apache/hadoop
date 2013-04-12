/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.GetGroupsTestBase;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.service.Service.STATE;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestGetGroups extends GetGroupsTestBase {
  
  private static final Log LOG = LogFactory.getLog(TestGetGroups.class);
  
  private static ResourceManager resourceManager;
  
  private static Configuration conf;
  
  @BeforeClass
  public static void setUpResourceManager() throws IOException, InterruptedException {
    conf = new YarnConfiguration();
    resourceManager = new ResourceManager() {
      @Override
      protected void doSecureLogin() throws IOException {
      };
    };
    resourceManager.init(conf);
    new Thread() {
      public void run() {
        resourceManager.start();
      };
    }.start();
    int waitCount = 0;
    while (resourceManager.getServiceState() == STATE.INITED
        && waitCount++ < 10) {
      LOG.info("Waiting for RM to start...");
      Thread.sleep(1000);
    }
    if (resourceManager.getServiceState() != STATE.STARTED) {
      throw new IOException(
          "ResourceManager failed to start. Final state is "
              + resourceManager.getServiceState());
    }
    LOG.info("ResourceManager RMAdmin address: " +
        conf.get(YarnConfiguration.RM_ADMIN_ADDRESS));
  }
  
  @SuppressWarnings("static-access")
  @Before
  public void setUpConf() {
    super.conf = this.conf;
  }
  
  @AfterClass
  public static void tearDownResourceManager() throws InterruptedException {
    if (resourceManager != null) {
      LOG.info("Stopping ResourceManager...");
      resourceManager.stop();
    }
  }
  
  @Override
  protected Tool getTool(PrintStream o) {
    return new GetGroupsForTesting(conf, o);
  }

}
