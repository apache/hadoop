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

package org.apache.hadoop.yarn.service.client;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.ClientAMProtocol;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.client.params.ClientArgs;
import org.apache.hadoop.yarn.service.conf.ExampleAppJson;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS;
import static org.apache.hadoop.yarn.service.client.params.Arguments.ARG_APPDEF;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.YARN_SERVICE_BASE_PATH;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class TestServiceCLI {

  protected Configuration conf = new YarnConfiguration();
  private File basedir;
  private ServiceCLI cli;
  private SliderFileSystem fs;

  private void buildApp(String appName, String appDef) throws Throwable {
    String[] args =
        { "build", appName, ARG_APPDEF, ExampleAppJson.resourceName(appDef) };
    ClientArgs clientArgs = new ClientArgs(args);
    clientArgs.parse();
    cli.exec(clientArgs);
  }

  @Before
  public void setup() throws Throwable {
    basedir = new File("target", "apps");
    conf.set(YARN_SERVICE_BASE_PATH, basedir.getAbsolutePath());
    conf.setLong(RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, 0);
    conf.setLong(RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, 1);
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    conf.setInt(CommonConfigurationKeysPublic.
        IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, 0);
    fs = new SliderFileSystem(conf);
    if (basedir.exists()) {
      FileUtils.deleteDirectory(basedir);
    } else {
      basedir.mkdirs();
    }

    // create a CLI and skip connection to AM
    cli = new ServiceCLI() {
      @Override protected void createServiceClient() {
        client = new ServiceClient() {
          @Override
          protected void serviceInit(Configuration configuration)
              throws Exception {
            super.serviceInit(conf);
            yarnClient = spy(yarnClient);
            ApplicationReport report = Records.newRecord(ApplicationReport.class);
            report.setYarnApplicationState(YarnApplicationState.RUNNING);
            report.setHost("localhost");
            doReturn(report).when(yarnClient).getApplicationReport(anyObject());
          }
          @Override
          protected ClientAMProtocol createAMProxy(String host, int port)
              throws IOException {
            return mock(ClientAMProtocol.class);
          }
        };
        client.init(conf);
        client.start();
      }
    };
  }

  @After
  public void tearDown() throws IOException {
    if (basedir != null) {
      FileUtils.deleteDirectory(basedir);
    }
  }

  // Test flex components count are persisted.
  @Test
  public void testFlexComponents() throws Throwable {
    buildApp("service-1", ExampleAppJson.APP_JSON);

    checkCompCount("master", 1L);

    // increase by 2
    String[] flexUpArgs = {"flex", "service-1", "--component", "master" , "+2"};
    ClientArgs clientArgs = new ClientArgs(flexUpArgs);
    clientArgs.parse();
    cli.exec(clientArgs);
    checkCompCount("master", 3L);

    // decrease by 1
    String[] flexDownArgs = {"flex", "service-1", "--component", "master", "-1"};
    clientArgs = new ClientArgs(flexDownArgs);
    clientArgs.parse();
    cli.exec(clientArgs);
    checkCompCount("master", 2L);

    String[] flexAbsoluteArgs = {"flex", "service-1", "--component", "master", "10"};
    clientArgs = new ClientArgs(flexAbsoluteArgs);
    clientArgs.parse();
    cli.exec(clientArgs);
    checkCompCount("master", 10L);
  }

  private void checkCompCount(String compName, long count) throws IOException {
    List<Component> components =
        ServiceApiUtil.getComponents(fs, "service-1");
    for (Component component : components) {
      if (component.getName().equals(compName)) {
        Assert.assertEquals(count, component.getNumberOfContainers().longValue());
        return;
      }
    }
    Assert.fail();
  }
}
