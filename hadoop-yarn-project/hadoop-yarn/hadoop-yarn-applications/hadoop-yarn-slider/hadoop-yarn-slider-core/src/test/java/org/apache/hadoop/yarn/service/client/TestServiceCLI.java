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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.ClientAMProtocol;
import org.apache.hadoop.yarn.service.client.params.ClientArgs;
import org.apache.hadoop.yarn.service.conf.ExampleAppJson;
import org.apache.slider.api.resource.Component;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.yarn.service.client.params.Arguments.ARG_APPDEF;
import static org.apache.hadoop.yarn.service.conf.SliderXmlConfKeys.KEY_SLIDER_BASE_PATH;
import static org.mockito.Mockito.mock;

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
    conf.set(KEY_SLIDER_BASE_PATH, basedir.getAbsolutePath());
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
          @Override protected ClientAMProtocol connectToAM(String appName)
              throws IOException, YarnException {
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
        ServiceApiUtil.getApplicationComponents(fs, "service-1");
    for (Component component : components) {
      if (component.getName().equals(compName)) {
        Assert.assertEquals(count, component.getNumberOfContainers().longValue());
        return;
      }
    }
    Assert.fail();
  }
}
