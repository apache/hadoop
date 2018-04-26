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
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.client.cli.ApplicationCLI;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.conf.ExampleAppJson;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import static org.apache.hadoop.yarn.client.api.AppAdminClient.YARN_APP_ADMIN_CLIENT_PREFIX;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.YARN_SERVICE_BASE_PATH;
import static org.mockito.Mockito.spy;

public class TestServiceCLI {
  private static final Logger LOG = LoggerFactory.getLogger(TestServiceCLI
      .class);

  private Configuration conf = new YarnConfiguration();
  private File basedir;
  private SliderFileSystem fs;
  private String basedirProp;
  private ApplicationCLI cli;

  private void createCLI() {
    cli = new ApplicationCLI();
    PrintStream sysOut = spy(new PrintStream(new ByteArrayOutputStream()));
    PrintStream sysErr = spy(new PrintStream(new ByteArrayOutputStream()));
    cli.setSysOutPrintStream(sysOut);
    cli.setSysErrPrintStream(sysErr);
    conf.set(YARN_APP_ADMIN_CLIENT_PREFIX + DUMMY_APP_TYPE,
        DummyServiceClient.class.getName());
    cli.setConf(conf);
  }

  private void buildApp(String serviceName, String appDef) throws Throwable {
    String[] args = {"app",
        "-D", basedirProp, "-save", serviceName,
        ExampleAppJson.resourceName(appDef),
        "-appTypes", DUMMY_APP_TYPE};
    ToolRunner.run(cli, ApplicationCLI.preProcessArgs(args));
  }

  private void buildApp(String serviceName, String appDef,
      String lifetime, String queue) throws Throwable {
    String[] args = {"app",
        "-D", basedirProp, "-save", serviceName,
        ExampleAppJson.resourceName(appDef),
        "-appTypes", DUMMY_APP_TYPE,
        "-updateLifetime", lifetime,
        "-changeQueue", queue};
    ToolRunner.run(cli, ApplicationCLI.preProcessArgs(args));
  }

  @Before
  public void setup() throws Throwable {
    basedir = new File("target", "apps");
    basedirProp = YARN_SERVICE_BASE_PATH + "=" + basedir.getAbsolutePath();
    conf.set(YARN_SERVICE_BASE_PATH, basedir.getAbsolutePath());
    fs = new SliderFileSystem(conf);
    if (basedir.exists()) {
      FileUtils.deleteDirectory(basedir);
    } else {
      basedir.mkdirs();
    }
    createCLI();
  }

  @After
  public void tearDown() throws IOException {
    if (basedir != null) {
      FileUtils.deleteDirectory(basedir);
    }
    cli.stop();
  }

  @Test
  public void testFlexComponents() throws Throwable {
    // currently can only test building apps, since that is the only
    // operation that doesn't require an RM
    // TODO: expand CLI test to try other commands
    String serviceName = "app-1";
    buildApp(serviceName, ExampleAppJson.APP_JSON);
    checkApp(serviceName, "master", 1L, 3600L, null);

    serviceName = "app-2";
    buildApp(serviceName, ExampleAppJson.APP_JSON, "1000", "qname");
    checkApp(serviceName, "master", 1L, 1000L, "qname");
  }

  @Test
  public void testInitiateServiceUpgrade() throws Exception {
    String[] args = {"app", "-upgrade", "app-1",
        "-initiate", ExampleAppJson.resourceName(ExampleAppJson.APP_JSON),
        "-appTypes", DUMMY_APP_TYPE};
    int result = cli.run(ApplicationCLI.preProcessArgs(args));
    Assert.assertEquals(result, 0);
  }

  @Test
  public void testInitiateAutoFinalizeServiceUpgrade() throws Exception {
    String[] args =  {"app", "-upgrade", "app-1",
        "-initiate", ExampleAppJson.resourceName(ExampleAppJson.APP_JSON),
        "-autoFinalize",
        "-appTypes", DUMMY_APP_TYPE};
    int result = cli.run(ApplicationCLI.preProcessArgs(args));
    Assert.assertEquals(result, 0);
  }

  @Test
  public void testUpgradeInstances() throws Exception {
    conf.set(YARN_APP_ADMIN_CLIENT_PREFIX + DUMMY_APP_TYPE,
        DummyServiceClient.class.getName());
    cli.setConf(conf);
    String[] args = {"app", "-upgrade", "app-1",
        "-instances", "comp1-0,comp1-1",
        "-appTypes", DUMMY_APP_TYPE};
    int result = cli.run(ApplicationCLI.preProcessArgs(args));
    Assert.assertEquals(result, 0);
  }


  private void checkApp(String serviceName, String compName, long count, Long
      lifetime, String queue) throws IOException {
    Service service = ServiceApiUtil.loadService(fs, serviceName);
    Assert.assertEquals(serviceName, service.getName());
    Assert.assertEquals(lifetime, service.getLifetime());
    Assert.assertEquals(queue, service.getQueue());
    List<Component> components = service.getComponents();
    for (Component component : components) {
      if (component.getName().equals(compName)) {
        Assert.assertEquals(count, component.getNumberOfContainers()
            .longValue());
        return;
      }
    }
    Assert.fail();
  }

  private static final String DUMMY_APP_TYPE = "dummy";

  /**
   * Dummy service client for test purpose.
   */
  public static class DummyServiceClient extends ServiceClient {

    @Override
    public int initiateUpgrade(String appName, String fileName,
        boolean autoFinalize) throws IOException, YarnException {
      return 0;
    }

    @Override
    public int actionUpgradeInstances(String appName,
        List<String> componentInstances) throws IOException, YarnException {
      return 0;
    }
  }
}
