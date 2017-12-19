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
import org.apache.hadoop.yarn.client.api.AppAdminClient;
import org.apache.hadoop.yarn.client.cli.ApplicationCLI;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.YARN_SERVICE_BASE_PATH;

public class TestServiceCLI {
  private static final Logger LOG = LoggerFactory.getLogger(TestServiceCLI
      .class);

  private Configuration conf = new YarnConfiguration();
  private File basedir;
  private SliderFileSystem fs;
  private String basedirProp;

  private void runCLI(String[] args) throws Exception {
    LOG.info("running CLI: yarn {}", Arrays.asList(args));
    ApplicationCLI cli = new ApplicationCLI();
    cli.setSysOutPrintStream(System.out);
    cli.setSysErrPrintStream(System.err);
    int res = ToolRunner.run(cli, ApplicationCLI.preProcessArgs(args));
    cli.stop();
  }

  private void buildApp(String serviceName, String appDef) throws Throwable {
    String[] args = {"app",
        "-D", basedirProp, "-save", serviceName,
        ExampleAppJson.resourceName(appDef),
        "-appTypes", AppAdminClient.UNIT_TEST_TYPE};
    runCLI(args);
  }

  private void buildApp(String serviceName, String appDef, String lifetime,
      String queue) throws Throwable {
    String[] args = {"app",
        "-D", basedirProp, "-save", serviceName,
        ExampleAppJson.resourceName(appDef),
        "-appTypes", AppAdminClient.UNIT_TEST_TYPE,
        "-updateLifetime", lifetime,
        "-changeQueue", queue};
    runCLI(args);
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
  }

  @After
  public void tearDown() throws IOException {
    if (basedir != null) {
      FileUtils.deleteDirectory(basedir);
    }
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
}
