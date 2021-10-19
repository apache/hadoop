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

package org.apache.hadoop.yarn.client.cli;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.inject.Guice;
import com.google.inject.Singleton;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStore.LogMutation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServices;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletResponse;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Class for testing {@link SchedConfCLI}.
 */
public class TestSchedConfCLI extends JerseyTestBase {

  private SchedConfCLI cli;

  private static MockRM rm;
  private static String userName;

  private static final File CONF_FILE = new File(new File("target",
      "test-classes"), YarnConfiguration.CS_CONFIGURATION_FILE);
  private static final File OLD_CONF_FILE = new File(new File("target",
      "test-classes"), YarnConfiguration.CS_CONFIGURATION_FILE + ".tmp");

  public TestSchedConfCLI() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Before
  public void setUp() {
    cli = new SchedConfCLI();
  }

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      Configuration conf = new YarnConfiguration();
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
          YarnConfiguration.MEMORY_CONFIGURATION_STORE);

      try {
        userName = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException ioe) {
        throw new RuntimeException("Unable to get current user name "
            + ioe.getMessage(), ioe);
      }

      CapacitySchedulerConfiguration csConf = new
          CapacitySchedulerConfiguration(new Configuration(false), false);
      setupQueueConfiguration(csConf);

      try {
        if (CONF_FILE.exists()) {
          if (!CONF_FILE.renameTo(OLD_CONF_FILE)) {
            throw new RuntimeException("Failed to rename conf file");
          }
        }
        FileOutputStream out = new FileOutputStream(CONF_FILE);
        csConf.writeXml(out);
        out.close();
      } catch (IOException e) {
        throw new RuntimeException("Failed to write XML file", e);
      }

      rm = new MockRM(conf);
      bind(ResourceManager.class).toInstance(rm);
      serve("/*").with(GuiceContainer.class);
      filter("/*").through(TestRMCustomAuthFilter.class);
    }
  }

  /**
   * Custom filter which sets the Remote User for testing purpose.
   */
  @Singleton
  public static class TestRMCustomAuthFilter extends AuthenticationFilter {
    @Override
    public void init(FilterConfig filterConfig) {

    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
        FilterChain filterChain) throws IOException, ServletException {
      HttpServletRequest httpRequest = (HttpServletRequest)request;
      HttpServletResponse httpResponse = (HttpServletResponse) response;
      httpRequest = new HttpServletRequestWrapper(httpRequest) {
        public String getAuthType() {
          return null;
        }

        public String getRemoteUser() {
          return userName;
        }

        public Principal getUserPrincipal() {
          return new Principal() {
            @Override
            public String getName() {
              return userName;
            }
          };
        }
      };
      doFilter(filterChain, httpRequest, httpResponse);
    }
  }

  private static void setupQueueConfiguration(
      CapacitySchedulerConfiguration config) {
    config.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{"testqueue"});
    String a = CapacitySchedulerConfiguration.ROOT + ".testqueue";
    config.setCapacity(a, 100f);
    config.setMaximumCapacity(a, 100f);
  }

  private void cleanUp() throws Exception {
    if (rm != null) {
      rm.stop();
    }
    CONF_FILE.delete();
    if (CONF_FILE.exists()) {
      throw new RuntimeException("Failed to delete configuration file");
    }
    if (OLD_CONF_FILE.exists()) {
      if (!OLD_CONF_FILE.renameTo(CONF_FILE)) {
        throw new RuntimeException("Failed to re-copy old" +
            " configuration file");
      }
    }
    super.tearDown();
  }

  @Test(timeout = 10000)
  public void testGetSchedulerConf() throws Exception {
    ByteArrayOutputStream sysOutStream = new ByteArrayOutputStream();
    PrintStream sysOut = new PrintStream(sysOutStream);
    System.setOut(sysOut);
    try {
      super.setUp();
      GuiceServletConfig.setInjector(
          Guice.createInjector(new WebServletModule()));
      int exitCode = cli.getSchedulerConf("", resource());
      assertEquals("SchedConfCLI failed to run", 0, exitCode);
      assertTrue("Failed to get scheduler configuration",
          sysOutStream.toString().contains("testqueue"));
    } finally {
      cleanUp();
    }
  }

  @Test(timeout = 10000)
  public void testFormatSchedulerConf() throws Exception {
    try {
      super.setUp();
      GuiceServletConfig.setInjector(
          Guice.createInjector(new WebServletModule()));
      ResourceScheduler scheduler = rm.getResourceScheduler();
      MutableConfigurationProvider provider =
          ((MutableConfScheduler) scheduler).getMutableConfProvider();

      SchedConfUpdateInfo schedUpdateInfo = new SchedConfUpdateInfo();
      HashMap<String, String> globalUpdates = new HashMap<>();
      globalUpdates.put("schedKey1", "schedVal1");
      schedUpdateInfo.setGlobalParams(globalUpdates);

      LogMutation log = provider.logAndApplyMutation(
          UserGroupInformation.getCurrentUser(), schedUpdateInfo);
      rm.getRMContext().getRMAdminService().refreshQueues();
      provider.confirmPendingMutation(log, true);

      Configuration schedulerConf = provider.getConfiguration();
      assertEquals("schedVal1", schedulerConf.get("schedKey1"));

      int exitCode = cli.formatSchedulerConf("", resource());
      assertEquals(0, exitCode);

      schedulerConf = provider.getConfiguration();
      assertNull(schedulerConf.get("schedKey1"));
    } finally {
      cleanUp();
    }
  }

  @Test(timeout = 10000)
  public void testInvalidConf() throws Exception {
    ByteArrayOutputStream sysErrStream = new ByteArrayOutputStream();
    PrintStream sysErr = new PrintStream(sysErrStream);
    System.setErr(sysErr);

    // conf pair with no key should be invalid
    executeCommand(sysErrStream, "-add", "root.a:=confVal");
    executeCommand(sysErrStream, "-update", "root.a:=confVal");
    executeCommand(sysErrStream, "-add", "root.a:confKey=confVal=conf");
    executeCommand(sysErrStream, "-update", "root.a:confKey=confVal=c");
  }

  private void executeCommand(ByteArrayOutputStream sysErrStream, String op,
      String queueConf) throws Exception {
    int exitCode = cli.run(new String[] {op, queueConf});
    assertNotEquals("Should return an error code", 0, exitCode);
    assertTrue(sysErrStream.toString()
        .contains("Specify configuration key " + "value as confKey=confVal."));
  }

  @Test(timeout = 10000)
  public void testAddQueues() {
    SchedConfUpdateInfo schedUpdateInfo = new SchedConfUpdateInfo();
    cli.addQueues("root.a:a1=aVal1,a2=aVal2,a3=", schedUpdateInfo);
    Map<String, String> paramValues = new HashMap<>();
    List<QueueConfigInfo> addQueueInfo = schedUpdateInfo.getAddQueueInfo();
    paramValues.put("a1", "aVal1");
    paramValues.put("a2", "aVal2");
    paramValues.put("a3", null);
    validateQueueConfigInfo(addQueueInfo, 0, "root.a", paramValues);

    schedUpdateInfo = new SchedConfUpdateInfo();
    cli.addQueues("root.b:b1=bVal1;root.c:c1=cVal1", schedUpdateInfo);
    addQueueInfo = schedUpdateInfo.getAddQueueInfo();
    assertEquals(2, addQueueInfo.size());
    paramValues.clear();
    paramValues.put("b1", "bVal1");
    validateQueueConfigInfo(addQueueInfo, 0, "root.b", paramValues);
    paramValues.clear();
    paramValues.put("c1", "cVal1");
    validateQueueConfigInfo(addQueueInfo, 1, "root.c", paramValues);
  }

  @Test(timeout = 10000)
  public void testAddQueuesWithCommaInValue() {
    SchedConfUpdateInfo schedUpdateInfo = new SchedConfUpdateInfo();
    cli.addQueues("root.a:a1=a1Val1\\,a1Val2 a1Val3,a2=a2Val1\\,a2Val2",
        schedUpdateInfo);
    List<QueueConfigInfo> addQueueInfo = schedUpdateInfo.getAddQueueInfo();
    Map<String, String> params = new HashMap<>();
    params.put("a1", "a1Val1,a1Val2 a1Val3");
    params.put("a2", "a2Val1,a2Val2");
    validateQueueConfigInfo(addQueueInfo, 0, "root.a", params);
  }

  @Test(timeout = 10000)
  public void testRemoveQueues() {
    SchedConfUpdateInfo schedUpdateInfo = new SchedConfUpdateInfo();
    cli.removeQueues("root.a;root.b;root.c.c1", schedUpdateInfo);
    List<String> removeInfo = schedUpdateInfo.getRemoveQueueInfo();
    assertEquals(3, removeInfo.size());
    assertEquals("root.a", removeInfo.get(0));
    assertEquals("root.b", removeInfo.get(1));
    assertEquals("root.c.c1", removeInfo.get(2));
  }

  @Test(timeout = 10000)
  public void testUpdateQueues() {
    SchedConfUpdateInfo schedUpdateInfo = new SchedConfUpdateInfo();
    Map<String, String> paramValues = new HashMap<>();
    cli.updateQueues("root.a:a1=aVal1,a2=aVal2,a3=", schedUpdateInfo);
    List<QueueConfigInfo> updateQueueInfo = schedUpdateInfo
        .getUpdateQueueInfo();
    paramValues.put("a1", "aVal1");
    paramValues.put("a2", "aVal2");
    paramValues.put("a3", null);
    validateQueueConfigInfo(updateQueueInfo, 0, "root.a", paramValues);

    schedUpdateInfo = new SchedConfUpdateInfo();
    cli.updateQueues("root.b:b1=bVal1;root.c:c1=cVal1", schedUpdateInfo);
    updateQueueInfo = schedUpdateInfo.getUpdateQueueInfo();
    assertEquals(2, updateQueueInfo.size());
    paramValues.clear();
    paramValues.put("b1", "bVal1");
    validateQueueConfigInfo(updateQueueInfo, 0, "root.b", paramValues);
    paramValues.clear();
    paramValues.put("c1", "cVal1");
    validateQueueConfigInfo(updateQueueInfo, 1, "root.c", paramValues);
  }

  private void validateQueueConfigInfo(
      List<QueueConfigInfo> updateQueueInfo, int index, String queuename,
      Map<String, String> paramValues) {
    QueueConfigInfo updateInfo = updateQueueInfo.get(index);
    assertEquals(queuename, updateInfo.getQueue());
    Map<String, String> params = updateInfo.getParams();
    assertEquals(paramValues.size(), params.size());
    paramValues.forEach((k, v) -> assertEquals(v, params.get(k)));
  }

  @Test(timeout = 10000)
  public void testUpdateQueuesWithCommaInValue() {
    SchedConfUpdateInfo schedUpdateInfo = new SchedConfUpdateInfo();
    cli.updateQueues("root.a:a1=a1Val1\\,a1Val2 a1Val3,a2=a2Val1\\,a2Val2",
        schedUpdateInfo);
    List<QueueConfigInfo> updateQueueInfo = schedUpdateInfo
        .getUpdateQueueInfo();
    Map<String, String> paramValues = new HashMap<>();
    paramValues.put("a1", "a1Val1,a1Val2 a1Val3");
    paramValues.put("a2", "a2Val1,a2Val2");
    validateQueueConfigInfo(updateQueueInfo, 0, "root.a", paramValues);
  }

  @Test(timeout = 10000)
  public void testGlobalUpdate() {
    SchedConfUpdateInfo schedUpdateInfo = new SchedConfUpdateInfo();
    cli.globalUpdates("schedKey1=schedVal1,schedKey2=schedVal2",
        schedUpdateInfo);
    Map<String, String> paramValues = new HashMap<>();
    paramValues.put("schedKey1", "schedVal1");
    paramValues.put("schedKey2", "schedVal2");
    validateGlobalParams(schedUpdateInfo, paramValues);
  }

  @Test(timeout = 10000)
  public void testGlobalUpdateWithCommaInValue() {
    SchedConfUpdateInfo schedUpdateInfo = new SchedConfUpdateInfo();
    cli.globalUpdates(
        "schedKey1=schedVal1.1\\,schedVal1.2 schedVal1.3,schedKey2=schedVal2",
        schedUpdateInfo);
    Map<String, String> paramValues = new HashMap<>();
    paramValues.put("schedKey1", "schedVal1.1,schedVal1.2 schedVal1.3");
    paramValues.put("schedKey2", "schedVal2");
    validateGlobalParams(schedUpdateInfo, paramValues);
  }

  private void validateGlobalParams(SchedConfUpdateInfo schedUpdateInfo,
      Map<String, String> paramValues) {
    Map<String, String> globalInfo = schedUpdateInfo.getGlobalParams();
    assertEquals(paramValues.size(), globalInfo.size());
    paramValues.forEach((k, v) -> assertEquals(v, globalInfo.get(k)));
  }
}
