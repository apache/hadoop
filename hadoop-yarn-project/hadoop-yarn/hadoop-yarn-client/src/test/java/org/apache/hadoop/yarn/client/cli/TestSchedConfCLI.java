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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Class for testing {@link SchedConfCLI}.
 */
public class TestSchedConfCLI extends JerseyTestBase {

  private ByteArrayOutputStream sysOutStream;
  private PrintStream sysOut;

  private ByteArrayOutputStream sysErrStream;
  private PrintStream sysErr;

  private SchedConfCLI cli;

  private static MockRM rm;
  private static String userName;
  private static CapacitySchedulerConfiguration csConf;

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
    sysOutStream = new ByteArrayOutputStream();
    sysOut =  new PrintStream(sysOutStream);
    System.setOut(sysOut);

    sysErrStream = new ByteArrayOutputStream();
    sysErr = new PrintStream(sysErrStream);
    System.setErr(sysErr);

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

      csConf = new CapacitySchedulerConfiguration(new Configuration(false),
          false);
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

      provider.logAndApplyMutation(UserGroupInformation.getCurrentUser(),
          schedUpdateInfo);
      rm.getRMContext().getRMAdminService().refreshQueues();
      provider.confirmPendingMutation(true);

      Configuration schedulerConf = provider.getConfiguration();
      assertEquals("schedVal1", schedulerConf.get("schedKey1"));

      int exitCode = cli.formatSchedulerConf("", resource());
      assertEquals(0, exitCode);

      schedulerConf = provider.getConfiguration();
      assertNull(schedulerConf.get("schedKey1"));
    } finally {
      if (rm != null) {
        rm.stop();
      }
      CONF_FILE.delete();
      if (OLD_CONF_FILE.exists()) {
        if (!OLD_CONF_FILE.renameTo(CONF_FILE)) {
          throw new RuntimeException("Failed to re-copy old" +
              " configuration file");
        }
      }
      super.tearDown();
    }
  }

  @Test(timeout = 10000)
  public void testInvalidConf() throws Exception {
    // conf pair with no key should be invalid
    int exitCode = cli.run(new String[] {"-add", "root.a:=confVal"});
    assertTrue("Should return an error code", exitCode != 0);
    assertTrue(sysErrStream.toString().contains("Specify configuration key " +
        "value as confKey=confVal."));
    exitCode = cli.run(new String[] {"-update", "root.a:=confVal"});
    assertTrue("Should return an error code", exitCode != 0);
    assertTrue(sysErrStream.toString().contains("Specify configuration key " +
        "value as confKey=confVal."));

    exitCode = cli.run(new String[] {"-add", "root.a:confKey=confVal=conf"});
    assertTrue("Should return an error code", exitCode != 0);
    assertTrue(sysErrStream.toString().contains("Specify configuration key " +
        "value as confKey=confVal."));
    exitCode = cli.run(new String[] {"-update", "root.a:confKey=confVal=c"});
    assertTrue("Should return an error code", exitCode != 0);
    assertTrue(sysErrStream.toString().contains("Specify configuration key " +
        "value as confKey=confVal."));
  }

  @Test(timeout = 10000)
  public void testAddQueues() {
    SchedConfUpdateInfo schedUpdateInfo = new SchedConfUpdateInfo();
    cli.addQueues("root.a:a1=aVal1,a2=aVal2,a3=", schedUpdateInfo);
    QueueConfigInfo addInfo = schedUpdateInfo.getAddQueueInfo().get(0);
    assertEquals("root.a", addInfo.getQueue());
    Map<String, String> params = addInfo.getParams();
    assertEquals(3, params.size());
    assertEquals("aVal1", params.get("a1"));
    assertEquals("aVal2", params.get("a2"));
    assertNull(params.get("a3"));

    schedUpdateInfo = new SchedConfUpdateInfo();
    cli.addQueues("root.b:b1=bVal1;root.c:c1=cVal1", schedUpdateInfo);
    assertEquals(2, schedUpdateInfo.getAddQueueInfo().size());
    QueueConfigInfo bAddInfo = schedUpdateInfo.getAddQueueInfo().get(0);
    assertEquals("root.b", bAddInfo.getQueue());
    Map<String, String> bParams = bAddInfo.getParams();
    assertEquals(1, bParams.size());
    assertEquals("bVal1", bParams.get("b1"));
    QueueConfigInfo cAddInfo = schedUpdateInfo.getAddQueueInfo().get(1);
    assertEquals("root.c", cAddInfo.getQueue());
    Map<String, String> cParams = cAddInfo.getParams();
    assertEquals(1, cParams.size());
    assertEquals("cVal1", cParams.get("c1"));
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
    cli.updateQueues("root.a:a1=aVal1,a2=aVal2,a3=", schedUpdateInfo);
    QueueConfigInfo updateInfo = schedUpdateInfo.getUpdateQueueInfo().get(0);
    assertEquals("root.a", updateInfo.getQueue());
    Map<String, String> params = updateInfo.getParams();
    assertEquals(3, params.size());
    assertEquals("aVal1", params.get("a1"));
    assertEquals("aVal2", params.get("a2"));
    assertNull(params.get("a3"));

    schedUpdateInfo = new SchedConfUpdateInfo();
    cli.updateQueues("root.b:b1=bVal1;root.c:c1=cVal1", schedUpdateInfo);
    assertEquals(2, schedUpdateInfo.getUpdateQueueInfo().size());
    QueueConfigInfo bUpdateInfo = schedUpdateInfo.getUpdateQueueInfo().get(0);
    assertEquals("root.b", bUpdateInfo.getQueue());
    Map<String, String> bParams = bUpdateInfo.getParams();
    assertEquals(1, bParams.size());
    assertEquals("bVal1", bParams.get("b1"));
    QueueConfigInfo cUpdateInfo = schedUpdateInfo.getUpdateQueueInfo().get(1);
    assertEquals("root.c", cUpdateInfo.getQueue());
    Map<String, String> cParams = cUpdateInfo.getParams();
    assertEquals(1, cParams.size());
    assertEquals("cVal1", cParams.get("c1"));
  }

  @Test(timeout = 10000)
  public void testGlobalUpdate() {
    SchedConfUpdateInfo schedUpdateInfo = new SchedConfUpdateInfo();
    cli.globalUpdates("schedKey1=schedVal1,schedKey2=schedVal2",
        schedUpdateInfo);
    Map<String, String> globalInfo = schedUpdateInfo.getGlobalParams();
    assertEquals(2, globalInfo.size());
    assertEquals("schedVal1", globalInfo.get("schedKey1"));
    assertEquals("schedVal2", globalInfo.get("schedKey2"));
  }
}
