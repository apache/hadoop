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

package org.apache.hadoop.lib.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.lib.lang.XException;
import org.apache.hadoop.test.HTestCase;
import org.apache.hadoop.test.TestDir;
import org.apache.hadoop.test.TestDirHelper;
import org.apache.hadoop.test.TestException;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

public class TestServer extends HTestCase {

  @Test
  @TestDir
  public void constructorsGetters() throws Exception {
    Server server = new Server("server", getAbsolutePath("/a"),
      getAbsolutePath("/b"), getAbsolutePath("/c"), getAbsolutePath("/d"),
      new Configuration(false));
    assertThat(server.getHomeDir()).isEqualTo(getAbsolutePath("/a"));
    assertThat(server.getConfigDir()).isEqualTo(getAbsolutePath("/b"));
    assertThat(server.getLogDir()).isEqualTo(getAbsolutePath("/c"));
    assertThat(server.getTempDir()).isEqualTo(getAbsolutePath("/d"));
    assertThat(server.getName()).isEqualTo("server");
    assertThat(server.getPrefix()).isEqualTo("server");
    assertThat(server.getPrefixedName("name")).isEqualTo("server.name");
    assertNotNull(server.getConfig());

    server = new Server("server", getAbsolutePath("/a"), getAbsolutePath("/b"),
      getAbsolutePath("/c"), getAbsolutePath("/d"));
    assertThat(server.getHomeDir()).isEqualTo(getAbsolutePath("/a"));
    assertThat(server.getConfigDir()).isEqualTo(getAbsolutePath("/b"));
    assertThat(server.getLogDir()).isEqualTo(getAbsolutePath("/c"));
    assertThat(server.getTempDir()).isEqualTo(getAbsolutePath("/d"));
    assertThat(server.getName()).isEqualTo("server");
    assertThat(server.getPrefix()).isEqualTo("server");
    assertThat(server.getPrefixedName("name")).isEqualTo("server.name");
    assertNull(server.getConfig());

    server = new Server("server", TestDirHelper.getTestDir().getAbsolutePath(), new Configuration(false));
    assertThat(server.getHomeDir()).isEqualTo(TestDirHelper.getTestDir().getAbsolutePath());
    assertThat(server.getConfigDir()).isEqualTo(TestDirHelper.getTestDir() + "/conf");
    assertThat(server.getLogDir()).isEqualTo(TestDirHelper.getTestDir() + "/log");
    assertThat(server.getTempDir()).isEqualTo(TestDirHelper.getTestDir() + "/temp");
    assertThat(server.getName()).isEqualTo("server");
    assertThat(server.getPrefix()).isEqualTo("server");
    assertThat(server.getPrefixedName("name")).isEqualTo("server.name");
    assertNotNull(server.getConfig());

    server = new Server("server", TestDirHelper.getTestDir().getAbsolutePath());
    assertThat(server.getHomeDir()).isEqualTo(TestDirHelper.getTestDir().getAbsolutePath());
    assertThat(server.getConfigDir()).isEqualTo(TestDirHelper.getTestDir() + "/conf");
    assertThat(server.getLogDir()).isEqualTo(TestDirHelper.getTestDir() + "/log");
    assertThat(server.getTempDir()).isEqualTo(TestDirHelper.getTestDir() + "/temp");
    assertThat(server.getName()).isEqualTo("server");
    assertThat(server.getPrefix()).isEqualTo("server");
    assertThat(server.getPrefixedName("name")).isEqualTo("server.name");
    assertNull(server.getConfig());
  }

  @Test
  @TestException(exception = ServerException.class, msgRegExp = "S01.*")
  @TestDir
  public void initNoHomeDir() throws Exception {
    File homeDir = new File(TestDirHelper.getTestDir(), "home");
    Configuration conf = new Configuration(false);
    conf.set("server.services", TestService.class.getName());
    Server server = new Server("server", homeDir.getAbsolutePath(), conf);
    server.init();
  }

  @Test
  @TestException(exception = ServerException.class, msgRegExp = "S02.*")
  @TestDir
  public void initHomeDirNotDir() throws Exception {
    File homeDir = new File(TestDirHelper.getTestDir(), "home");
    new FileOutputStream(homeDir).close();
    Configuration conf = new Configuration(false);
    conf.set("server.services", TestService.class.getName());
    Server server = new Server("server", homeDir.getAbsolutePath(), conf);
    server.init();
  }

  @Test
  @TestException(exception = ServerException.class, msgRegExp = "S01.*")
  @TestDir
  public void initNoConfigDir() throws Exception {
    File homeDir = new File(TestDirHelper.getTestDir(), "home");
    assertTrue(homeDir.mkdir());
    assertTrue(new File(homeDir, "log").mkdir());
    assertTrue(new File(homeDir, "temp").mkdir());
    Configuration conf = new Configuration(false);
    conf.set("server.services", TestService.class.getName());
    Server server = new Server("server", homeDir.getAbsolutePath(), conf);
    server.init();
  }

  @Test
  @TestException(exception = ServerException.class, msgRegExp = "S02.*")
  @TestDir
  public void initConfigDirNotDir() throws Exception {
    File homeDir = new File(TestDirHelper.getTestDir(), "home");
    assertTrue(homeDir.mkdir());
    assertTrue(new File(homeDir, "log").mkdir());
    assertTrue(new File(homeDir, "temp").mkdir());
    File configDir = new File(homeDir, "conf");
    new FileOutputStream(configDir).close();
    Configuration conf = new Configuration(false);
    conf.set("server.services", TestService.class.getName());
    Server server = new Server("server", homeDir.getAbsolutePath(), conf);
    server.init();
  }

  @Test
  @TestException(exception = ServerException.class, msgRegExp = "S01.*")
  @TestDir
  public void initNoLogDir() throws Exception {
    File homeDir = new File(TestDirHelper.getTestDir(), "home");
    assertTrue(homeDir.mkdir());
    assertTrue(new File(homeDir, "conf").mkdir());
    assertTrue(new File(homeDir, "temp").mkdir());
    Configuration conf = new Configuration(false);
    conf.set("server.services", TestService.class.getName());
    Server server = new Server("server", homeDir.getAbsolutePath(), conf);
    server.init();
  }

  @Test
  @TestException(exception = ServerException.class, msgRegExp = "S02.*")
  @TestDir
  public void initLogDirNotDir() throws Exception {
    File homeDir = new File(TestDirHelper.getTestDir(), "home");
    assertTrue(homeDir.mkdir());
    assertTrue(new File(homeDir, "conf").mkdir());
    assertTrue(new File(homeDir, "temp").mkdir());
    File logDir = new File(homeDir, "log");
    new FileOutputStream(logDir).close();
    Configuration conf = new Configuration(false);
    conf.set("server.services", TestService.class.getName());
    Server server = new Server("server", homeDir.getAbsolutePath(), conf);
    server.init();
  }

  @Test
  @TestException(exception = ServerException.class, msgRegExp = "S01.*")
  @TestDir
  public void initNoTempDir() throws Exception {
    File homeDir = new File(TestDirHelper.getTestDir(), "home");
    assertTrue(homeDir.mkdir());
    assertTrue(new File(homeDir, "conf").mkdir());
    assertTrue(new File(homeDir, "log").mkdir());
    Configuration conf = new Configuration(false);
    conf.set("server.services", TestService.class.getName());
    Server server = new Server("server", homeDir.getAbsolutePath(), conf);
    server.init();
  }

  @Test
  @TestException(exception = ServerException.class, msgRegExp = "S02.*")
  @TestDir
  public void initTempDirNotDir() throws Exception {
    File homeDir = new File(TestDirHelper.getTestDir(), "home");
    assertTrue(homeDir.mkdir());
    assertTrue(new File(homeDir, "conf").mkdir());
    assertTrue(new File(homeDir, "log").mkdir());
    File tempDir = new File(homeDir, "temp");
    new FileOutputStream(tempDir).close();
    Configuration conf = new Configuration(false);
    conf.set("server.services", TestService.class.getName());
    Server server = new Server("server", homeDir.getAbsolutePath(), conf);
    server.init();
  }

  @Test
  @TestException(exception = ServerException.class, msgRegExp = "S05.*")
  @TestDir
  public void siteFileNotAFile() throws Exception {
    String homeDir = TestDirHelper.getTestDir().getAbsolutePath();
    File siteFile = new File(homeDir, "server-site.xml");
    assertTrue(siteFile.mkdir());
    Server server = new Server("server", homeDir, homeDir, homeDir, homeDir);
    server.init();
  }

  private Server createServer(Configuration conf) {
    return new Server("server", TestDirHelper.getTestDir().getAbsolutePath(),
                      TestDirHelper.getTestDir().getAbsolutePath(),
                      TestDirHelper.getTestDir().getAbsolutePath(), TestDirHelper.getTestDir().getAbsolutePath(), conf);
  }

  @Test
  @TestDir
  public void log4jFile() throws Exception {
    InputStream is = Server.getResource("default-log4j.properties");
    OutputStream os = new FileOutputStream(new File(TestDirHelper.getTestDir(), "server-log4j.properties"));
    IOUtils.copyBytes(is, os, 1024, true);
    Configuration conf = new Configuration(false);
    Server server = createServer(conf);
    server.init();
  }

  public static class LifeCycleService extends BaseService {

    public LifeCycleService() {
      super("lifecycle");
    }

    @Override
    protected void init() throws ServiceException {
      assertThat(getServer().getStatus()).isEqualTo(Server.Status.BOOTING);
    }

    @Override
    public void destroy() {
      assertThat(getServer().getStatus()).isEqualTo(Server.Status.SHUTTING_DOWN);
      super.destroy();
    }

    @Override
    public Class getInterface() {
      return LifeCycleService.class;
    }
  }

  @Test
  @TestDir
  public void lifeCycle() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("server.services", LifeCycleService.class.getName());
    Server server = createServer(conf);
    assertThat(server.getStatus()).isEqualTo(Server.Status.UNDEF);
    server.init();
    assertNotNull(server.get(LifeCycleService.class));
    assertThat(server.getStatus()).isEqualTo(Server.Status.NORMAL);
    server.destroy();
    assertThat(server.getStatus()).isEqualTo(Server.Status.SHUTDOWN);
  }

  @Test
  @TestDir
  public void startWithStatusNotNormal() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("server.startup.status", "ADMIN");
    Server server = createServer(conf);
    server.init();
    assertThat(server.getStatus()).isEqualTo(Server.Status.ADMIN);
    server.destroy();
  }

  @Test(expected = IllegalArgumentException.class)
  @TestDir
  public void nonSeteableStatus() throws Exception {
    Configuration conf = new Configuration(false);
    Server server = createServer(conf);
    server.init();
    server.setStatus(Server.Status.SHUTDOWN);
  }

  public static class TestService implements Service {
    static List<String> LIFECYCLE = new ArrayList<String>();

    @Override
    public void init(Server server) throws ServiceException {
      LIFECYCLE.add("init");
    }

    @Override
    public void postInit() throws ServiceException {
      LIFECYCLE.add("postInit");
    }

    @Override
    public void destroy() {
      LIFECYCLE.add("destroy");
    }

    @Override
    public Class[] getServiceDependencies() {
      return new Class[0];
    }

    @Override
    public Class getInterface() {
      return TestService.class;
    }

    @Override
    public void serverStatusChange(Server.Status oldStatus, Server.Status newStatus) throws ServiceException {
      LIFECYCLE.add("serverStatusChange");
    }
  }

  public static class TestServiceExceptionOnStatusChange extends TestService {

    @Override
    public void serverStatusChange(Server.Status oldStatus, Server.Status newStatus) throws ServiceException {
      throw new RuntimeException();
    }
  }

  @Test
  @TestDir
  public void changeStatus() throws Exception {
    TestService.LIFECYCLE.clear();
    Configuration conf = new Configuration(false);
    conf.set("server.services", TestService.class.getName());
    Server server = createServer(conf);
    server.init();
    server.setStatus(Server.Status.ADMIN);
    assertTrue(TestService.LIFECYCLE.contains("serverStatusChange"));
  }

  @Test
  @TestException(exception = ServerException.class, msgRegExp = "S11.*")
  @TestDir
  public void changeStatusServiceException() throws Exception {
    TestService.LIFECYCLE.clear();
    Configuration conf = new Configuration(false);
    conf.set("server.services", TestServiceExceptionOnStatusChange.class.getName());
    Server server = createServer(conf);
    server.init();
  }

  @Test
  @TestDir
  public void setSameStatus() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("server.services", TestService.class.getName());
    Server server = createServer(conf);
    server.init();
    TestService.LIFECYCLE.clear();
    server.setStatus(server.getStatus());
    assertFalse(TestService.LIFECYCLE.contains("serverStatusChange"));
  }

  @Test
  @TestDir
  public void serviceLifeCycle() throws Exception {
    TestService.LIFECYCLE.clear();
    Configuration conf = new Configuration(false);
    conf.set("server.services", TestService.class.getName());
    Server server = createServer(conf);
    server.init();
    assertNotNull(server.get(TestService.class));
    server.destroy();
    assertThat(TestService.LIFECYCLE).isEqualTo(
            Arrays.asList("init", "postInit", "serverStatusChange", "destroy"));
  }

  @Test
  @TestDir
  public void loadingDefaultConfig() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Server server = new Server("testserver", dir, dir, dir, dir);
    server.init();
    assertThat(server.getConfig().get("testserver.a")).isEqualTo("default");
  }

  @Test
  @TestDir
  public void loadingSiteConfig() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    File configFile = new File(dir, "testserver-site.xml");
    Writer w = new FileWriter(configFile);
    w.write("<configuration><property><name>testserver.a</name><value>site</value></property></configuration>");
    w.close();
    Server server = new Server("testserver", dir, dir, dir, dir);
    server.init();
    assertThat(server.getConfig().get("testserver.a")).isEqualTo("site");
  }

  @Test
  @TestDir
  public void loadingSysPropConfig() throws Exception {
    try {
      System.setProperty("testserver.a", "sysprop");
      String dir = TestDirHelper.getTestDir().getAbsolutePath();
      File configFile = new File(dir, "testserver-site.xml");
      Writer w = new FileWriter(configFile);
      w.write("<configuration><property><name>testserver.a</name><value>site</value></property></configuration>");
      w.close();
      Server server = new Server("testserver", dir, dir, dir, dir);
      server.init();
      assertThat(server.getConfig().get("testserver.a")).isEqualTo("sysprop");
    } finally {
      System.getProperties().remove("testserver.a");
    }
  }

  @Test(expected = IllegalStateException.class)
  @TestDir
  public void illegalState1() throws Exception {
    Server server = new Server("server", TestDirHelper.getTestDir().getAbsolutePath(), new Configuration(false));
    server.destroy();
  }

  @Test(expected = IllegalStateException.class)
  @TestDir
  public void illegalState2() throws Exception {
    Server server = new Server("server", TestDirHelper.getTestDir().getAbsolutePath(), new Configuration(false));
    server.get(Object.class);
  }

  @Test(expected = IllegalStateException.class)
  @TestDir
  public void illegalState3() throws Exception {
    Server server = new Server("server", TestDirHelper.getTestDir().getAbsolutePath(), new Configuration(false));
    server.setService(null);
  }

  @Test(expected = IllegalStateException.class)
  @TestDir
  public void illegalState4() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Server server = new Server("server", dir, dir, dir, dir, new Configuration(false));
    server.init();
    server.init();
  }

  private static List<String> ORDER = new ArrayList<String>();

  public abstract static class MyService implements Service, XException.ERROR {
    private String id;
    private Class serviceInterface;
    private Class[] dependencies;
    private boolean failOnInit;
    private boolean failOnDestroy;

    protected MyService(String id, Class serviceInterface, Class[] dependencies, boolean failOnInit,
                        boolean failOnDestroy) {
      this.id = id;
      this.serviceInterface = serviceInterface;
      this.dependencies = dependencies;
      this.failOnInit = failOnInit;
      this.failOnDestroy = failOnDestroy;
    }


    @Override
    public void init(Server server) throws ServiceException {
      ORDER.add(id + ".init");
      if (failOnInit) {
        throw new ServiceException(this);
      }
    }

    @Override
    public void postInit() throws ServiceException {
      ORDER.add(id + ".postInit");
    }

    @Override
    public String getTemplate() {
      return "";
    }

    @Override
    public void destroy() {
      ORDER.add(id + ".destroy");
      if (failOnDestroy) {
        throw new RuntimeException();
      }
    }

    @Override
    public Class[] getServiceDependencies() {
      return dependencies;
    }

    @Override
    public Class getInterface() {
      return serviceInterface;
    }

    @Override
    public void serverStatusChange(Server.Status oldStatus, Server.Status newStatus) throws ServiceException {
    }
  }

  public static class MyService1 extends MyService {

    public MyService1() {
      super("s1", MyService1.class, null, false, false);
    }

    protected MyService1(String id, Class serviceInterface, Class[] dependencies, boolean failOnInit,
                         boolean failOnDestroy) {
      super(id, serviceInterface, dependencies, failOnInit, failOnDestroy);
    }
  }

  public static class MyService2 extends MyService {
    public MyService2() {
      super("s2", MyService2.class, null, true, false);
    }
  }


  public static class MyService3 extends MyService {
    public MyService3() {
      super("s3", MyService3.class, null, false, false);
    }
  }

  public static class MyService1a extends MyService1 {
    public MyService1a() {
      super("s1a", MyService1.class, null, false, false);
    }
  }

  public static class MyService4 extends MyService1 {

    public MyService4() {
      super("s4a", String.class, null, false, false);
    }
  }

  public static class MyService5 extends MyService {

    public MyService5() {
      super("s5", MyService5.class, null, false, true);
    }

    protected MyService5(String id, Class serviceInterface, Class[] dependencies, boolean failOnInit,
                         boolean failOnDestroy) {
      super(id, serviceInterface, dependencies, failOnInit, failOnDestroy);
    }
  }

  public static class MyService5a extends MyService5 {

    public MyService5a() {
      super("s5a", MyService5.class, null, false, false);
    }
  }

  public static class MyService6 extends MyService {

    public MyService6() {
      super("s6", MyService6.class, new Class[]{MyService1.class}, false, false);
    }
  }

  public static class MyService7 extends MyService {

    @SuppressWarnings({"UnusedParameters"})
    public MyService7(String foo) {
      super("s6", MyService7.class, new Class[]{MyService1.class}, false, false);
    }
  }

  @Test
  @TestException(exception = ServerException.class, msgRegExp = "S08.*")
  @TestDir
  public void invalidSservice() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", "foo");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
  }

  @Test
  @TestException(exception = ServerException.class, msgRegExp = "S07.*")
  @TestDir
  public void serviceWithNoDefaultConstructor() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", MyService7.class.getName());
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
  }

  @Test
  @TestException(exception = ServerException.class, msgRegExp = "S04.*")
  @TestDir
  public void serviceNotImplementingServiceInterface() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", MyService4.class.getName());
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
  }

  @Test
  @TestException(exception = ServerException.class, msgRegExp = "S10.*")
  @TestDir
  public void serviceWithMissingDependency() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    String services = StringUtils.join(",", Arrays.asList(MyService3.class.getName(), MyService6.class.getName())
    );
    conf.set("server.services", services);
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
  }

  @Test
  @TestDir
  public void services() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf;
    Server server;

    // no services
    ORDER.clear();
    conf = new Configuration(false);
    server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    assertThat(ORDER.size()).isEqualTo(0);

    // 2 services init/destroy
    ORDER.clear();
    String services = StringUtils.join(",", Arrays.asList(MyService1.class.getName(), MyService3.class.getName())
    );
    conf = new Configuration(false);
    conf.set("server.services", services);
    server = new Server("server", dir, dir, dir, dir, conf);
    server.init();

    assertThat(server.get(MyService1.class).getInterface()).isEqualTo(MyService1.class);
    assertThat(server.get(MyService3.class).getInterface()).isEqualTo(MyService3.class);
    assertThat(ORDER.size()).isEqualTo(4);
    assertThat(ORDER.get(0)).isEqualTo("s1.init");
    assertThat(ORDER.get(1)).isEqualTo("s3.init");
    assertThat(ORDER.get(2)).isEqualTo("s1.postInit");
    assertThat(ORDER.get(3)).isEqualTo("s3.postInit");
    server.destroy();
    assertThat(ORDER.size()).isEqualTo(6);
    assertThat(ORDER.get(4)).isEqualTo("s3.destroy");
    assertThat(ORDER.get(5)).isEqualTo("s1.destroy");

    // 3 services, 2nd one fails on init
    ORDER.clear();
    services = StringUtils.join(",", Arrays.asList(MyService1.class.getName(), MyService2.class.getName(),
                                                   MyService3.class.getName()));
    conf = new Configuration(false);
    conf.set("server.services", services);

    server = new Server("server", dir, dir, dir, dir, conf);
    try {
      server.init();
      fail();
    } catch (ServerException ex) {
      assertThat(ex.getError().getClass()).isEqualTo(MyService2.class);
    } catch (Exception ex) {
      fail();
    }
    assertThat(ORDER.size()).isEqualTo(3);
    assertThat(ORDER.get(0)).isEqualTo("s1.init");
    assertThat(ORDER.get(1)).isEqualTo("s2.init");
    assertThat(ORDER.get(2)).isEqualTo("s1.destroy");

    // 2 services one fails on destroy
    ORDER.clear();
    services = StringUtils.join(",", Arrays.asList(MyService1.class.getName(), MyService5.class.getName()));
    conf = new Configuration(false);
    conf.set("server.services", services);
    server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    assertThat(ORDER.size()).isEqualTo(4);
    assertThat(ORDER.get(0)).isEqualTo("s1.init");
    assertThat(ORDER.get(1)).isEqualTo("s5.init");
    assertThat(ORDER.get(2)).isEqualTo("s1.postInit");
    assertThat(ORDER.get(3)).isEqualTo("s5.postInit");
    server.destroy();
    assertThat(ORDER.size()).isEqualTo(6);
    assertThat(ORDER.get(4)).isEqualTo("s5.destroy");
    assertThat(ORDER.get(5)).isEqualTo("s1.destroy");

    // service override via ext
    ORDER.clear();
    services = StringUtils.join(",", Arrays.asList(MyService1.class.getName(), MyService3.class.getName()));
    String servicesExt = StringUtils.join(",", Arrays.asList(MyService1a.class.getName()));

    conf = new Configuration(false);
    conf.set("server.services", services);
    conf.set("server.services.ext", servicesExt);
    server = new Server("server", dir, dir, dir, dir, conf);
    server.init();

    assertThat(server.get(MyService1.class).getClass()).isEqualTo(MyService1a.class);
    assertThat(ORDER.size()).isEqualTo(4);
    assertThat(ORDER.get(0)).isEqualTo("s1a.init");
    assertThat(ORDER.get(1)).isEqualTo("s3.init");
    assertThat(ORDER.get(2)).isEqualTo("s1a.postInit");
    assertThat(ORDER.get(3)).isEqualTo("s3.postInit");
    server.destroy();
    assertThat(ORDER.size()).isEqualTo(6);
    assertThat(ORDER.get(4)).isEqualTo("s3.destroy");
    assertThat(ORDER.get(5)).isEqualTo("s1a.destroy");
    // service override via setService
    ORDER.clear();
    services = StringUtils.join(",", Arrays.asList(MyService1.class.getName(), MyService3.class.getName()));
    conf = new Configuration(false);
    conf.set("server.services", services);
    server = new Server("server", dir, dir, dir, dir, conf);
    server.init();

    server.setService(MyService1a.class);
    assertThat(ORDER.size()).isEqualTo(6);
    assertThat(ORDER.get(4)).isEqualTo("s1.destroy");
    assertThat(ORDER.get(5)).isEqualTo("s1a.init");

    assertThat(server.get(MyService1.class).getClass()).isEqualTo(MyService1a.class);

    server.destroy();
    assertThat(ORDER.size()).isEqualTo(8);
    assertThat(ORDER.get(6)).isEqualTo("s3.destroy");
    assertThat(ORDER.get(7)).isEqualTo("s1a.destroy");

    // service add via setService
    ORDER.clear();
    services = StringUtils.join(",", Arrays.asList(MyService1.class.getName(), MyService3.class.getName()));
    conf = new Configuration(false);
    conf.set("server.services", services);
    server = new Server("server", dir, dir, dir, dir, conf);
    server.init();

    server.setService(MyService5.class);
    assertThat(ORDER.size()).isEqualTo(5);
    assertThat(ORDER.get(4)).isEqualTo("s5.init");

    assertThat(server.get(MyService5.class).getClass()).isEqualTo(MyService5.class);

    server.destroy();
    assertThat(ORDER.size()).isEqualTo(8);
    assertThat(ORDER.get(5)).isEqualTo("s5.destroy");
    assertThat(ORDER.get(6)).isEqualTo("s3.destroy");
    assertThat(ORDER.get(7)).isEqualTo("s1.destroy");

    // service add via setService exception
    ORDER.clear();
    services = StringUtils.join(",", Arrays.asList(MyService1.class.getName(), MyService3.class.getName()));
    conf = new Configuration(false);
    conf.set("server.services", services);
    server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    try {
      server.setService(MyService7.class);
      fail();
    } catch (ServerException ex) {
      assertThat(ex.getError()).isEqualTo(ServerException.ERROR.S09);
    } catch (Exception ex) {
      fail();
    }
    assertThat(ORDER.size()).isEqualTo(6);
    assertThat(ORDER.get(4)).isEqualTo("s3.destroy");
    assertThat(ORDER.get(5)).isEqualTo("s1.destroy");

    // service with dependency
    ORDER.clear();
    services = StringUtils.join(",", Arrays.asList(MyService1.class.getName(), MyService6.class.getName()));
    conf = new Configuration(false);
    conf.set("server.services", services);
    server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    assertThat(server.get(MyService1.class).getInterface()).isEqualTo(MyService1.class);
    assertThat(server.get(MyService6.class).getInterface()).isEqualTo(MyService6.class);

    server.destroy();
  }

  /**
   * Creates an absolute path by appending the given relative path to the test
   * root.
   * 
   * @param relativePath String relative path
   * @return String absolute path formed by appending relative path to test root
   */
  private static String getAbsolutePath(String relativePath) {
    return new File(TestDirHelper.getTestDir(), relativePath).getAbsolutePath();
  }
}
