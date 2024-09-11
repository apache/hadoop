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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.client.cli.ApplicationCLI;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.conf.ExampleAppJson;
import org.apache.hadoop.yarn.service.conf.YarnServiceConstants;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.hadoop.yarn.client.api.AppAdminClient.YARN_APP_ADMIN_CLIENT_PREFIX;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.DEPENDENCY_TARBALL_PATH;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.YARN_SERVICE_BASE_PATH;
import static org.apache.hadoop.yarn.service.exceptions.LauncherExitCodes.EXIT_SUCCESS;
import static org.apache.hadoop.yarn.service.exceptions.LauncherExitCodes.EXIT_UNAUTHORIZED;
import static org.mockito.Mockito.spy;

public class TestServiceCLI {
  private static final Logger LOG = LoggerFactory.getLogger(TestServiceCLI
      .class);

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private Configuration conf = new YarnConfiguration();
  private SliderFileSystem fs;
  private ApplicationCLI cli;
  private File basedir;
  private String basedirProp;
  private File dependencyTarGzBaseDir;
  private Path dependencyTarGz;
  private String dependencyTarGzProp;
  private String yarnAdminNoneAclProp;
  private String dfsAdminAclProp;

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

  private int runCLI(String[] args) throws Exception {
    LOG.info("running CLI: yarn {}", Arrays.asList(args));
    return ToolRunner.run(cli, ApplicationCLI.preProcessArgs(args));
  }

  private void buildApp(String serviceName, String appDef) throws Throwable {
    String[] args = {"app",
        "-D", basedirProp, "-save", serviceName,
        ExampleAppJson.resourceName(appDef),
        "-appTypes", DUMMY_APP_TYPE};
    Assert.assertEquals(EXIT_SUCCESS, runCLI(args));
  }

  private void buildApp(String serviceName, String appDef,
      String lifetime, String queue) throws Throwable {
    String[] args = {"app",
        "-D", basedirProp, "-save", serviceName,
        ExampleAppJson.resourceName(appDef),
        "-appTypes", DUMMY_APP_TYPE,
        "-updateLifetime", lifetime,
        "-changeQueue", queue};
    Assert.assertEquals(EXIT_SUCCESS, runCLI(args));
  }

  private static Path getDependencyTarGz(File dir) {
    return new Path(new File(dir, YarnServiceConstants
        .DEPENDENCY_TAR_GZ_FILE_NAME + YarnServiceConstants
        .DEPENDENCY_TAR_GZ_FILE_EXT).getAbsolutePath());
  }

  @Before
  public void setup() throws Throwable {
    basedir = new File("target", "apps");
    basedirProp = YARN_SERVICE_BASE_PATH + "=" + basedir.getAbsolutePath();
    conf.set(YARN_SERVICE_BASE_PATH, basedir.getAbsolutePath());
    fs = new SliderFileSystem(conf);
    dependencyTarGzBaseDir = tmpFolder.getRoot();
    fs.getFileSystem()
        .setPermission(new Path(dependencyTarGzBaseDir.getAbsolutePath()),
            new FsPermission("755"));
    dependencyTarGz = getDependencyTarGz(dependencyTarGzBaseDir);
    dependencyTarGzProp = DEPENDENCY_TARBALL_PATH + "=" + dependencyTarGz
        .toString();
    conf.set(DEPENDENCY_TARBALL_PATH, dependencyTarGz.toString());

    if (basedir.exists()) {
      FileUtils.deleteDirectory(basedir);
    } else {
      basedir.mkdirs();
    }
    yarnAdminNoneAclProp = YarnConfiguration.YARN_ADMIN_ACL + "=none";
    dfsAdminAclProp = DFSConfigKeys.DFS_ADMIN + "=" +
        UserGroupInformation.getCurrentUser();
    System.setProperty(YarnServiceConstants.PROPERTY_LIB_DIR, basedir
        .getAbsolutePath());
    createCLI();
  }

  @After
  public void tearDown() throws IOException {
    if (basedir != null) {
      FileUtils.deleteDirectory(basedir);
    }
    cli.stop();
  }

  @Test (timeout = 180000)
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
    assertThat(result).isEqualTo(0);
  }

  @Test (timeout = 180000)
  public void testInitiateAutoFinalizeServiceUpgrade() throws Exception {
    String[] args =  {"app", "-upgrade", "app-1",
        "-initiate", ExampleAppJson.resourceName(ExampleAppJson.APP_JSON),
        "-autoFinalize",
        "-appTypes", DUMMY_APP_TYPE};
    int result = cli.run(ApplicationCLI.preProcessArgs(args));
    assertThat(result).isEqualTo(0);
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
    assertThat(result).isEqualTo(0);
  }

  @Test
  public void testUpgradeComponents() throws Exception {
    conf.set(YARN_APP_ADMIN_CLIENT_PREFIX + DUMMY_APP_TYPE,
        DummyServiceClient.class.getName());
    cli.setConf(conf);
    String[] args = {"app", "-upgrade", "app-1",
        "-components", "comp1,comp2",
        "-appTypes", DUMMY_APP_TYPE};
    int result = cli.run(ApplicationCLI.preProcessArgs(args));
    assertThat(result).isEqualTo(0);
  }

  @Test
  public void testGetInstances() throws Exception {
    conf.set(YARN_APP_ADMIN_CLIENT_PREFIX + DUMMY_APP_TYPE,
        DummyServiceClient.class.getName());
    cli.setConf(conf);
    String[] args = {"container", "-list", "app-1",
        "-components", "comp1,comp2",
        "-appTypes", DUMMY_APP_TYPE};
    int result = cli.run(ApplicationCLI.preProcessArgs(args));
    assertThat(result).isEqualTo(0);
  }

  @Test
  public void testCancelUpgrade() throws Exception {
    conf.set(YARN_APP_ADMIN_CLIENT_PREFIX + DUMMY_APP_TYPE,
        DummyServiceClient.class.getName());
    cli.setConf(conf);
    String[] args = {"app", "-upgrade", "app-1",
        "-cancel", "-appTypes", DUMMY_APP_TYPE};
    int result = cli.run(ApplicationCLI.preProcessArgs(args));
    assertThat(result).isEqualTo(0);
  }

  @Test (timeout = 180000)
  public void testEnableFastLaunch() throws Exception {
    fs.getFileSystem().create(new Path(basedir.getAbsolutePath(), "test.jar"))
        .close();

    Path defaultPath = new Path(dependencyTarGz.toString());
    Assert.assertFalse("Dependency tarball should not exist before the test",
        fs.isFile(defaultPath));
    String[] args = {"app", "-D", dependencyTarGzProp, "-enableFastLaunch",
        "-appTypes", DUMMY_APP_TYPE};
    Assert.assertEquals(EXIT_SUCCESS, runCLI(args));
    Assert.assertTrue("Dependency tarball did not exist after the test",
        fs.isFile(defaultPath));

    File secondBaseDir = new File(dependencyTarGzBaseDir, "2");
    Path secondTarGz = getDependencyTarGz(secondBaseDir);
    Assert.assertFalse("Dependency tarball should not exist before the test",
        fs.isFile(secondTarGz));
    String[] args2 = {"app", "-D", yarnAdminNoneAclProp, "-D",
        dfsAdminAclProp, "-D", dependencyTarGzProp, "-enableFastLaunch",
        secondBaseDir.getAbsolutePath(), "-appTypes", DUMMY_APP_TYPE};
    Assert.assertEquals(EXIT_SUCCESS, runCLI(args2));
    Assert.assertTrue("Dependency tarball did not exist after the test",
        fs.isFile(secondTarGz));
  }

  @Test (timeout = 180000)
  public void testEnableFastLaunchUserPermissions() throws Exception {
    String[] args = {"app", "-D", yarnAdminNoneAclProp, "-D",
        dependencyTarGzProp, "-enableFastLaunch", "-appTypes", DUMMY_APP_TYPE};
    Assert.assertEquals(EXIT_UNAUTHORIZED, runCLI(args));
  }

  @Test (timeout = 180000)
  public void testEnableFastLaunchFilePermissions() throws Exception {
    File badDir = new File(dependencyTarGzBaseDir, "bad");
    badDir.mkdir();
    fs.getFileSystem().setPermission(new Path(badDir.getAbsolutePath()),
        new FsPermission("751"));

    String[] args = {"app", "-D", dependencyTarGzProp, "-enableFastLaunch",
        badDir.getAbsolutePath(), "-appTypes", DUMMY_APP_TYPE};
    Assert.assertEquals(EXIT_UNAUTHORIZED, runCLI(args));

    badDir = new File(badDir, "child");
    badDir.mkdir();
    fs.getFileSystem().setPermission(new Path(badDir.getAbsolutePath()),
        new FsPermission("755"));

    String[] args2 = {"app", "-D", dependencyTarGzProp, "-enableFastLaunch",
        badDir.getAbsolutePath(), "-appTypes", DUMMY_APP_TYPE};
    Assert.assertEquals(EXIT_UNAUTHORIZED, runCLI(args2));

    badDir = new File(dependencyTarGzBaseDir, "badx");
    badDir.mkdir();
    fs.getFileSystem().setPermission(new Path(badDir.getAbsolutePath()),
        new FsPermission("754"));

    String[] args3 = {"app", "-D", dependencyTarGzProp, "-enableFastLaunch",
        badDir.getAbsolutePath(), "-appTypes", DUMMY_APP_TYPE};
    Assert.assertEquals(EXIT_UNAUTHORIZED, runCLI(args3));
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

    @Override
    public int actionUpgradeComponents(String appName, List<String> components)
        throws IOException, YarnException {
      return 0;
    }

    @Override
    public String getInstances(String appName, List<String> components,
        String version, List<String> containerStates)
        throws IOException, YarnException {
      return "";
    }

    @Override
    public int actionCancelUpgrade(String appName) throws IOException,
        YarnException {
      return 0;
    }
  }
}
