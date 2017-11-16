/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.
    containermanager.linux.runtime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilePermission;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketPermission;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.yarn.api.ApplicationConstants.Environment.JAVA_HOME;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.JavaSandboxLinuxContainerRuntime.NMContainerPolicyUtils.MULTI_COMMAND_REGEX;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.JavaSandboxLinuxContainerRuntime.NMContainerPolicyUtils.CLEAN_CMD_REGEX;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.JavaSandboxLinuxContainerRuntime.NMContainerPolicyUtils.CONTAINS_JAVA_CMD;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.JavaSandboxLinuxContainerRuntime.NMContainerPolicyUtils.POLICY_APPEND_FLAG;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.JavaSandboxLinuxContainerRuntime.NMContainerPolicyUtils.POLICY_FILE;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.JavaSandboxLinuxContainerRuntime.NMContainerPolicyUtils.POLICY_FLAG;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.JavaSandboxLinuxContainerRuntime.NMContainerPolicyUtils.SECURITY_FLAG;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.JavaSandboxLinuxContainerRuntime.POLICY_FILE_DIR;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.APPID;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_ID_STR;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_LOCAL_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_RUN_CMDS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_WORK_DIR;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.FILECACHE_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.LOCALIZED_RESOURCES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.LOCAL_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.LOG_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.RUN_AS_USER;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.USER;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.USER_LOCAL_DIRS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test policy file generation and policy enforcement for the
 * {@link JavaSandboxLinuxContainerRuntime}.
 */
public class TestJavaSandboxLinuxContainerRuntime {

  private final static String HADOOP_HOME = "hadoop.home.dir";
  private final static String HADOOP_HOME_DIR = System.getProperty(HADOOP_HOME);
  private final Properties baseProps = new Properties(System.getProperties());

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private static File grantFile, denyFile, policyFile,
          grantDir, denyDir, containerDir;
  private static java.nio.file.Path policyFilePath;
  private static SecurityManager securityManager;
  private Map<Path, List<String>> resources;
  private Map<String, String> env;
  private List<String> whitelistGroup;

  private PrivilegedOperationExecutor mockExecutor;
  private JavaSandboxLinuxContainerRuntime runtime;
  private ContainerRuntimeContext.Builder runtimeContextBuilder;
  private Configuration conf;

  private final static String NORMAL_USER = System.getProperty("user.name");
  private final static String NORMAL_GROUP = "normalGroup";
  private final static String WHITELIST_USER = "picard";
  private final static String WHITELIST_GROUP = "captains";
  private final static String CONTAINER_ID = "container_1234567890";
  private final static String APPLICATION_ID = "application_1234567890";
  private File baseTestDirectory;

  @Before
  public void setup() throws Exception {

    baseTestDirectory = new File(System.getProperty("test.build.data",
        System.getProperty("java.io.tmpdir", "target")),
        TestJavaSandboxLinuxContainerRuntime.class.getName());

    whitelistGroup = new ArrayList<>();
    whitelistGroup.add(WHITELIST_GROUP);

    conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES,
        WHITELIST_USER + "=" + WHITELIST_GROUP + "," + NORMAL_GROUP + ";"
            + NORMAL_USER + "=" + NORMAL_GROUP + ";");
    conf.set("hadoop.tmp.dir", baseTestDirectory.getAbsolutePath());

    Files.deleteIfExists(Paths.get(baseTestDirectory.getAbsolutePath(),
        POLICY_FILE_DIR, CONTAINER_ID + "-" + POLICY_FILE));

    mockExecutor = mock(PrivilegedOperationExecutor.class);
    runtime = new JavaSandboxLinuxContainerRuntime(mockExecutor);
    runtime.initialize(conf, null);

    resources = new HashMap<>();
    grantDir = new File(baseTestDirectory, "grantDir");
    denyDir = new File(baseTestDirectory, "denyDir");
    containerDir = new File(baseTestDirectory,
        APPLICATION_ID + Path.SEPARATOR + CONTAINER_ID);
    grantDir.mkdirs();
    denyDir.mkdirs();
    containerDir.mkdirs();

    grantFile = File.createTempFile("grantFile", "tmp", grantDir);
    denyFile = File.createTempFile("denyFile", "tmp", denyDir);

    List<String> symLinks = new ArrayList<>();
    symLinks.add(grantFile.getName());
    resources.put(new Path(grantFile.getCanonicalPath()), symLinks);

    env = new HashMap();
    env.put(JAVA_HOME.name(), System.getenv(JAVA_HOME.name()));

    policyFile = File.createTempFile("java", "policy", containerDir);
    policyFilePath = Paths.get(policyFile.getAbsolutePath());

    runtimeContextBuilder = createRuntimeContext();

    if (HADOOP_HOME_DIR == null) {
      System.setProperty(HADOOP_HOME, policyFile.getParent());
    }

    OutputStream outStream = new FileOutputStream(policyFile);
    JavaSandboxLinuxContainerRuntime.NMContainerPolicyUtils
        .generatePolicyFile(outStream, symLinks, null, resources, conf);
    outStream.close();

    System.setProperty("java.security.policy", policyFile.getCanonicalPath());
    securityManager = new SecurityManager();
  }

  public ContainerRuntimeContext.Builder createRuntimeContext(){

    Container container = mock(Container.class);
    ContainerLaunchContext  ctx = mock(ContainerLaunchContext.class);

    when(container.getLaunchContext()).thenReturn(ctx);
    when(ctx.getEnvironment()).thenReturn(env);

    ContainerRuntimeContext.Builder builder =
        new ContainerRuntimeContext.Builder(container);

    List<String> localDirs = new ArrayList<>();

    builder.setExecutionAttribute(LOCALIZED_RESOURCES, resources)
        .setExecutionAttribute(RUN_AS_USER, NORMAL_USER)
        .setExecutionAttribute(CONTAINER_ID_STR, CONTAINER_ID)
        .setExecutionAttribute(APPID, APPLICATION_ID)
        .setExecutionAttribute(CONTAINER_WORK_DIR,
            new Path(containerDir.toString()))
        .setExecutionAttribute(LOCAL_DIRS, localDirs)
        .setExecutionAttribute(LOG_DIRS, localDirs)
        .setExecutionAttribute(FILECACHE_DIRS, localDirs)
        .setExecutionAttribute(USER_LOCAL_DIRS, localDirs)
        .setExecutionAttribute(CONTAINER_LOCAL_DIRS, localDirs)
        .setExecutionAttribute(CONTAINER_RUN_CMDS, localDirs);

    return builder;
  }

  public static final String SOCKET_PERMISSION_FORMAT =
      "grant { \n" +
      "   permission %1s \"%2s\", \"%3s\";\n" +
      "};\n";
  public static final String RUNTIME_PERMISSION_FORMAT =
      "grant { \n" +
          "   permission %1s \"%2s\";\n" +
          "};\n";

  @Test
  public void testGroupPolicies()
      throws IOException, ContainerExecutionException {
    // Generate new policy files each containing one grant
    File openSocketPolicyFile =
        File.createTempFile("openSocket", "policy", baseTestDirectory);
    File classLoaderPolicyFile =
        File.createTempFile("createClassLoader", "policy", baseTestDirectory);
    Permission socketPerm = new SocketPermission("localhost:0", "listen");
    Permission runtimePerm = new RuntimePermission("createClassLoader");

    StringBuilder socketPermString = new StringBuilder();
    Formatter openSocketPolicyFormatter = new Formatter(socketPermString);
    openSocketPolicyFormatter.format(SOCKET_PERMISSION_FORMAT,
        socketPerm.getClass().getName(), socketPerm.getName(),
        socketPerm.getActions());
    FileWriter socketPermWriter = new FileWriter(openSocketPolicyFile);
    socketPermWriter.write(socketPermString.toString());
    socketPermWriter.close();

    StringBuilder classLoaderPermString = new StringBuilder();
    Formatter classLoaderPolicyFormatter = new Formatter(classLoaderPermString);
    classLoaderPolicyFormatter.format(RUNTIME_PERMISSION_FORMAT,
        runtimePerm.getClass().getName(), runtimePerm.getName());
    FileWriter classLoaderPermWriter = new FileWriter(classLoaderPolicyFile);
    classLoaderPermWriter.write(classLoaderPermString.toString());
    classLoaderPermWriter.close();

    conf.set(YarnConfiguration.YARN_CONTAINER_SANDBOX_POLICY_GROUP_PREFIX +
        WHITELIST_GROUP, openSocketPolicyFile.toString());
    conf.set(YarnConfiguration.YARN_CONTAINER_SANDBOX_POLICY_GROUP_PREFIX +
        NORMAL_GROUP, classLoaderPolicyFile.toString());

    String[] inputCommand = {"$JAVA_HOME/bin/java jar MyJob.jar"};
    List<String> commands = Arrays.asList(inputCommand);

    runtimeContextBuilder.setExecutionAttribute(USER, WHITELIST_USER);
    runtimeContextBuilder.setExecutionAttribute(CONTAINER_RUN_CMDS, commands);

    runtime.prepareContainer(runtimeContextBuilder.build());

    //pull generated policy from cmd
    Matcher policyMatches = Pattern.compile(POLICY_APPEND_FLAG + "=?([^ ]+)")
        .matcher(commands.get(0));
    policyMatches.find();
    String generatedPolicy = policyMatches.group(1);

    //Test that generated policy file has included both policies
    Assert.assertTrue(
        Files.readAllLines(Paths.get(generatedPolicy)).contains(
            classLoaderPermString.toString().split("\n")[1]));
    Assert.assertTrue(
        Files.readAllLines(Paths.get(generatedPolicy)).contains(
            socketPermString.toString().split("\n")[1]));
  }

  @Test
  public void testGrant() throws Exception {
    FilePermission grantPermission =
        new FilePermission(grantFile.getAbsolutePath(), "read");
    securityManager.checkPermission(grantPermission);
  }

  @Test
  public void testDeny() throws Exception {
    FilePermission denyPermission =
        new FilePermission(denyFile.getAbsolutePath(), "read");
    exception.expect(java.security.AccessControlException.class);
    securityManager.checkPermission(denyPermission);
  }

  @Test
  public void testEnforcingMode() throws ContainerExecutionException {
    String[] nonJavaCommands = {
        "bash malicious_script.sh",
        "python malicious_script.py"
    };

    List<String> commands = Arrays.asList(nonJavaCommands);
    exception.expect(ContainerExecutionException.class);
    JavaSandboxLinuxContainerRuntime.NMContainerPolicyUtils
        .appendSecurityFlags(commands, env, policyFilePath,
            JavaSandboxLinuxContainerRuntime.SandboxMode.enforcing);

  }

  @Test
  public void testPermissiveMode() throws ContainerExecutionException {
    String[] nonJavaCommands = {
        "bash non-java-script.sh",
        "python non-java-script.py"
    };

    List<String> commands = Arrays.asList(nonJavaCommands);
    JavaSandboxLinuxContainerRuntime.NMContainerPolicyUtils
        .appendSecurityFlags(commands, env, policyFilePath,
              JavaSandboxLinuxContainerRuntime.SandboxMode.permissive);
  }

  @Test
  public void testDisabledSandboxWithWhitelist()
      throws ContainerExecutionException {

    String[] inputCommand = {
        "java jar MyJob.jar"
    };
    List<String> commands = Arrays.asList(inputCommand);

    conf.set(YarnConfiguration.YARN_CONTAINER_SANDBOX_WHITELIST_GROUP,
        WHITELIST_GROUP);

    runtimeContextBuilder.setExecutionAttribute(USER, WHITELIST_USER);
    runtimeContextBuilder.setExecutionAttribute(CONTAINER_RUN_CMDS, commands);
    runtime.prepareContainer(runtimeContextBuilder.build());

    Assert.assertTrue("Command should not be modified when user is " +
            "member of whitelisted group",
        inputCommand[0].equals(commands.get(0)));
  }

  @Test
  public void testEnabledSandboxWithWhitelist()
      throws ContainerExecutionException{
    String[] inputCommand = {
        "$JAVA_HOME/bin/java jar -Djava.security.manager MyJob.jar"
    };
    List<String> commands = Arrays.asList(inputCommand);

    conf.set(YarnConfiguration.YARN_CONTAINER_SANDBOX_WHITELIST_GROUP,
        WHITELIST_GROUP);

    runtimeContextBuilder.setExecutionAttribute(USER, WHITELIST_USER);
    runtimeContextBuilder.setExecutionAttribute(CONTAINER_RUN_CMDS, commands);
    runtime.prepareContainer(runtimeContextBuilder.build());

    Assert.assertTrue("Command should be modified to include " +
            "policy file in whitelisted Sandbox mode",
        commands.get(0).contains(SECURITY_FLAG)
        && commands.get(0).contains(POLICY_FLAG));
  }

  @Test
  public void testDeniedWhitelistGroup() throws ContainerExecutionException {

    String[] inputCommand = {
        "$JAVA_HOME/bin/java jar MyJob.jar"
    };
    List<String> commands = Arrays.asList(inputCommand);

    conf.set(YarnConfiguration.YARN_CONTAINER_SANDBOX_WHITELIST_GROUP,
        WHITELIST_GROUP);

    runtimeContextBuilder.setExecutionAttribute(USER, NORMAL_USER);
    runtimeContextBuilder.setExecutionAttribute(CONTAINER_RUN_CMDS, commands);
    runtime.prepareContainer(runtimeContextBuilder.build());

    Assert.assertTrue("Java security manager must be enabled for "
            + "unauthorized users",
        commands.get(0).contains(SECURITY_FLAG));
  }

  @Test
  public void testChainedCmdRegex(){
    String[] multiCmds = {
        "cmd1 && cmd2",
        "cmd1 || cmd2",
        "cmd1 `cmd2`",
        "cmd1 $(cmd2)",
        "cmd1; \\\n cmd2",
        "cmd1; cmd2",
        "cmd1|&cmd2",
        "cmd1|cmd2",
        "cmd1&cmd2"
    };

    Arrays.stream(multiCmds)
        .forEach(cmd -> Assert.assertTrue(cmd.matches(MULTI_COMMAND_REGEX)));
    Assert.assertFalse("cmd1 &> logfile".matches(MULTI_COMMAND_REGEX));
  }

  @Test
  public void testContainsJavaRegex(){
    String[] javaCmds = {
        "$JAVA_HOME/bin/java -cp App.jar AppClass",
        "$JAVA_HOME/bin/java -jar App.jar AppClass &> logfile"
    };
    String[] nonJavaCmds = {
        "$JAVA_HOME/bin/jajavava -cp App.jar AppClass",
        "/nm/app/container/usercache/badjava -cp Bad.jar ChaosClass"
    };
    for(String javaCmd : javaCmds) {
      Assert.assertTrue(javaCmd.matches(CONTAINS_JAVA_CMD));
    }
    for(String nonJavaCmd : nonJavaCmds) {
      Assert.assertFalse(nonJavaCmd.matches(CONTAINS_JAVA_CMD));
    }
  }

  @Test
  public void testCleanCmdRegex(){
    String[] securityManagerCmds = {
        "/usr/bin/java -Djava.security.manager -cp $CLASSPATH $MainClass",
        "-Djava.security.manager -Djava.security.policy==testpolicy keepThis"
    };
    String[] cleanedCmdsResult = {
        "/usr/bin/java  -cp $CLASSPATH $MainClass",
        "keepThis"
    };
    for(int i = 0; i < securityManagerCmds.length; i++){
      Assert.assertEquals(
          securityManagerCmds[i].replaceAll(CLEAN_CMD_REGEX, "").trim(),
          cleanedCmdsResult[i]);
    }
  }

  @Test
  public void testAppendSecurityFlags() throws ContainerExecutionException {
    String securityString = "-Djava.security.manager -Djava.security.policy=="
        + policyFile.getAbsolutePath();
    String[] badCommands = {
        "$JAVA_HOME/bin/java -Djava.security.manager "
            + "-Djava.security.policy=/home/user/java.policy",
        "$JAVA_HOME/bin/java -cp MyApp.jar MrAppMaster"
    };
    String[] cleanCommands = {
        "$JAVA_HOME/bin/java " + securityString,
        "$JAVA_HOME/bin/java " + securityString
            + " -cp MyApp.jar MrAppMaster"
    };

    List<String> commands = Arrays.asList(badCommands);
    JavaSandboxLinuxContainerRuntime.NMContainerPolicyUtils
        .appendSecurityFlags(commands, env, policyFilePath,
            JavaSandboxLinuxContainerRuntime.SandboxMode.enforcing);

    for(int i = 0; i < commands.size(); i++) {
      Assert.assertTrue(commands.get(i).trim().equals(cleanCommands[i].trim()));
    }
  }

  @After
  public void cleanup(){
    System.setProperties(baseProps);
  }
}