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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilePermission;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.AllPermission;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.Path.SEPARATOR;
import static org.apache.hadoop.util.Shell.SYSPROP_HADOOP_HOME_DIR;
import static org.apache.hadoop.yarn.api.ApplicationConstants.Environment.JAVA_HOME;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.YARN_CONTAINER_SANDBOX;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.YARN_CONTAINER_SANDBOX_POLICY_GROUP_PREFIX;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_ID_STR;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_LOCAL_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_RUN_CMDS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.LOCALIZED_RESOURCES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.USER;
/**
 * <p>This class extends the {@link DefaultLinuxContainerRuntime} specifically
 * for containers which run Java commands.  It generates a new java security
 * policy file per container and modifies the java command to enable the
 * Java Security Manager with the generated policy.</p>
 *
 * The behavior of the {@link JavaSandboxLinuxContainerRuntime} can be modified
 * using the following settings:
 *
 * <ul>
 *   <li>
 *     {@value YarnConfiguration#YARN_CONTAINER_SANDBOX} :
 *     This yarn-site.xml setting has three options:
 *     <ul>
 *     <li>disabled - Default behavior. {@link LinuxContainerRuntime}
 *     is disabled</li>
 *     <li>permissive - JVM containers will run with Java Security Manager
 *     enabled.  Non-JVM containers will run normally</li>
 *     <li>enforcing - JVM containers will run with Java Security Manager
 *     enabled.  Non-JVM containers will be prevented from executing and an
 *     {@link ContainerExecutionException} will be thrown.</li>
 *     </ul>
 *   </li>
 *   <li>
 *     {@value YarnConfiguration#YARN_CONTAINER_SANDBOX_FILE_PERMISSIONS} :
 *     Determines the file permissions for the application directories.  The
 *     permissions come in the form of comma separated values
 *     (e.g. read,write,execute,delete). Defaults to {@code read} for read-only.
 *   </li>
 *   <li>
 *     {@value YarnConfiguration#YARN_CONTAINER_SANDBOX_POLICY} :
 *     Accepts canonical path to a java policy file on the local filesystem.
 *     This file will be loaded as the base policy, any additional container
 *     grants will be appended to this base file.  If not specified, the default
 *     java.policy file provided with hadoop resources will be used.
 *   </li>
 *   <li>
 *     {@value YarnConfiguration#YARN_CONTAINER_SANDBOX_WHITELIST_GROUP} :
 *     Optional setting to specify a YARN queue which will be exempt from the
 *     sand-boxing process.
 *   </li>
 *   <li>
 *     {@value
 *     YarnConfiguration#YARN_CONTAINER_SANDBOX_POLICY_GROUP_PREFIX}$groupName :
 *     Optional setting to map groups to java policy files.  The value is a path
 *     to the java policy file for $groupName.  A user which is a member of
 *     multiple groups with different policies will receive the superset of all
 *     the permissions across their groups.
 *   </li>
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JavaSandboxLinuxContainerRuntime
    extends DefaultLinuxContainerRuntime {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultLinuxContainerRuntime.class);
  private Configuration configuration;
  private SandboxMode sandboxMode;

  public static final String POLICY_FILE_DIR = "nm-sandbox-policies";

  private static Path policyFileDir;
  private static final FileAttribute<Set<PosixFilePermission>> POLICY_ATTR =
      PosixFilePermissions.asFileAttribute(
          PosixFilePermissions.fromString("rwxr-xr-x"));

  private Map<String, Path> containerPolicies = new HashMap<>();

  /**
   * Create an instance using the given {@link PrivilegedOperationExecutor}
   * instance for performing operations.
   *
   * @param privilegedOperationExecutor the {@link PrivilegedOperationExecutor}
   * instance
   */
  public JavaSandboxLinuxContainerRuntime(
      PrivilegedOperationExecutor privilegedOperationExecutor) {
    super(privilegedOperationExecutor);
  }

  @Override
  public void initialize(Configuration conf)
      throws ContainerExecutionException {
    this.configuration = conf;
    this.sandboxMode =
        SandboxMode.get(
            this.configuration.get(YARN_CONTAINER_SANDBOX,
                YarnConfiguration.DEFAULT_YARN_CONTAINER_SANDBOX));

    super.initialize(conf);
  }

  /**
   * Initialize the Java Security Policy directory.  Either creates the
   * directory if it doesn't exist, or clears the contents of the directory if
   * already created.
   * @throws ContainerExecutionException If unable to resolve policy directory
   */
  private void initializePolicyDir() throws ContainerExecutionException {
    String hadoopTempDir = configuration.get("hadoop.tmp.dir");
    if (hadoopTempDir == null) {
      throw new ContainerExecutionException("hadoop.tmp.dir not set!");
    }
    policyFileDir = Paths.get(hadoopTempDir, POLICY_FILE_DIR);
    //Delete any existing policy files if the directory has already been created
    if(Files.exists(policyFileDir)){
      try (DirectoryStream<Path> stream =
         Files.newDirectoryStream(policyFileDir)){
        for(Path policyFile : stream){
          Files.delete(policyFile);
        }
      }catch(IOException e){
        throw new ContainerExecutionException("Unable to initialize policy "
            + "directory: " + e);
      }
    } else {
      try {
        policyFileDir = Files.createDirectories(
            Paths.get(hadoopTempDir, POLICY_FILE_DIR), POLICY_ATTR);
      } catch (IOException e) {
        throw new ContainerExecutionException("Unable to create policy file " +
            "directory: " + e);
      }
    }
  }

  /**
   *  Prior to environment from being written locally need to generate
   *  policy file which limits container access to a small set of directories.
   *  Additionally the container run command needs to be modified to include
   *  flags to enable the java security manager with the generated policy.
   *  <br>
   *  The Java Sandbox will be circumvented if the user is a member of the
   *  group specified in:
   *  {@value YarnConfiguration#YARN_CONTAINER_SANDBOX_WHITELIST_GROUP} and if
   *  they do not include the JVM flag:
   *  {@value NMContainerPolicyUtils#SECURITY_FLAG}
   *
   * @param ctx The {@link ContainerRuntimeContext} containing container
   *            setup properties.
   * @throws ContainerExecutionException Exception thrown if temporary policy
   * file directory can't be created, or if any exceptions occur during policy
   * file parsing and generation.
   */
  @Override
  public void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {

    @SuppressWarnings("unchecked")
    List<String> localDirs =
        ctx.getExecutionAttribute(CONTAINER_LOCAL_DIRS);
    @SuppressWarnings("unchecked")
    Map<org.apache.hadoop.fs.Path, List<String>> resources =
        ctx.getExecutionAttribute(LOCALIZED_RESOURCES);
    @SuppressWarnings("unchecked")
    List<String> commands =
        ctx.getExecutionAttribute(CONTAINER_RUN_CMDS);
    Map<String, String> env =
        ctx.getContainer().getLaunchContext().getEnvironment();
    String username =
        ctx.getExecutionAttribute(USER);

    if(!isSandboxContainerWhitelisted(username, commands)) {
      String tmpDirBase = configuration.get("hadoop.tmp.dir");
      if (tmpDirBase == null) {
        throw new ContainerExecutionException("hadoop.tmp.dir not set!");
      }

      OutputStream policyOutputStream = null;
      try {
        String containerID = ctx.getExecutionAttribute(CONTAINER_ID_STR);
        initializePolicyDir();

        List<String> groupPolicyFiles =
            getGroupPolicyFiles(configuration, ctx.getExecutionAttribute(USER));
        Path policyFilePath = Files.createFile(
            Paths.get(policyFileDir.toString(),
            containerID + "-" + NMContainerPolicyUtils.POLICY_FILE),
            POLICY_ATTR);
        policyOutputStream = Files.newOutputStream(policyFilePath);

        containerPolicies.put(containerID, policyFilePath);

        NMContainerPolicyUtils.generatePolicyFile(policyOutputStream,
            localDirs, groupPolicyFiles, resources, configuration);
        NMContainerPolicyUtils.appendSecurityFlags(
            commands, env, policyFilePath, sandboxMode);

      } catch (IOException e) {
        throw new ContainerExecutionException(e);
      } finally {
        IOUtils.cleanupWithLogger(LOG, policyOutputStream);
      }
    }
  }

  @Override
  public void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    try {
      super.launchContainer(ctx);
    } finally {
      deletePolicyFiles(ctx);
    }
  }

  /**
   * Determine if JVMSandboxLinuxContainerRuntime should be used.  This is
   * decided based on the value of
   * {@value YarnConfiguration#YARN_CONTAINER_SANDBOX}
   * @return true if Sandbox is requested, false otherwise
   */
  boolean isSandboxContainerRequested() {
    return sandboxMode != SandboxMode.disabled;
  }

  private static List<String> getGroupPolicyFiles(Configuration conf,
      String user) throws ContainerExecutionException {
    Groups groups = Groups.getUserToGroupsMappingService(conf);
    List<String> userGroups;
    try {
      userGroups = groups.getGroups(user);
    } catch (IOException e) {
      throw new ContainerExecutionException("Container user does not exist");
    }

    return userGroups.stream()
        .map(group -> conf.get(YARN_CONTAINER_SANDBOX_POLICY_GROUP_PREFIX
            + group))
        .filter(groupPolicy -> groupPolicy != null)
        .collect(Collectors.toList());
  }

  /**
   * Determine if the container should be whitelisted (i.e. exempt from the
   * Java Security Manager).
   * @param username The name of the user running the container
   * @param commands The list of run commands for the container
   * @return boolean value denoting whether the container should be whitelisted.
   * @throws ContainerExecutionException If container user can not be resolved
   */
  private boolean isSandboxContainerWhitelisted(String username,
      List<String> commands) throws ContainerExecutionException {
    String whitelistGroup = configuration.get(
        YarnConfiguration.YARN_CONTAINER_SANDBOX_WHITELIST_GROUP);
    Groups groups = Groups.getUserToGroupsMappingService(configuration);
    List<String> userGroups;
    boolean isWhitelisted = false;

    try {
      userGroups = groups.getGroups(username);
    } catch (IOException e) {
      throw new ContainerExecutionException("Container user does not exist");
    }

    if(whitelistGroup != null && userGroups.contains(whitelistGroup)) {
      // If any command has security flag, whitelisting is disabled
      for(String cmd : commands) {
        if(cmd.contains(NMContainerPolicyUtils.SECURITY_FLAG)){
          isWhitelisted = false;
          break;
        } else {
          isWhitelisted = true;
        }
      }
    }
    return isWhitelisted;
  }

  /**
   * Deletes policy files for container specified by parameter.  Additionally
   * this method will age off any stale policy files generated by
   * {@link JavaSandboxLinuxContainerRuntime}
   * @param ctx Container context for files to be deleted
   * @throws ContainerExecutionException if unable to access or delete policy
   * files or generated policy file directory
   */
  private void deletePolicyFiles(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    try {
      Files.delete(containerPolicies.remove(
          ctx.getExecutionAttribute(CONTAINER_ID_STR)));
    } catch (IOException e) {
      throw new ContainerExecutionException("Unable to delete policy file: "
          + e);
    }
  }

  /**
   * Enumeration of the modes the JavaSandboxLinuxContainerRuntime can use.
   * See {@link JavaSandboxLinuxContainerRuntime} for details on the
   * behavior of each setting.
   */
  public enum SandboxMode {
    enforcing("enforcing"),
    permissive("permissive"),
    disabled("disabled");

    private final String mode;
    SandboxMode(String mode){
      this.mode = mode;
    }

    public static SandboxMode get(String mode) {

      if(enforcing.mode.equals(mode)) {
        return enforcing;
      } else if(permissive.mode.equals(mode)) {
        return permissive;
      } else {
        return disabled;
      }
    }

    public String toString(){
      return mode;
    }
  }

  /**
   * Static utility class defining String constants and static methods for the
   * use of the {@link JavaSandboxLinuxContainerRuntime}.
   */
  static final class NMContainerPolicyUtils{

    static final String POLICY_FILE = "java.policy";
    static final String SECURITY_DEBUG = " -Djava.security.debug=all";
    static final String SECURITY_FLAG = "-Djava.security.manager";
    static final String POLICY_APPEND_FLAG = "-Djava.security.policy=";
    static final String POLICY_FLAG = POLICY_APPEND_FLAG + "=";
    static final String JAVA_CMD = "/bin/java ";
    static final String JVM_SECURITY_CMD =
        JAVA_CMD + SECURITY_FLAG + " " + POLICY_FLAG;

    static final String STRIP_POLICY_FLAG = POLICY_APPEND_FLAG + "[^ ]+";
    static final String CONTAINS_JAVA_CMD = "\\$" + JAVA_HOME + JAVA_CMD + ".*";
    static final String MULTI_COMMAND_REGEX =
        "(?s).*(" + //command read as single line
        "(&[^>]|&&)|(\\|{1,2})|(\\|&)|" + //Matches '&','&&','|','||' and '|&'
        "(`[^`]+`)|(\\$\\([^)]+\\))|" + //Matches occurrences of $() or ``
        "(;)" + //Matches end of statement ';'
        ").*";
    static final String CLEAN_CMD_REGEX =
        "(" + SECURITY_FLAG + ")|" +
            "(" + STRIP_POLICY_FLAG + ")";

    static final String FILE_PERMISSION_FORMAT = "   permission "
        + FilePermission.class.getCanonicalName()
        + " \"%1$s" + SEPARATOR + "-\", \"%2$s\";%n";
    static final String HADOOP_HOME_PERMISSION = "%ngrant codeBase \"file:"
        + Paths.get(System.getProperty(SYSPROP_HADOOP_HOME_DIR))
        + SEPARATOR + "-\" {%n" +
        "  permission " + AllPermission.class.getCanonicalName() + ";%n};%n";
    static final Logger LOG =
            LoggerFactory.getLogger(NMContainerPolicyUtils.class);

    /**
     * Write new policy file to policyOutStream which will include read access
     * to localize resources.  Optionally a default policyFilePath can be
     * specified to append a custom policy implementation to the new policy file
     * @param policyOutStream OutputStream pointing to java.policy file
     * @param localDirs Container local directories
     * @param resources List of local container resources
     * @param conf YARN configuration
     * @throws IOException - If policy file generation is unable to read the
     * base policy file or if it is unable to create a new policy file.
     */
    static void generatePolicyFile(OutputStream policyOutStream,
        List<String> localDirs, List<String> groupPolicyPaths,
        Map<org.apache.hadoop.fs.Path, List<String>> resources,
        Configuration conf)
        throws IOException {

      String policyFilePath =
          conf.get(YarnConfiguration.YARN_CONTAINER_SANDBOX_POLICY);
      String filePermissions =
          conf.get(YarnConfiguration.YARN_CONTAINER_SANDBOX_FILE_PERMISSIONS,
            YarnConfiguration.DEFAULT_YARN_CONTAINER_SANDBOX_FILE_PERMISSIONS);

      Set<String> cacheDirs = new HashSet<>();
      for(org.apache.hadoop.fs.Path path : resources.keySet()) {
        cacheDirs.add(path.getParent().toString());
      }

      if (groupPolicyPaths != null) {
        for(String policyPath : groupPolicyPaths) {
          Files.copy(Paths.get(policyPath), policyOutStream);
        }
      } else if (policyFilePath == null) {
        IOUtils.copyBytes(
            NMContainerPolicyUtils.class.getResourceAsStream("/" + POLICY_FILE),
            policyOutStream, conf, false);
      } else {
        Files.copy(Paths.get(policyFilePath), policyOutStream);
      }

      Formatter filePermissionFormat = new Formatter(policyOutStream,
          StandardCharsets.UTF_8.name());
      filePermissionFormat.format(HADOOP_HOME_PERMISSION);
      filePermissionFormat.format("grant {%n");
      for(String localDir : localDirs) {
        filePermissionFormat.format(
            FILE_PERMISSION_FORMAT, localDir, filePermissions);
      }
      for(String cacheDir : cacheDirs) {
        filePermissionFormat.format(
            FILE_PERMISSION_FORMAT, cacheDir, filePermissions);
      }
      filePermissionFormat.format("};%n");
      filePermissionFormat.flush();
    }

    /**
     * Modify command to enable the Java Security Manager and specify
     * java.policy file.  Will modify the passed commands to strip any
     * existing java security configurations.  Expects a java command to be the
     * first and only executable provided in enforcing mode.  In passive mode
     * any commands with '||' or '&&' will not be modified.
     * @param commands List of container commands
     * @param env Container environment variables
     * @param policyPath Path to the container specific policy file
     * @param sandboxMode (enforcing, permissive, disabled) Determines
     *          whether non-java containers will be launched
     * @throws ContainerExecutionException - Exception thrown if
     * JVM Sandbox enabled in 'enforcing' mode and a non-java command is
     * provided in the list of commands
     */
    static void appendSecurityFlags(List<String> commands,
        Map<String, String> env, Path policyPath, SandboxMode sandboxMode)
        throws ContainerExecutionException {

      for(int i = 0; i < commands.size(); i++){
        String command = commands.get(i);
        if(validateJavaHome(env.get(JAVA_HOME.name()))
            && command.matches(CONTAINS_JAVA_CMD)
            && !command.matches(MULTI_COMMAND_REGEX)){
          command = command.replaceAll(CLEAN_CMD_REGEX, "");
          String securityString = JVM_SECURITY_CMD + policyPath + " ";
          if(LOG.isDebugEnabled()) {
            securityString += SECURITY_DEBUG;
          }
          commands.set(i, command.replaceFirst(JAVA_CMD, securityString));
        } else if (sandboxMode == SandboxMode.enforcing){
          throw new ContainerExecutionException(
              "Only JVM containers permitted in YARN sandbox mode (enforcing). "
            + "The following command can not be executed securely: " + command);
        } else if (sandboxMode == SandboxMode.permissive){
          LOG.warn("The container will run without the java security manager"
              + " due to an unsupported container command.  The command"
              + " will be permitted to run in Sandbox permissive mode: "
              + command);
        }
      }
    }

    private static boolean validateJavaHome(String containerJavaHome)
        throws ContainerExecutionException{
      if (System.getenv(JAVA_HOME.name()) == null) {
        throw new ContainerExecutionException(
            "JAVA_HOME is not set for NodeManager");
      }
      if (containerJavaHome == null) {
        throw new ContainerExecutionException(
            "JAVA_HOME is not set for container");
      }
      return System.getenv(JAVA_HOME.name()).equals(containerJavaHome);
    }
  }
}
