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

package org.apache.slider.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Files;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.KerberosDiags;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationTimeout;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Times;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.api.SliderApplicationApi;
import org.apache.slider.api.SliderClusterProtocol;
import org.apache.slider.api.StateValues;
import org.apache.slider.api.proto.Messages;
import org.apache.slider.api.types.ContainerInformation;
import org.apache.slider.api.types.NodeInformationList;
import org.apache.slider.api.types.SliderInstanceDescription;
import org.apache.slider.client.ipc.SliderApplicationIpcClient;
import org.apache.slider.client.ipc.SliderClusterOperations;
import org.apache.slider.common.Constants;
import org.apache.slider.common.SliderExitCodes;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.params.AbstractActionArgs;
import org.apache.slider.common.params.AbstractClusterBuildingActionArgs;
import org.apache.slider.common.params.ActionAMSuicideArgs;
import org.apache.slider.common.params.ActionClientArgs;
import org.apache.slider.common.params.ActionCreateArgs;
import org.apache.slider.common.params.ActionDependencyArgs;
import org.apache.slider.common.params.ActionDestroyArgs;
import org.apache.slider.common.params.ActionDiagnosticArgs;
import org.apache.slider.common.params.ActionEchoArgs;
import org.apache.slider.common.params.ActionExistsArgs;
import org.apache.slider.common.params.ActionFlexArgs;
import org.apache.slider.common.params.ActionFreezeArgs;
import org.apache.slider.common.params.ActionInstallKeytabArgs;
import org.apache.slider.common.params.ActionInstallPackageArgs;
import org.apache.slider.common.params.ActionKDiagArgs;
import org.apache.slider.common.params.ActionKeytabArgs;
import org.apache.slider.common.params.ActionKillContainerArgs;
import org.apache.slider.common.params.ActionListArgs;
import org.apache.slider.common.params.ActionLookupArgs;
import org.apache.slider.common.params.ActionNodesArgs;
import org.apache.slider.common.params.ActionPackageArgs;
import org.apache.slider.common.params.ActionRegistryArgs;
import org.apache.slider.common.params.ActionResolveArgs;
import org.apache.slider.common.params.ActionResourceArgs;
import org.apache.slider.common.params.ActionStatusArgs;
import org.apache.slider.common.params.ActionThawArgs;
import org.apache.slider.common.params.ActionTokensArgs;
import org.apache.slider.common.params.ActionUpgradeArgs;
import org.apache.slider.common.params.Arguments;
import org.apache.slider.common.params.ClientArgs;
import org.apache.slider.common.params.CommonArgs;
import org.apache.slider.common.params.LaunchArgsAccessor;
import org.apache.slider.common.tools.ConfigHelper;
import org.apache.slider.common.tools.Duration;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.common.tools.SliderVersionInfo;
import org.apache.slider.core.buildutils.InstanceBuilder;
import org.apache.slider.core.buildutils.InstanceIO;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.conf.ResourcesInputPropertiesValidator;
import org.apache.slider.core.conf.TemplateInputPropertiesValidator;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.NoSuchNodeException;
import org.apache.slider.core.exceptions.NotFoundException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException;
import org.apache.slider.core.exceptions.UsageException;
import org.apache.slider.core.exceptions.WaitTimeoutException;
import org.apache.slider.core.launch.AppMasterLauncher;
import org.apache.slider.core.launch.ClasspathConstructor;
import org.apache.slider.core.launch.CredentialUtils;
import org.apache.slider.core.launch.JavaCommandLineBuilder;
import org.apache.slider.core.launch.LaunchedApplication;
import org.apache.slider.core.launch.SerializedApplicationReport;
import org.apache.slider.core.main.RunService;
import org.apache.slider.core.persist.AppDefinitionPersister;
import org.apache.slider.core.persist.ApplicationReportSerDeser;
import org.apache.slider.core.persist.ConfPersister;
import org.apache.slider.core.persist.JsonSerDeser;
import org.apache.slider.core.persist.LockAcquireFailedException;
import org.apache.slider.core.registry.SliderRegistryUtils;
import org.apache.slider.core.registry.YarnAppListClient;
import org.apache.slider.core.registry.docstore.ConfigFormat;
import org.apache.slider.core.registry.docstore.PublishedConfigSet;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.core.registry.docstore.PublishedExports;
import org.apache.slider.core.registry.docstore.PublishedExportsOutputter;
import org.apache.slider.core.registry.docstore.PublishedExportsSet;
import org.apache.slider.core.registry.retrieve.RegistryRetriever;
import org.apache.slider.core.zk.BlockingZKWatcher;
import org.apache.slider.core.zk.ZKIntegration;
import org.apache.slider.core.zk.ZKPathBuilder;
import org.apache.slider.providers.AbstractClientProvider;
import org.apache.slider.providers.SliderProviderFactory;
import org.apache.slider.providers.agent.AgentKeys;
import org.apache.slider.providers.docker.DockerClientProvider;
import org.apache.slider.providers.slideram.SliderAMClientProvider;
import org.apache.slider.server.appmaster.SliderAppMaster;
import org.apache.slider.server.appmaster.rpc.RpcBinder;
import org.apache.slider.server.services.utility.AbstractSliderLaunchedService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.registry.client.binding.RegistryUtils.*;
import static org.apache.slider.api.InternalKeys.*;
import static org.apache.slider.api.OptionKeys.*;
import static org.apache.slider.api.ResourceKeys.*;
import static org.apache.slider.common.Constants.HADOOP_JAAS_DEBUG;
import static org.apache.slider.common.params.SliderActions.*;
import static org.apache.slider.common.tools.SliderUtils.*;


/**
 * Client service for Slider
 */

public class SliderClient extends AbstractSliderLaunchedService implements RunService,
    SliderExitCodes, SliderKeys, ErrorStrings, SliderClientAPI {
  private static final Logger log = LoggerFactory.getLogger(SliderClient.class);
  public static final String E_MUST_BE_A_VALID_JSON_FILE
      = "Invalid configuration. Must be a valid json file.";
  public static final String E_INVALID_INSTALL_LOCATION
      = "A valid install location must be provided for the client.";
  public static final String E_UNABLE_TO_READ_SUPPLIED_PACKAGE_FILE
      = "Unable to read supplied package file";
  public static final String E_INVALID_APPLICATION_PACKAGE_LOCATION
      = "A valid application package location required.";
  public static final String E_INVALID_INSTALL_PATH = "Install path is not a valid directory";
  public static final String E_INSTALL_PATH_DOES_NOT_EXIST = "Install path does not exist";
  public static final String E_INVALID_APPLICATION_TYPE_NAME
      = "A valid application type name is required (e.g. HBASE).";
  public static final String E_USE_REPLACEPKG_TO_OVERWRITE = "Use --replacepkg to overwrite.";
  public static final String E_PACKAGE_DOES_NOT_EXIST = "Package does not exist";
  public static final String E_NO_ZOOKEEPER_QUORUM = "No Zookeeper quorum defined";
  public static final String E_NO_RESOURCE_MANAGER = "No valid Resource Manager address provided";
  public static final String E_PACKAGE_EXISTS = "Package exists";
  private static PrintStream clientOutputStream = System.out;

  // value should not be changed without updating string find in slider.py
  private static final String PASSWORD_PROMPT = "Enter password for";

  private ClientArgs serviceArgs;
  public ApplicationId applicationId;
  
  private String deployedClusterName;
  /**
   * Cluster operations against the deployed cluster -will be null
   * if no bonding has yet taken place
   */
  private SliderClusterOperations sliderClusterOperations;

  protected SliderFileSystem sliderFileSystem;

  /**
   * Yarn client service
   */
  private SliderYarnClientImpl yarnClient;
  private YarnAppListClient yarnAppListClient;
  private AggregateConf launchedInstanceDefinition;

  /**
   * The YARN registry service
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private RegistryOperations registryOperations;

  /**
   * Constructor
   */
  public SliderClient() {
    super("Slider Client");
    new HdfsConfiguration();
    new YarnConfiguration();
  }

  /**
   * This is called <i>Before serviceInit is called</i>
   * @param config the initial configuration build up by the
   * service launcher.
   * @param args argument list list of arguments passed to the command line
   * after any launcher-specific commands have been stripped.
   * @return the post-binding configuration to pass to the <code>init()</code>
   * operation.
   * @throws Exception
   */
  @Override
  public Configuration bindArgs(Configuration config, String... args) throws Exception {
    config = super.bindArgs(config, args);
    serviceArgs = new ClientArgs(args);
    serviceArgs.parse();
    // add the slider XML config
    ConfigHelper.injectSliderXMLResource();
    // yarn-ify
    YarnConfiguration yarnConfiguration = new YarnConfiguration(config);
    return patchConfiguration(yarnConfiguration);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    Configuration clientConf = loadSliderClientXML();
    ConfigHelper.mergeConfigurations(conf, clientConf, SLIDER_CLIENT_XML, true);
    serviceArgs.applyDefinitions(conf);
    serviceArgs.applyFileSystemBinding(conf);
    AbstractActionArgs coreAction = serviceArgs.getCoreAction();
    // init security with our conf
    if (!coreAction.disableSecureLogin() && isHadoopClusterSecure(conf)) {
      forceLogin();
      initProcessSecurity(conf);
    }
    if (coreAction.getHadoopServicesRequired()) {
      initHadoopBinding();
    }
    super.serviceInit(conf);
  }

  /**
   * Launched service execution. This runs {@link #exec()}
   * then catches some exceptions and converts them to exit codes
   * @return an exit code
   * @throws Throwable
   */
  @Override
  public int runService() throws Throwable {
    try {
      return exec();
    } catch (FileNotFoundException | PathNotFoundException nfe) {
      throw new NotFoundException(nfe, nfe.toString());
    }
  }

  /**
   * Execute the command line
   * @return an exit code
   * @throws Throwable on a failure
   */
  public int exec() throws Throwable {

    // choose the action
    String action = serviceArgs.getAction();
    if (isUnset(action)) {
      throw new SliderException(EXIT_USAGE, serviceArgs.usage());
    }

    int exitCode = EXIT_SUCCESS;
    String clusterName = serviceArgs.getClusterName();
    // actions

    switch (action) {
      case ACTION_AM_SUICIDE:
        exitCode = actionAmSuicide(clusterName,
            serviceArgs.getActionAMSuicideArgs());
        break;
      
      case ACTION_BUILD:
        exitCode = actionBuild(clusterName, serviceArgs.getActionBuildArgs());
        break;
      
      case ACTION_CLIENT:
        exitCode = actionClient(serviceArgs.getActionClientArgs());
        break;

      case ACTION_CREATE:
        exitCode = actionCreate(clusterName, serviceArgs.getActionCreateArgs());
        break;

      case ACTION_DEPENDENCY:
        exitCode = actionDependency(serviceArgs.getActionDependencyArgs());
        break;

      case ACTION_DESTROY:
        exitCode = actionDestroy(clusterName, serviceArgs.getActionDestroyArgs());
        break;

      case ACTION_DIAGNOSTICS:
        exitCode = actionDiagnostic(serviceArgs.getActionDiagnosticArgs());
        break;
      
      case ACTION_EXISTS:
        exitCode = actionExists(clusterName,
            serviceArgs.getActionExistsArgs());
        break;
      
      case ACTION_FLEX:
        exitCode = actionFlex(clusterName, serviceArgs.getActionFlexArgs());
        break;
      
      case ACTION_FREEZE:
        exitCode = actionFreeze(clusterName, serviceArgs.getActionFreezeArgs());
        break;
      
      case ACTION_HELP:
        log.info(serviceArgs.usage());
        break;

      case ACTION_KDIAG:
        exitCode = actionKDiag(serviceArgs.getActionKDiagArgs());
        break;

      case ACTION_KILL_CONTAINER:
        exitCode = actionKillContainer(clusterName,
            serviceArgs.getActionKillContainerArgs());
        break;

      case ACTION_INSTALL_KEYTAB:
        exitCode = actionInstallKeytab(serviceArgs.getActionInstallKeytabArgs());
        break;
      
      case ACTION_INSTALL_PACKAGE:
        exitCode = actionInstallPkg(serviceArgs.getActionInstallPackageArgs());
        break;

      case ACTION_KEYTAB:
        exitCode = actionKeytab(serviceArgs.getActionKeytabArgs());
        break;

      case ACTION_LIST:
        exitCode = actionList(clusterName, serviceArgs.getActionListArgs());
        break;

      case ACTION_LOOKUP:
        exitCode = actionLookup(serviceArgs.getActionLookupArgs());
        break;

      case ACTION_NODES:
        exitCode = actionNodes("", serviceArgs.getActionNodesArgs());
        break;

      case ACTION_PACKAGE:
        exitCode = actionPackage(serviceArgs.getActionPackageArgs());
        break;

      case ACTION_REGISTRY:
        exitCode = actionRegistry(serviceArgs.getActionRegistryArgs());
        break;
      
      case ACTION_RESOLVE:
        exitCode = actionResolve(serviceArgs.getActionResolveArgs());
        break;

      case ACTION_RESOURCE:
        exitCode = actionResource(serviceArgs.getActionResourceArgs());
        break;

      case ACTION_STATUS:
        exitCode = actionStatus(clusterName, serviceArgs.getActionStatusArgs());
        break;

      case ACTION_THAW:
        exitCode = actionThaw(clusterName, serviceArgs.getActionThawArgs());
        break;

      case ACTION_TOKENS:
        exitCode = actionTokens(serviceArgs.getActionTokenArgs());
        break;

      case ACTION_UPDATE:
        exitCode = actionUpdate(clusterName, serviceArgs.getActionUpdateArgs());
        break;

      case ACTION_UPGRADE:
        exitCode = actionUpgrade(clusterName, serviceArgs.getActionUpgradeArgs());
        break;

      case ACTION_VERSION:
        exitCode = actionVersion();
        break;
      
      default:
        throw new SliderException(EXIT_UNIMPLEMENTED,
            "Unimplemented: " + action);
    }
   
    return exitCode;
  }

  /**
   * Perform everything needed to init the hadoop binding.
   * This assumes that the service is already  in inited or started state
   * @throws IOException
   * @throws SliderException
   */
  protected void initHadoopBinding() throws IOException, SliderException {
    // validate the client
    validateSliderClientEnvironment(null);
    //create the YARN client
    yarnClient = new SliderYarnClientImpl();
    yarnClient.init(getConfig());
    if (getServiceState() == STATE.STARTED) {
      yarnClient.start();
    }
    addService(yarnClient);
    yarnAppListClient =
        new YarnAppListClient(yarnClient, getUsername(), getConfig());
    // create the filesystem
    sliderFileSystem = new SliderFileSystem(getConfig());
  }

  /**
   * Delete the zookeeper node associated with the calling user and the cluster
   * TODO: YARN registry operations
   **/
  @VisibleForTesting
  public boolean deleteZookeeperNode(String clusterName) throws YarnException, IOException {
    String user = getUsername();
    String zkPath = ZKIntegration.mkClusterPath(user, clusterName);
    Exception e = null;
    try {
      Configuration config = getConfig();
      ZKIntegration client = getZkClient(clusterName, user);
      if (client != null) {
        if (client.exists(zkPath)) {
          log.info("Deleting zookeeper path {}", zkPath);
        }
        client.deleteRecursive(zkPath);
        return true;
      }
    } catch (InterruptedException | BadConfigException | KeeperException ex) {
      e = ex;
    }
    if (e != null) {
      log.warn("Unable to recursively delete zk node {}", zkPath, e);
    }

    return false;
  }

  /**
   * Create the zookeeper node associated with the calling user and the cluster
   *
   * @param clusterName slider application name
   * @param nameOnly should the name only be created (i.e. don't create ZK node)
   * @return the path, using the policy implemented in
   *   {@link ZKIntegration#mkClusterPath(String, String)}
   * @throws YarnException
   * @throws IOException
   */
  @VisibleForTesting
  public String createZookeeperNode(String clusterName, Boolean nameOnly) throws YarnException, IOException {
    try {
      return createZookeeperNodeInner(clusterName, nameOnly);
    } catch (KeeperException.NodeExistsException e) {
      return null;
    } catch (KeeperException e) {
      return null;
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.toString());
    }
  }

  /**
   * Create the zookeeper node associated with the calling user and the cluster
   * -throwing exceptions on any failure
   * @param clusterName cluster name
   * @param nameOnly create the path, not the node
   * @return the path, using the policy implemented in
   *   {@link ZKIntegration#mkClusterPath(String, String)}
   * @throws YarnException
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  @VisibleForTesting
  public String createZookeeperNodeInner(String clusterName, Boolean nameOnly)
      throws YarnException, IOException, KeeperException, InterruptedException {
    String user = getUsername();
    String zkPath = ZKIntegration.mkClusterPath(user, clusterName);
    if (nameOnly) {
      return zkPath;
    }
    ZKIntegration client = getZkClient(clusterName, user);
    if (client != null) {
      // set up the permissions. This must be done differently on a secure cluster from an insecure
      // one
      List<ACL> zkperms = new ArrayList<>();
      if (UserGroupInformation.isSecurityEnabled()) {
        zkperms.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS));
        zkperms.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));
      } else {
        zkperms.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE));
      }
      client.createPath(zkPath, "",
          zkperms,
          CreateMode.PERSISTENT);
      return zkPath;
    } else {
      return null;
    }
  }

  /**
   * Gets a zookeeper client, returns null if it cannot connect to zookeeper
   **/
  protected ZKIntegration getZkClient(String clusterName, String user) throws YarnException {
    String registryQuorum = lookupZKQuorum();
    ZKIntegration client = null;
    try {
      BlockingZKWatcher watcher = new BlockingZKWatcher();
      client = ZKIntegration.newInstance(registryQuorum, user, clusterName, true, false, watcher,
          ZKIntegration.SESSION_TIMEOUT);
      boolean fromCache = client.init();
      if (!fromCache) {
        watcher.waitForZKConnection(2 * 1000);
      }
    } catch (InterruptedException e) {
      client = null;
      log.warn("Interrupted - unable to connect to zookeeper quorum {}",
          registryQuorum, e);
    } catch (IOException e) {
      log.warn("Unable to connect to zookeeper quorum {}", registryQuorum, e);
    }
    return client;
  }

  /**
   * Keep this signature for backward compatibility with
   * force=true by default.
   */
  @Override
  public int actionDestroy(String clustername) throws YarnException,
                                                      IOException {
    ActionDestroyArgs destroyArgs = new ActionDestroyArgs();
    destroyArgs.force = true;
    return actionDestroy(clustername, destroyArgs);
  }

  @Override
  public int actionDestroy(String clustername,
      ActionDestroyArgs destroyArgs) throws YarnException, IOException {
    // verify that a live cluster isn't there
    validateClusterName(clustername);
    //no=op, it is now mandatory. 
    verifyBindingsDefined();
    verifyNoLiveClusters(clustername, "Destroy");
    boolean forceDestroy = destroyArgs.force;
    log.debug("actionDestroy({}, force={})", clustername, forceDestroy);

    // create the directory path
    Path clusterDirectory = sliderFileSystem.buildClusterDirPath(clustername);
    // delete the directory;
    FileSystem fs = sliderFileSystem.getFileSystem();
    boolean exists = fs.exists(clusterDirectory);
    if (exists) {
      log.debug("Application Instance {} found at {}: destroying", clustername, clusterDirectory);
      if (!forceDestroy) {
        // fail the command if --force is not explicitly specified
        throw new UsageException("Destroy will permanently delete directories and registries. "
            + "Reissue this command with the --force option if you want to proceed.");
      }
      if (!fs.delete(clusterDirectory, true)) {
        log.warn("Filesystem returned false from delete() operation");
      }

      if(!deleteZookeeperNode(clustername)) {
        log.warn("Unable to perform node cleanup in Zookeeper.");
      }

      if (fs.exists(clusterDirectory)) {
        log.warn("Failed to delete {}", clusterDirectory);
      }

    } else {
      log.debug("Application Instance {} already destroyed", clustername);
    }

    // rm the registry entry —do not let this block the destroy operations
    String registryPath = SliderRegistryUtils.registryPathForInstance(
        clustername);
    try {
      getRegistryOperations().delete(registryPath, true);
    } catch (IOException e) {
      log.warn("Error deleting registry entry {}: {} ", registryPath, e, e);
    } catch (SliderException e) {
      log.warn("Error binding to registry {} ", e, e);
    }

    List<ApplicationReport> instances = findAllLiveInstances(clustername);
    // detect any race leading to cluster creation during the check/destroy process
    // and report a problem.
    if (!instances.isEmpty()) {
      throw new SliderException(EXIT_APPLICATION_IN_USE,
                              clustername + ": "
                              + E_DESTROY_CREATE_RACE_CONDITION
                              + " :" +
                              instances.get(0));
    }
    log.info("Destroyed cluster {}", clustername);
    return EXIT_SUCCESS;
  }
  
  @Override
  public int actionAmSuicide(String clustername,
      ActionAMSuicideArgs args) throws YarnException, IOException {
    SliderClusterOperations clusterOperations =
      createClusterOperations(clustername);
    clusterOperations.amSuicide(args.message, args.exitcode, args.waittime);
    return EXIT_SUCCESS;
  }

  @Override
  public AbstractClientProvider createClientProvider(String provider)
    throws SliderException {
    SliderProviderFactory factory =
      SliderProviderFactory.createSliderProviderFactory(provider);
    return factory.createClientProvider();
  }

  /**
   * Create the cluster -saving the arguments to a specification file first
   * @param clustername cluster name
   * @return the status code
   * @throws YarnException Yarn problems
   * @throws IOException other problems
   * @throws BadCommandArgumentsException bad arguments.
   */
  public int actionCreate(String clustername, ActionCreateArgs createArgs) throws
                                               YarnException,
                                               IOException {

    actionBuild(clustername, createArgs);
    Path clusterDirectory = sliderFileSystem.buildClusterDirPath(clustername);
    AggregateConf instanceDefinition = loadInstanceDefinitionUnresolved(
        clustername, clusterDirectory);
    try {
      checkForCredentials(getConfig(), instanceDefinition.getAppConf(),
          clustername);
    } catch (IOException e) {
      sliderFileSystem.getFileSystem().delete(clusterDirectory, true);
      throw e;
    }
    return startCluster(clustername, createArgs, createArgs.lifetime);
  }

  @Override
  public int actionUpgrade(String clustername, ActionUpgradeArgs upgradeArgs)
      throws YarnException, IOException {
    File template = upgradeArgs.template;
    File resources = upgradeArgs.resources;
    List<String> containers = upgradeArgs.containers;
    List<String> components = upgradeArgs.components;

    // For upgrade spec, let's be little more strict with validation. If either
    // --template or --resources is specified, then both needs to be specified.
    // Otherwise the internal app config and resources states of the app will be
    // unwantedly modified and the change will take effect to the running app
    // immediately.
    require(!(template != null && resources == null),
          "Option %s must be specified with option %s",
          Arguments.ARG_RESOURCES, Arguments.ARG_TEMPLATE);

    require(!(resources != null && template == null),
          "Option %s must be specified with option %s",
          Arguments.ARG_TEMPLATE, Arguments.ARG_RESOURCES);

    // For upgrade spec, both --template and --resources should be specified
    // and neither of --containers or --components should be used
    if (template != null && resources != null) {
      require(CollectionUtils.isEmpty(containers),
            "Option %s cannot be specified with %s or %s",
            Arguments.ARG_CONTAINERS, Arguments.ARG_TEMPLATE,
            Arguments.ARG_RESOURCES);
      require(CollectionUtils.isEmpty(components),
              "Option %s cannot be specified with %s or %s",
              Arguments.ARG_COMPONENTS, Arguments.ARG_TEMPLATE,
              Arguments.ARG_RESOURCES);

      // not an error to try to upgrade a stopped cluster, just return success
      // code, appropriate log messages have already been dumped
      if (!isAppInRunningState(clustername)) {
        return EXIT_SUCCESS;
      }

      // Now initiate the upgrade spec flow
      buildInstanceDefinition(clustername, upgradeArgs, true, true, true);
      SliderClusterOperations clusterOperations = createClusterOperations(clustername);
      clusterOperations.amSuicide("AM restarted for application upgrade", 1, 1000);
      return EXIT_SUCCESS;
    }

    // Since neither --template or --resources were specified, it is upgrade
    // containers flow. Here any one or both of --containers and --components
    // can be specified. If a container is specified with --containers option
    // and also belongs to a component type specified with --components, it will
    // be upgraded only once.
    return actionUpgradeContainers(clustername, upgradeArgs);
  }

  private int actionUpgradeContainers(String clustername,
      ActionUpgradeArgs upgradeArgs) throws YarnException, IOException {
    verifyBindingsDefined();
    validateClusterName(clustername);
    int waittime = upgradeArgs.getWaittime(); // ignored for now
    String text = "Upgrade containers";
    log.debug("actionUpgradeContainers({}, reason={}, wait={})", clustername,
        text, waittime);

    // not an error to try to upgrade a stopped cluster, just return success
    // code, appropriate log messages have already been dumped
    if (!isAppInRunningState(clustername)) {
      return EXIT_SUCCESS;
    }

    // Create sets of containers and components to get rid of duplicates and
    // for quick lookup during checks below
    Set<String> containers = new HashSet<>();
    if (upgradeArgs.containers != null) {
      containers.addAll(new ArrayList<>(upgradeArgs.containers));
    }
    Set<String> components = new HashSet<>();
    if (upgradeArgs.components != null) {
      components.addAll(new ArrayList<>(upgradeArgs.components));
    }

    // check validity of component names and running containers here
    List<ContainerInformation> liveContainers = getContainers(clustername);
    Set<String> validContainers = new HashSet<>();
    Set<String> validComponents = new HashSet<>();
    for (ContainerInformation liveContainer : liveContainers) {
      boolean allContainersAndComponentsAccountedFor = true;
      if (CollectionUtils.isNotEmpty(containers)) {
        if (containers.contains(liveContainer.containerId)) {
          containers.remove(liveContainer.containerId);
          validContainers.add(liveContainer.containerId);
        }
        allContainersAndComponentsAccountedFor = false;
      }
      if (CollectionUtils.isNotEmpty(components)) {
        if (components.contains(liveContainer.component)) {
          components.remove(liveContainer.component);
          validComponents.add(liveContainer.component);
        }
        allContainersAndComponentsAccountedFor = false;
      }
      if (allContainersAndComponentsAccountedFor) {
        break;
      }
    }

    // If any item remains in containers or components then they are invalid.
    // Log warning for them and proceed.
    if (CollectionUtils.isNotEmpty(containers)) {
      log.warn("Invalid set of containers provided {}", containers);
    }
    if (CollectionUtils.isNotEmpty(components)) {
      log.warn("Invalid set of components provided {}", components);
    }

    // If not a single valid container or component is specified do not proceed
    if (CollectionUtils.isEmpty(validContainers)
        && CollectionUtils.isEmpty(validComponents)) {
      log.error("Not a single valid container or component specified. Nothing to do.");
      return EXIT_NOT_FOUND;
    }

    SliderClusterProtocol appMaster = connect(findInstance(clustername));
    Messages.UpgradeContainersRequestProto r =
      Messages.UpgradeContainersRequestProto
              .newBuilder()
              .setMessage(text)
              .addAllContainer(validContainers)
              .addAllComponent(validComponents)
              .build();
    appMaster.upgradeContainers(r);
    log.info("Cluster upgrade issued for -");
    if (CollectionUtils.isNotEmpty(validContainers)) {
      log.info(" Containers (total {}): {}", validContainers.size(),
          validContainers);
    }
    if (CollectionUtils.isNotEmpty(validComponents)) {
      log.info(" Components (total {}): {}", validComponents.size(),
          validComponents);
    }

    return EXIT_SUCCESS;
  }

  // returns true if and only if app is in RUNNING state
  private boolean isAppInRunningState(String clustername) throws YarnException,
      IOException {
    // is this actually a known cluster?
    sliderFileSystem.locateInstanceDefinition(clustername);
    ApplicationReport app = findInstance(clustername);
    if (app == null) {
      // exit early
      log.info("Cluster {} not running", clustername);
      return false;
    }
    log.debug("App to upgrade was found: {}:\n{}", clustername,
        new OnDemandReportStringifier(app));
    if (app.getYarnApplicationState().ordinal() >= YarnApplicationState.FINISHED.ordinal()) {
      log.info("Cluster {} is in a terminated state {}. Use command '{}' instead.",
          clustername, app.getYarnApplicationState(), ACTION_UPDATE);
      return false;
    }

    // IPC request to upgrade containers is possible if the app is running.
    if (app.getYarnApplicationState().ordinal() < YarnApplicationState.RUNNING
        .ordinal()) {
      log.info("Cluster {} is in a pre-running state {}. To upgrade it needs "
          + "to be RUNNING.", clustername, app.getYarnApplicationState());
      return false;
    }

    return true;
  }

  protected static void checkForCredentials(Configuration conf,
      ConfTree tree, String clusterName) throws IOException {
    if (tree.credentials == null || tree.credentials.isEmpty()) {
      log.info("No credentials requested");
      return;
    }

    Console console = System.console();
    for (Entry<String, List<String>> cred : tree.credentials.entrySet()) {
      String provider = cred.getKey()
          .replaceAll(Pattern.quote("${CLUSTER_NAME}"), clusterName)
          .replaceAll(Pattern.quote("${CLUSTER}"), clusterName);
      List<String> aliases = cred.getValue();
      if (aliases == null || aliases.isEmpty()) {
        continue;
      }
      Configuration c = new Configuration(conf);
      c.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, provider);
      CredentialProvider credentialProvider = CredentialProviderFactory.getProviders(c).get(0);
      Set<String> existingAliases = new HashSet<>(credentialProvider.getAliases());
      for (String alias : aliases) {
        if (existingAliases.contains(alias.toLowerCase(Locale.ENGLISH))) {
          log.info("Credentials for " + alias + " found in " + provider);
        } else {
          if (console == null) {
            throw new IOException("Unable to input password for " + alias +
                " because System.console() is null; provider " + provider +
                " must be populated manually");
          }
          char[] pass = readPassword(alias, console);
          credentialProvider.createCredentialEntry(alias, pass);
          credentialProvider.flush();
          Arrays.fill(pass, ' ');
        }
      }
    }
  }

  private static char[] readOnePassword(String alias) throws IOException {
    Console console = System.console();
    if (console == null) {
      throw new IOException("Unable to input password for " + alias +
          " because System.console() is null");
    }
    return readPassword(alias, console);
  }

  private static char[] readPassword(String alias, Console console)
      throws IOException {
    char[] cred = null;

    boolean noMatch;
    do {
      console.printf("%s %s: \n", PASSWORD_PROMPT, alias);
      char[] newPassword1 = console.readPassword();
      console.printf("%s %s again: \n", PASSWORD_PROMPT, alias);
      char[] newPassword2 = console.readPassword();
      noMatch = !Arrays.equals(newPassword1, newPassword2);
      if (noMatch) {
        if (newPassword1 != null) Arrays.fill(newPassword1, ' ');
        log.info(String.format("Passwords don't match. Try again."));
      } else {
        cred = newPassword1;
      }
      if (newPassword2 != null) Arrays.fill(newPassword2, ' ');
    } while (noMatch);
    if (cred == null)
      throw new IOException("Could not read credentials for " + alias +
          " from stdin");
    return cred;
  }

  @Override
  public int actionBuild(String clustername,
      AbstractClusterBuildingActionArgs buildInfo) throws
                                               YarnException,
                                               IOException {

    buildInstanceDefinition(clustername, buildInfo, false, false);
    return EXIT_SUCCESS; 
  }

  @Override
  public int actionKeytab(ActionKeytabArgs keytabInfo)
      throws YarnException, IOException {
    if (keytabInfo.install) {
      return actionInstallKeytab(keytabInfo);
    } else if (keytabInfo.delete) {
      return actionDeleteKeytab(keytabInfo);
    } else if (keytabInfo.list) {
      return actionListKeytab(keytabInfo);
    } else {
      throw new BadCommandArgumentsException(
          "Keytab option specified not found.\n"
          + CommonArgs.usage(serviceArgs, ACTION_KEYTAB));
    }
  }

  private int actionListKeytab(ActionKeytabArgs keytabInfo) throws IOException {
    String folder = keytabInfo.folder != null ? keytabInfo.folder : StringUtils.EMPTY;
    Path keytabPath = sliderFileSystem.buildKeytabInstallationDirPath(folder);
    RemoteIterator<LocatedFileStatus> files =
        sliderFileSystem.getFileSystem().listFiles(keytabPath, true);
    log.info("Keytabs:");
    while (files.hasNext()) {
      log.info("\t" + files.next().getPath().toString());
    }

    return EXIT_SUCCESS;
  }

  private int actionDeleteKeytab(ActionKeytabArgs keytabInfo)
      throws BadCommandArgumentsException, IOException {
    if (StringUtils.isEmpty(keytabInfo.folder)) {
      throw new BadCommandArgumentsException(
          "A valid destination keytab sub-folder name is required (e.g. 'security').\n"
          + CommonArgs.usage(serviceArgs, ACTION_KEYTAB));
    }

    if (StringUtils.isEmpty(keytabInfo.keytab)) {
      throw new BadCommandArgumentsException("A keytab name is required.");
    }

    Path pkgPath = sliderFileSystem.buildKeytabInstallationDirPath(keytabInfo.folder);

    Path fileInFs = new Path(pkgPath, keytabInfo.keytab );
    log.info("Deleting keytab {}", fileInFs);
    FileSystem sfs = sliderFileSystem.getFileSystem();
    require(sfs.exists(fileInFs), "No keytab to delete found at %s",
        fileInFs.toUri());
    sfs.delete(fileInFs, false);

    return EXIT_SUCCESS;
  }

  private int actionInstallKeytab(ActionKeytabArgs keytabInfo)
      throws BadCommandArgumentsException, IOException {
    Path srcFile = null;
    require(isSet(keytabInfo.folder),
        "A valid destination keytab sub-folder name is required (e.g. 'security').\n"
        + CommonArgs.usage(serviceArgs, ACTION_KEYTAB));

    requireArgumentSet(Arguments.ARG_KEYTAB, keytabInfo.keytab);
    File keytabFile = new File(keytabInfo.keytab);
    require(keytabFile.isFile(),
        "Unable to access supplied keytab file at %s", keytabFile.getAbsolutePath());
    srcFile = new Path(keytabFile.toURI());

    Path pkgPath = sliderFileSystem.buildKeytabInstallationDirPath(keytabInfo.folder);
    FileSystem sfs = sliderFileSystem.getFileSystem();
    sfs.mkdirs(pkgPath);
    sfs.setPermission(pkgPath, new FsPermission(
        FsAction.ALL, FsAction.NONE, FsAction.NONE));

    Path fileInFs = new Path(pkgPath, srcFile.getName());
    log.info("Installing keytab {} at {} and overwrite is {}.",
        srcFile, fileInFs, keytabInfo.overwrite);
    require(!(sfs.exists(fileInFs) && !keytabInfo.overwrite),
        "Keytab exists at %s. Use --overwrite to overwrite.", fileInFs.toUri());

    sfs.copyFromLocalFile(false, keytabInfo.overwrite, srcFile, fileInFs);
    sfs.setPermission(fileInFs,
        new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));

    return EXIT_SUCCESS;
  }

  @Override
  public int actionInstallKeytab(ActionInstallKeytabArgs installKeytabInfo)
      throws YarnException, IOException {
    log.warn("The 'install-keytab' option has been deprecated.  Please use 'keytab --install'.");
    return actionKeytab(new ActionKeytabArgs(installKeytabInfo));
  }

  @Override
  public int actionInstallPkg(ActionInstallPackageArgs installPkgInfo) throws
      YarnException,
      IOException {
    log.warn("The " + ACTION_INSTALL_PACKAGE
        + " option has been deprecated. Please use '"
        + ACTION_PACKAGE + " " + ClientArgs.ARG_INSTALL + "'.");
    if (StringUtils.isEmpty(installPkgInfo.name)) {
      throw new BadCommandArgumentsException(
          E_INVALID_APPLICATION_TYPE_NAME + "\n"
              + CommonArgs.usage(serviceArgs, ACTION_INSTALL_PACKAGE));
    }
    Path srcFile = extractPackagePath(installPkgInfo.packageURI);

    // Do not provide new options to install-package command as it is in
    // deprecated mode. So version is kept null here. Use package --install.
    Path pkgPath = sliderFileSystem.buildPackageDirPath(installPkgInfo.name,
        null);
    FileSystem sfs = sliderFileSystem.getFileSystem();
    sfs.mkdirs(pkgPath);

    Path fileInFs = new Path(pkgPath, srcFile.getName());
    log.info("Installing package {} at {} and overwrite is {}.",
        srcFile, fileInFs, installPkgInfo.replacePkg);
    require(!(sfs.exists(fileInFs) && !installPkgInfo.replacePkg),
          "Package exists at %s. : %s", fileInFs.toUri(), E_USE_REPLACEPKG_TO_OVERWRITE);
    sfs.copyFromLocalFile(false, installPkgInfo.replacePkg, srcFile, fileInFs);
    return EXIT_SUCCESS;
  }

  @Override
  public int actionResource(ActionResourceArgs resourceInfo)
      throws YarnException, IOException {
    if (resourceInfo.help) {
      actionHelp(ACTION_RESOURCE);
      return EXIT_SUCCESS;
    } else if (resourceInfo.install) {
      return actionInstallResource(resourceInfo);
    } else if (resourceInfo.delete) {
      return actionDeleteResource(resourceInfo);
    } else if (resourceInfo.list) {
      return actionListResource(resourceInfo);
    } else {
      throw new BadCommandArgumentsException(
          "Resource option specified not found.\n"
              + CommonArgs.usage(serviceArgs, ACTION_RESOURCE));
    }
  }

  private int actionListResource(ActionResourceArgs resourceInfo) throws IOException {
    String folder = resourceInfo.folder != null ? resourceInfo.folder : StringUtils.EMPTY;
    Path path = sliderFileSystem.buildResourcePath(folder);
    RemoteIterator<LocatedFileStatus> files =
        sliderFileSystem.getFileSystem().listFiles(path, true);
    log.info("Resources:");
    while (files.hasNext()) {
      log.info("\t" + files.next().getPath().toString());
    }

    return EXIT_SUCCESS;
  }

  private int actionDeleteResource(ActionResourceArgs resourceInfo)
      throws BadCommandArgumentsException, IOException {
    if (StringUtils.isEmpty(resourceInfo.resource)) {
      throw new BadCommandArgumentsException("A file name is required.");
    }

    Path fileInFs;
    if (resourceInfo.folder == null) {
      fileInFs = sliderFileSystem.buildResourcePath(resourceInfo.resource);
    } else {
      fileInFs = sliderFileSystem.buildResourcePath(resourceInfo.folder,
          resourceInfo.resource);
    }

    log.info("Deleting resource {}", fileInFs);
    FileSystem sfs = sliderFileSystem.getFileSystem();
    require(sfs.exists(fileInFs), "No resource to delete found at %s", fileInFs.toUri());
    sfs.delete(fileInFs, true);

    return EXIT_SUCCESS;
  }

  private int actionInstallResource(ActionResourceArgs resourceInfo)
      throws BadCommandArgumentsException, IOException {
    Path srcFile = null;
    String folder = resourceInfo.folder != null ? resourceInfo.folder : StringUtils.EMPTY;

    requireArgumentSet(Arguments.ARG_RESOURCE, resourceInfo.resource);
    File file = new File(resourceInfo.resource);
    require(file.isFile() || file.isDirectory(),
        "Unable to access supplied file at %s", file.getAbsolutePath());

    File[] files;
    if (file.isDirectory()) {
      files = file.listFiles();
    } else {
      files = new File[] { file };
    }

    Path pkgPath = sliderFileSystem.buildResourcePath(folder);
    FileSystem sfs = sliderFileSystem.getFileSystem();

    if (!sfs.exists(pkgPath)) {
      sfs.mkdirs(pkgPath);
      sfs.setPermission(pkgPath, new FsPermission(
          FsAction.ALL, FsAction.NONE, FsAction.NONE));
    } else {
      require(sfs.isDirectory(pkgPath), "Specified folder %s exists and is " +
          "not a directory", folder);
    }

    for (File f : files) {
      srcFile = new Path(f.toURI());

      Path fileInFs = new Path(pkgPath, srcFile.getName());
      log.info("Installing file {} at {} and overwrite is {}.",
          srcFile, fileInFs, resourceInfo.overwrite);
      require(!(sfs.exists(fileInFs) && !resourceInfo.overwrite),
          "File exists at %s. Use --overwrite to overwrite.", fileInFs.toUri());

      sfs.copyFromLocalFile(false, resourceInfo.overwrite, srcFile, fileInFs);
      sfs.setPermission(fileInFs,
          new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));
    }

    return EXIT_SUCCESS;
  }

  @Override
  public int actionClient(ActionClientArgs clientInfo) throws
      YarnException,
      IOException {
    if (clientInfo.install) {
      return doClientInstall(clientInfo);
    } else {
      throw new BadCommandArgumentsException(
          "Only install, keystore, and truststore commands are supported for the client.\n"
          + CommonArgs.usage(serviceArgs, ACTION_CLIENT));

    }
  }

  private int doClientInstall(ActionClientArgs clientInfo)
      throws IOException, SliderException {

    require(clientInfo.installLocation != null,
          E_INVALID_INSTALL_LOCATION +"\n"
          + CommonArgs.usage(serviceArgs, ACTION_CLIENT));
    require(clientInfo.installLocation.exists(),
        E_INSTALL_PATH_DOES_NOT_EXIST + ": " + clientInfo.installLocation.getAbsolutePath());

    require(clientInfo.installLocation.isDirectory(),
        E_INVALID_INSTALL_PATH + ": " + clientInfo.installLocation.getAbsolutePath());

    File pkgFile;
    File tmpDir = null;

    require(isSet(clientInfo.packageURI) || isSet(clientInfo.name),
        E_INVALID_APPLICATION_PACKAGE_LOCATION);
    if (isSet(clientInfo.packageURI)) {
      pkgFile = new File(clientInfo.packageURI);
    } else {
      Path appDirPath = sliderFileSystem.buildAppDefDirPath(clientInfo.name);
      Path appDefPath = new Path(appDirPath, SliderKeys.DEFAULT_APP_PKG);
      require(sliderFileSystem.isFile(appDefPath),
          E_INVALID_APPLICATION_PACKAGE_LOCATION);
      tmpDir = Files.createTempDir();
      pkgFile = new File(tmpDir, SliderKeys.DEFAULT_APP_PKG);
      sliderFileSystem.copyHdfsFileToLocal(appDefPath, pkgFile);
    }
    require(pkgFile.isFile(),
        E_UNABLE_TO_READ_SUPPLIED_PACKAGE_FILE + " at %s", pkgFile.getAbsolutePath());

    JSONObject config = null;
    if(clientInfo.clientConfig != null) {
      try {
        byte[] encoded = Files.toByteArray(clientInfo.clientConfig);
        config = new JSONObject(new String(encoded, Charset.defaultCharset()));
      } catch (JSONException jsonEx) {
        log.error("Unable to read supplied configuration at {}: {}",
            clientInfo.clientConfig, jsonEx);
        log.debug("Unable to read supplied configuration at {}: {}",
            clientInfo.clientConfig, jsonEx, jsonEx);
        throw new BadConfigException(E_MUST_BE_A_VALID_JSON_FILE, jsonEx);
      }
    }

    // Only INSTALL is supported
    AbstractClientProvider
        provider = createClientProvider(SliderProviderFactory.DEFAULT_CLUSTER_TYPE);
    provider.processClientOperation(sliderFileSystem,
        getRegistryOperations(),
        getConfig(),
        "INSTALL",
        clientInfo.installLocation,
        pkgFile,
        config,
        clientInfo.name);
    return EXIT_SUCCESS;
  }


  @Override
  public int actionPackage(ActionPackageArgs actionPackageInfo)
      throws YarnException, IOException {
    initializeOutputStream(actionPackageInfo.out);
    int exitCode = -1;
    if (actionPackageInfo.help) {
      exitCode = actionHelp(ACTION_PACKAGE);
    }
    if (actionPackageInfo.install) {
      exitCode = actionPackageInstall(actionPackageInfo);
    }
    if (actionPackageInfo.delete) {
      exitCode = actionPackageDelete(actionPackageInfo);
    }
    if (actionPackageInfo.list) {
      exitCode = actionPackageList();
    }
    if (actionPackageInfo.instances) {
      exitCode = actionPackageInstances();
    }
    finalizeOutputStream(actionPackageInfo.out);
    if (exitCode != -1) {
      return exitCode;
    }
    throw new BadCommandArgumentsException(
        "Select valid package operation option");
  }

  private void initializeOutputStream(String outFile)
      throws IOException {
    if (outFile != null) {
      clientOutputStream = new PrintStream(outFile, "UTF-8");
    } else {
      clientOutputStream = System.out;
    }
  }

  private void finalizeOutputStream(String outFile) {
    if (outFile != null && clientOutputStream != null) {
      clientOutputStream.flush();
      clientOutputStream.close();
    }
    clientOutputStream = System.out;
  }

  private int actionPackageInstances() throws YarnException, IOException {
    Map<String, Path> persistentInstances = sliderFileSystem
        .listPersistentInstances();
    if (persistentInstances.isEmpty()) {
      log.info("No slider cluster specification available");
      return EXIT_SUCCESS;
    }
    String pkgPathValue = sliderFileSystem
        .buildPackageDirPath(StringUtils.EMPTY, StringUtils.EMPTY).toUri()
        .getPath();
    FileSystem fs = sliderFileSystem.getFileSystem();
    Iterator<Map.Entry<String, Path>> instanceItr = persistentInstances
        .entrySet().iterator();
    log.info("List of applications with its package name and path");
    println("%-25s  %15s  %30s  %s", "Cluster Name", "Package Name",
        "Package Version", "Application Location");
    while(instanceItr.hasNext()) {
      Map.Entry<String, Path> entry = instanceItr.next();
      String clusterName = entry.getKey();
      Path clusterPath = entry.getValue();
      AggregateConf instanceDefinition = loadInstanceDefinitionUnresolved(
          clusterName, clusterPath);
      Path appDefPath = null;
      try {
        appDefPath = new Path(
            getApplicationDefinitionPath(instanceDefinition
                .getAppConfOperations()));
      } catch (BadConfigException e) {
        // Invalid cluster state, so move on to next. No need to log anything
        // as this is just listing of instances.
        continue;
      }
      if (!appDefPath.isUriPathAbsolute()) {
        appDefPath = new Path(fs.getHomeDirectory(), appDefPath);
      }
      String appDefPathStr = appDefPath.toUri().toString();
      try {
        if (appDefPathStr.contains(pkgPathValue) && fs.isFile(appDefPath)) {
          String packageName = appDefPath.getParent().getName();
          String packageVersion = StringUtils.EMPTY;
          if (instanceDefinition.isVersioned()) {
            packageVersion = packageName;
            packageName = appDefPath.getParent().getParent().getName();
          }
          println("%-25s  %15s  %30s  %s", clusterName, packageName,
              packageVersion, appDefPathStr);
        }
      } catch (IOException e) {
        log.debug("{} application definition path {} is not found.", clusterName, appDefPathStr);
      }
    }
    return EXIT_SUCCESS;
  }

  private int actionPackageList() throws IOException {
    Path pkgPath = sliderFileSystem.buildPackageDirPath(StringUtils.EMPTY,
        StringUtils.EMPTY);
    log.info("Package install path : {}", pkgPath);
    FileSystem sfs = sliderFileSystem.getFileSystem();
    if (!sfs.isDirectory(pkgPath)) {
      log.info("No package(s) installed");
      return EXIT_SUCCESS;
    }
    FileStatus[] fileStatus = sfs.listStatus(pkgPath);
    boolean hasPackage = false;
    StringBuilder sb = new StringBuilder();
    sb.append("List of installed packages:\n");
    for (FileStatus fstat : fileStatus) {
      if (fstat.isDirectory()) {
        sb.append("\t").append(fstat.getPath().getName());
        sb.append("\n");
        hasPackage = true;
      }
    }
    if (hasPackage) {
      println(sb.toString());
    } else {
      log.info("No package(s) installed");
    }
    return EXIT_SUCCESS;
  }

  private void createSummaryMetainfoFile(Path srcFile, Path destFile,
      boolean overwrite) throws IOException {
    FileSystem srcFs = srcFile.getFileSystem(getConfig());
    try (InputStream inputStreamJson = SliderUtils
        .getApplicationResourceInputStream(srcFs, srcFile, "metainfo.json");
        InputStream inputStreamXml = SliderUtils
            .getApplicationResourceInputStream(srcFs, srcFile, "metainfo.xml");) {
      InputStream inputStream = null;
      Path summaryFileInFs = null;
      if (inputStreamJson != null) {
        inputStream = inputStreamJson;
        summaryFileInFs = new Path(destFile.getParent(), destFile.getName()
            + ".metainfo.json");
        log.info("Found JSON metainfo file in package");
      } else if (inputStreamXml != null) {
        inputStream = inputStreamXml;
        summaryFileInFs = new Path(destFile.getParent(), destFile.getName()
            + ".metainfo.xml");
        log.info("Found XML metainfo file in package");
      }
      if (inputStream != null) {
        try (FSDataOutputStream dataOutputStream = sliderFileSystem
            .getFileSystem().create(summaryFileInFs, overwrite)) {
          log.info("Creating summary metainfo file");
          IOUtils.copy(inputStream, dataOutputStream);
        }
      }
    }
  }

  private int actionPackageInstall(ActionPackageArgs actionPackageArgs)
      throws YarnException, IOException {
    requireArgumentSet(Arguments.ARG_NAME, actionPackageArgs.name);

    Path srcFile = extractPackagePath(actionPackageArgs.packageURI);

    Path pkgPath = sliderFileSystem.buildPackageDirPath(actionPackageArgs.name,
        actionPackageArgs.version);
    FileSystem fs = sliderFileSystem.getFileSystem();
    if (!fs.exists(pkgPath)) {
      fs.mkdirs(pkgPath);
    }

    Path fileInFs = new Path(pkgPath, srcFile.getName());
    require(actionPackageArgs.replacePkg || !fs.exists(fileInFs),
        E_PACKAGE_EXISTS +" at  %s. Use --replacepkg to overwrite.", fileInFs.toUri());

    log.info("Installing package {} to {} (overwrite set to {})", srcFile,
        fileInFs, actionPackageArgs.replacePkg);
    fs.copyFromLocalFile(false, actionPackageArgs.replacePkg, srcFile, fileInFs);
    createSummaryMetainfoFile(srcFile, fileInFs, actionPackageArgs.replacePkg);

    String destPathWithHomeDir = Path
        .getPathWithoutSchemeAndAuthority(fileInFs).toString();
    String destHomeDir = Path.getPathWithoutSchemeAndAuthority(
        fs.getHomeDirectory()).toString();
    // a somewhat contrived approach to stripping out the home directory and any trailing
    // separator; designed to work on windows and unix
    String destPathWithoutHomeDir;
    if (destPathWithHomeDir.startsWith(destHomeDir)) {
      destPathWithoutHomeDir = destPathWithHomeDir.substring(destHomeDir.length());
      if (destPathWithoutHomeDir.startsWith("/") || destPathWithoutHomeDir.startsWith("\\")) {
        destPathWithoutHomeDir = destPathWithoutHomeDir.substring(1);
      }
    } else {
      destPathWithoutHomeDir = destPathWithHomeDir;
    }
    log.info("Set " + AgentKeys.APP_DEF + " in your app config JSON to {}",
        destPathWithoutHomeDir);

    return EXIT_SUCCESS;
  }

  private Path extractPackagePath(String packageURI)
      throws BadCommandArgumentsException {
    require(isSet(packageURI), E_INVALID_APPLICATION_PACKAGE_LOCATION);
    File pkgFile = new File(packageURI);
    require(pkgFile.isFile(),
        E_UNABLE_TO_READ_SUPPLIED_PACKAGE_FILE + ":  " + pkgFile.getAbsolutePath());
    return new Path(pkgFile.toURI());
  }

  private int actionPackageDelete(ActionPackageArgs actionPackageArgs) throws
      YarnException, IOException {
    requireArgumentSet(Arguments.ARG_NAME, actionPackageArgs.name);

    Path pkgPath = sliderFileSystem.buildPackageDirPath(actionPackageArgs.name,
        actionPackageArgs.version);
    FileSystem fs = sliderFileSystem.getFileSystem();
    require(fs.exists(pkgPath), E_PACKAGE_DOES_NOT_EXIST +": %s ", pkgPath.toUri());
    log.info("Deleting package {} at {}.", actionPackageArgs.name, pkgPath);

    if(fs.delete(pkgPath, true)) {
      log.info("Deleted package {} " + actionPackageArgs.name);
      return EXIT_SUCCESS;
    } else {
      log.warn("Package deletion failed.");
      return EXIT_NOT_FOUND;
    }
  }

  @Override
  public int actionUpdate(String clustername,
      AbstractClusterBuildingActionArgs buildInfo) throws
      YarnException, IOException {
    if (buildInfo.lifetime > 0) {
      updateLifetime(clustername, buildInfo.lifetime);
    } else {
      buildInstanceDefinition(clustername, buildInfo, true, true);
    }
    return EXIT_SUCCESS;
  }

  public void updateLifetime(String appName, long lifetime)
      throws YarnException, IOException {
    EnumSet<YarnApplicationState> appStates = EnumSet.range(
        YarnApplicationState.NEW, YarnApplicationState.RUNNING);
    ApplicationReport report = findInstance(appName, appStates);
    if (report == null) {
      throw new YarnException("Application not found for " + appName);
    }
    ApplicationId appId = report.getApplicationId();
    log.info("Updating lifetime of an application: appName = " + appName
        + ", appId = " + appId+ ", lifetime = " + lifetime);
    Map<ApplicationTimeoutType, String> map = new HashMap<>();
    String newTimeout =
        Times.formatISO8601(System.currentTimeMillis() + lifetime * 1000);
    map.put(ApplicationTimeoutType.LIFETIME, newTimeout);
    UpdateApplicationTimeoutsRequest request =
        UpdateApplicationTimeoutsRequest.newInstance(appId, map);
    yarnClient.updateApplicationTimeouts(request);
    log.info("Successfully updated lifetime for an application: appName = "
        + appName + ", appId = " + appId
        + ". New expiry time in ISO8601 format is " + newTimeout);
  }

  /**
   * Build up the AggregateConfiguration for an application instance then
   * persists it
   * @param clustername name of the cluster
   * @param buildInfo the arguments needed to build the cluster
   * @param overwrite true if existing cluster directory can be overwritten
   * @param liveClusterAllowed true if live cluster can be modified
   * @throws YarnException
   * @throws IOException
   */

  public void buildInstanceDefinition(String clustername,
      AbstractClusterBuildingActionArgs buildInfo, boolean overwrite,
      boolean liveClusterAllowed) throws YarnException, IOException {
    buildInstanceDefinition(clustername, buildInfo, overwrite,
        liveClusterAllowed, false);
  }

  public void buildInstanceDefinition(String clustername,
      AbstractClusterBuildingActionArgs buildInfo, boolean overwrite,
      boolean liveClusterAllowed, boolean isUpgradeFlow) throws YarnException,
      IOException {
    // verify that a live cluster isn't there
    validateClusterName(clustername);
    verifyBindingsDefined();
    if (!liveClusterAllowed) {
      verifyNoLiveClusters(clustername, "Create");
    }

    Configuration conf = getConfig();
    String registryQuorum = lookupZKQuorum();

    Path appconfdir = buildInfo.getConfdir();
    // Provider
    String providerName = buildInfo.getProvider();
    requireArgumentSet(Arguments.ARG_PROVIDER, providerName);
    log.debug("Provider is {}", providerName);
    SliderAMClientProvider sliderAM = new SliderAMClientProvider(conf);
    AbstractClientProvider provider =
      createClientProvider(providerName);
    InstanceBuilder builder =
      new InstanceBuilder(sliderFileSystem, 
                          getConfig(),
                          clustername);
    
    AggregateConf instanceDefinition = new AggregateConf();
    ConfTreeOperations appConf = instanceDefinition.getAppConfOperations();
    ConfTreeOperations resources = instanceDefinition.getResourceOperations();
    ConfTreeOperations internal = instanceDefinition.getInternalOperations();
    //initial definition is set by the providers 
    sliderAM.prepareInstanceConfiguration(instanceDefinition);
    provider.prepareInstanceConfiguration(instanceDefinition);

    //load in any specified on the command line
    if (buildInfo.resources != null) {
      try {
        resources.mergeFile(buildInfo.resources,
                            new ResourcesInputPropertiesValidator());

      } catch (IOException e) {
        throw new BadConfigException(e,
               "incorrect argument to %s: \"%s\" : %s ", 
                                     Arguments.ARG_RESOURCES,
                                     buildInfo.resources,
                                     e.toString());
      }
    }
    if (buildInfo.template != null) {
      try {
        appConf.mergeFile(buildInfo.template,
                          new TemplateInputPropertiesValidator());
      } catch (IOException e) {
        throw new BadConfigException(e,
                                     "incorrect argument to %s: \"%s\" : %s ",
                                     Arguments.ARG_TEMPLATE,
                                     buildInfo.template,
                                     e.toString());
      }
    }

    if (isUpgradeFlow) {
      ActionUpgradeArgs upgradeInfo = (ActionUpgradeArgs) buildInfo;
      if (!upgradeInfo.force) {
        validateClientAndClusterResource(clustername, resources);
      }
    }

    //get the command line options
    ConfTree cmdLineAppOptions = buildInfo.buildAppOptionsConfTree();
    ConfTree cmdLineResourceOptions = buildInfo.buildResourceOptionsConfTree();

    appConf.merge(cmdLineAppOptions);

    AppDefinitionPersister appDefinitionPersister = new AppDefinitionPersister(sliderFileSystem);
    appDefinitionPersister.processSuppliedDefinitions(clustername, buildInfo, appConf);

    // put the role counts into the resources file
    Map<String, String> argsRoleMap = buildInfo.getComponentMap();
    for (Map.Entry<String, String> roleEntry : argsRoleMap.entrySet()) {
      String count = roleEntry.getValue();
      String key = roleEntry.getKey();
      log.info("{} => {}", key, count);
      resources.getOrAddComponent(key).put(COMPONENT_INSTANCES, count);
    }

    //all CLI role options
    Map<String, Map<String, String>> appOptionMap =
      buildInfo.getCompOptionMap();
    appConf.mergeComponents(appOptionMap);

    //internal picks up core. values only
    internal.propagateGlobalKeys(appConf, "slider.");
    internal.propagateGlobalKeys(appConf, "internal.");

    //copy over role. and yarn. values ONLY to the resources
    if (PROPAGATE_RESOURCE_OPTION) {
      resources.propagateGlobalKeys(appConf, "component.");
      resources.propagateGlobalKeys(appConf, "role.");
      resources.propagateGlobalKeys(appConf, "yarn.");
      resources.mergeComponentsPrefix(appOptionMap, "component.", true);
      resources.mergeComponentsPrefix(appOptionMap, "yarn.", true);
      resources.mergeComponentsPrefix(appOptionMap, "role.", true);
    }

    // resource component args
    appConf.merge(cmdLineResourceOptions);
    resources.merge(cmdLineResourceOptions);
    resources.mergeComponents(buildInfo.getResourceCompOptionMap());

    builder.init(providerName, instanceDefinition);
    builder.resolve();
    builder.propagateFilename();
    builder.propagatePrincipals();
    builder.setImageDetailsIfAvailable(buildInfo.getImage(),
                                       buildInfo.getAppHomeDir());
    builder.setQueue(buildInfo.queue);

    String quorum = buildInfo.getZKhosts();
    if (isUnset(quorum)) {
      quorum = registryQuorum;
    }
    if (isUnset(quorum)) {
      throw new BadConfigException(E_NO_ZOOKEEPER_QUORUM);
    }
    ZKPathBuilder zkPaths = new ZKPathBuilder(getAppName(),
        getUsername(),
        clustername,
        registryQuorum,
        quorum);
    String zookeeperRoot = buildInfo.getAppZKPath();

    if (isSet(zookeeperRoot)) {
      zkPaths.setAppPath(zookeeperRoot);
    } else {
      String createDefaultZkNode = appConf.getGlobalOptions()
          .getOption(AgentKeys.CREATE_DEF_ZK_NODE, "false");
      if (createDefaultZkNode.equals("true")) {
        String defaultZKPath = createZookeeperNode(clustername, false);
        log.debug("ZK node created for application instance: {}", defaultZKPath);
        if (defaultZKPath != null) {
          zkPaths.setAppPath(defaultZKPath);
        }
      } else {
        // create AppPath if default is being used
        String defaultZKPath = createZookeeperNode(clustername, true);
        log.debug("ZK node assigned to application instance: {}", defaultZKPath);
        zkPaths.setAppPath(defaultZKPath);
      }
    }

    builder.addZKBinding(zkPaths);

    //then propagate any package URI
    if (buildInfo.packageURI != null) {
      appConf.set(AgentKeys.PACKAGE_PATH, buildInfo.packageURI);
    }

    propagatePythonExecutable(conf, instanceDefinition);

    // make any substitutions needed at this stage
    replaceTokens(appConf.getConfTree(), getUsername(), clustername);

    // TODO: Refactor the validation code and persistence code
    try {
      persistInstanceDefinition(overwrite, appconfdir, builder);
      appDefinitionPersister.persistPackages();

    } catch (LockAcquireFailedException e) {
      log.warn("Failed to get a Lock on {} : {}", builder, e, e);
      throw new BadClusterStateException("Failed to save " + clustername
                                         + ": " + e);
    }

    // providers to validate what there is
    // TODO: Validation should be done before persistence
    AggregateConf instanceDescription = builder.getInstanceDescription();
    validateInstanceDefinition(sliderAM, instanceDescription, sliderFileSystem);
    validateInstanceDefinition(provider, instanceDescription, sliderFileSystem);
  }

  private void validateClientAndClusterResource(String clustername,
      ConfTreeOperations clientResources) throws BadClusterStateException,
      SliderException, IOException {
    log.info("Validating upgrade resource definition with current cluster "
        + "state (components and instance count)");
    Map<String, Integer> clientComponentInstances = new HashMap<>();
    for (String componentName : clientResources.getComponentNames()) {
      if (!SliderKeys.COMPONENT_AM.equals(componentName)) {
        clientComponentInstances.put(componentName, clientResources
            .getComponentOptInt(componentName,
                COMPONENT_INSTANCES, -1));
      }
    }

    AggregateConf clusterConf = null;
    try {
      clusterConf = loadPersistedClusterDescription(clustername);
    } catch (LockAcquireFailedException e) {
      log.warn("Failed to get a Lock on cluster resource : {}", e, e);
      throw new BadClusterStateException(
          "Failed to load client resource definition " + clustername + ": " + e, e);
    }
    Map<String, Integer> clusterComponentInstances = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> component : clusterConf
        .getResources().components.entrySet()) {
      if (!SliderKeys.COMPONENT_AM.equals(component.getKey())) {
        clusterComponentInstances.put(
            component.getKey(),
            Integer.decode(component.getValue().get(
                COMPONENT_INSTANCES)));
      }
    }

    // client and cluster should be an exact match
    Iterator<Map.Entry<String, Integer>> clientComponentInstanceIt = clientComponentInstances
        .entrySet().iterator();
    while (clientComponentInstanceIt.hasNext()) {
      Map.Entry<String, Integer> clientComponentInstanceEntry = clientComponentInstanceIt.next();
      if (clusterComponentInstances.containsKey(clientComponentInstanceEntry.getKey())) {
        // compare instance count now and remove from both maps if they match
        if (clusterComponentInstances
            .get(clientComponentInstanceEntry.getKey()).intValue() == clientComponentInstanceEntry
            .getValue().intValue()) {
          clusterComponentInstances.remove(clientComponentInstanceEntry
              .getKey());
          clientComponentInstanceIt.remove();
        }
      }
    }

    if (!clientComponentInstances.isEmpty()
        || !clusterComponentInstances.isEmpty()) {
      log.error("Mismatch found in upgrade resource definition and cluster "
          + "resource state");
      if (!clientComponentInstances.isEmpty()) {
        log.info("The upgrade resource definitions that do not match are:");
        for (Map.Entry<String, Integer> clientComponentInstanceEntry : clientComponentInstances
            .entrySet()) {
          log.info("    Component Name: {}, Instance count: {}",
              clientComponentInstanceEntry.getKey(),
              clientComponentInstanceEntry.getValue());
        }
      }
      if (!clusterComponentInstances.isEmpty()) {
        log.info("The cluster resources that do not match are:");
        for (Map.Entry<String, Integer> clusterComponentInstanceEntry : clusterComponentInstances
            .entrySet()) {
          log.info("    Component Name: {}, Instance count: {}",
              clusterComponentInstanceEntry.getKey(),
              clusterComponentInstanceEntry.getValue());
        }
      }
      throw new BadConfigException("Resource definition provided for "
          + "upgrade does not match with that of the currently running "
          + "cluster.\nIf you are aware of what you are doing, rerun the "
          + "command with " + Arguments.ARG_FORCE + " option.");
    }
  }

  protected void persistInstanceDefinition(boolean overwrite,
                                         Path appconfdir,
                                         InstanceBuilder builder)
      throws IOException, SliderException, LockAcquireFailedException {
    builder.persist(appconfdir, overwrite);
  }

  @VisibleForTesting
  public static void replaceTokens(ConfTree conf,
      String userName, String clusterName) throws IOException {
    Map<String,String> newglobal = new HashMap<>();
    for (Entry<String,String> entry : conf.global.entrySet()) {
      newglobal.put(entry.getKey(), replaceTokens(entry.getValue(),
          userName, clusterName));
    }
    conf.global.putAll(newglobal);

    for (String component : conf.components.keySet()) {
      Map<String,String> newComponent = new HashMap<>();
      for (Entry<String,String> entry : conf.components.get(component).entrySet()) {
        newComponent.put(entry.getKey(), replaceTokens(entry.getValue(),
            userName, clusterName));
      }
      conf.components.get(component).putAll(newComponent);
    }

    Map<String,List<String>> newcred = new HashMap<>();
    for (Entry<String,List<String>> entry : conf.credentials.entrySet()) {
      List<String> resultList = new ArrayList<>();
      for (String v : entry.getValue()) {
        resultList.add(replaceTokens(v, userName, clusterName));
      }
      newcred.put(replaceTokens(entry.getKey(), userName, clusterName),
          resultList);
    }
    conf.credentials.clear();
    conf.credentials.putAll(newcred);
  }

  private static String replaceTokens(String s, String userName,
      String clusterName) throws IOException {
    return s.replaceAll(Pattern.quote("${USER}"), userName)
        .replaceAll(Pattern.quote("${USER_NAME}"), userName);
  }

  public FsPermission getClusterDirectoryPermissions(Configuration conf) {
    String clusterDirPermsOct =
      conf.get(CLUSTER_DIRECTORY_PERMISSIONS, DEFAULT_CLUSTER_DIRECTORY_PERMISSIONS);
    return new FsPermission(clusterDirPermsOct);
  }

  /**
   * Verify that the Resource Manager is configured (on a non-HA cluster).
   * with a useful error message
   * @throws BadCommandArgumentsException the exception raised on an invalid config
   */
  public void verifyBindingsDefined() throws BadCommandArgumentsException {
    InetSocketAddress rmAddr = getRmAddress(getConfig());
    if (!getConfig().getBoolean(YarnConfiguration.RM_HA_ENABLED, false)
     && !isAddressDefined(rmAddr)) {
      throw new BadCommandArgumentsException(
        E_NO_RESOURCE_MANAGER
        + " in the argument "
        + Arguments.ARG_MANAGER
        + " or the configuration property "
        + YarnConfiguration.RM_ADDRESS 
        + " value :" + rmAddr);
    }
  }

  /**
   * Load and start a cluster specification.
   * This assumes that all validation of args and cluster state
   * have already taken place
   *
   * @param clustername name of the cluster.
   * @param launchArgs launch arguments
   * @param lifetime
   * @return the exit code
   * @throws YarnException
   * @throws IOException
   */
  protected int startCluster(String clustername, LaunchArgsAccessor launchArgs,
      long lifetime) throws YarnException, IOException {
    Path clusterDirectory = sliderFileSystem.buildClusterDirPath(clustername);
    AggregateConf instanceDefinition = loadInstanceDefinitionUnresolved(
      clustername,
      clusterDirectory);

    LaunchedApplication launchedApplication =
      launchApplication(clustername, clusterDirectory, instanceDefinition,
                        serviceArgs.isDebug(), lifetime);

    if (launchArgs.getOutputFile() != null) {
      // output file has been requested. Get the app report and serialize it
      ApplicationReport report =
          launchedApplication.getApplicationReport();
      SerializedApplicationReport sar = new SerializedApplicationReport(report);
      sar.submitTime = System.currentTimeMillis();
      ApplicationReportSerDeser serDeser = new ApplicationReportSerDeser();
      serDeser.save(sar, launchArgs.getOutputFile());
    }
    int waittime = launchArgs.getWaittime();
    if (waittime > 0) {
      return waitForAppRunning(launchedApplication, waittime, waittime);
    } else {
      // no waiting
      return EXIT_SUCCESS;
    }
  }

  /**
   * Load the instance definition. It is not resolved at this point
   * @param name cluster name
   * @param clusterDirectory cluster dir
   * @return the loaded configuration
   * @throws IOException
   * @throws SliderException
   * @throws UnknownApplicationInstanceException if the file is not found
   */
  public AggregateConf loadInstanceDefinitionUnresolved(String name,
            Path clusterDirectory) throws IOException, SliderException {

    try {
      AggregateConf definition =
        InstanceIO.loadInstanceDefinitionUnresolved(sliderFileSystem,
                                                    clusterDirectory);
      definition.setName(name);
      return definition;
    } catch (FileNotFoundException e) {
      throw UnknownApplicationInstanceException.unknownInstance(name, e);
    }
  }

  /**
   * Load the instance definition. 
   * @param name cluster name
   * @param resolved flag to indicate the cluster should be resolved
   * @return the loaded configuration
   * @throws IOException IO problems
   * @throws SliderException slider explicit issues
   * @throws UnknownApplicationInstanceException if the file is not found
   */
    public AggregateConf loadInstanceDefinition(String name,
        boolean resolved) throws
        IOException,
        SliderException {

    Path clusterDirectory = sliderFileSystem.buildClusterDirPath(name);
    AggregateConf instanceDefinition = loadInstanceDefinitionUnresolved(
      name,
      clusterDirectory);
    if (resolved) {
      instanceDefinition.resolve();
    }
    return instanceDefinition;

  }

  protected AppMasterLauncher setupAppMasterLauncher(String clustername,
      Path clusterDirectory, AggregateConf instanceDefinition, boolean debugAM,
      long lifetime)
    throws YarnException, IOException{
    deployedClusterName = clustername;
    validateClusterName(clustername);
    verifyNoLiveClusters(clustername, "Launch");
    Configuration config = getConfig();
    lookupZKQuorum();
    boolean clusterSecure = isHadoopClusterSecure(config);
    //create the Slider AM provider -this helps set up the AM
    SliderAMClientProvider sliderAM = new SliderAMClientProvider(config);

    instanceDefinition.resolve();
    launchedInstanceDefinition = instanceDefinition;

    ConfTreeOperations internalOperations = instanceDefinition.getInternalOperations();
    MapOperations internalOptions = internalOperations.getGlobalOptions();
    ConfTreeOperations resourceOperations = instanceDefinition.getResourceOperations();
    ConfTreeOperations appOperations = instanceDefinition.getAppConfOperations();
    Path generatedConfDirPath =
      createPathThatMustExist(internalOptions.getMandatoryOption(
        INTERNAL_GENERATED_CONF_PATH));
    Path snapshotConfPath =
      createPathThatMustExist(internalOptions.getMandatoryOption(
        INTERNAL_SNAPSHOT_CONF_PATH));


    // cluster Provider
    AbstractClientProvider provider = createClientProvider(
      internalOptions.getMandatoryOption(INTERNAL_PROVIDER_NAME));
    if (log.isDebugEnabled()) {
      log.debug(instanceDefinition.toString());
    }
    MapOperations sliderAMResourceComponent =
      resourceOperations.getOrAddComponent(SliderKeys.COMPONENT_AM);
    MapOperations resourceGlobalOptions = resourceOperations.getGlobalOptions();

    // add the tags if available
    Set<String> applicationTags = provider.getApplicationTags(sliderFileSystem,
        appOperations, clustername);

    Credentials credentials = null;
    if (clusterSecure) {
      // pick up oozie credentials
      credentials = CredentialUtils.loadTokensFromEnvironment(System.getenv(),
          config);
      if (credentials == null) {
        // nothing from oozie, so build up directly
        credentials = new Credentials(
            UserGroupInformation.getCurrentUser().getCredentials());
        CredentialUtils.addRMRenewableFSDelegationTokens(config,
            sliderFileSystem.getFileSystem(),
            credentials);
        CredentialUtils.addRMDelegationToken(yarnClient, credentials);

      } else {
        log.info("Using externally supplied credentials to launch AM");
      }
    }

    AppMasterLauncher amLauncher = new AppMasterLauncher(clustername,
        SliderKeys.APP_TYPE,
        config,
        sliderFileSystem,
        yarnClient,
        clusterSecure,
        sliderAMResourceComponent,
        resourceGlobalOptions,
        applicationTags,
        credentials);

    ApplicationId appId = amLauncher.getApplicationId();
    // set the application name;
    amLauncher.setKeepContainersOverRestarts(true);
    // set lifetime in submission context;
    Map<ApplicationTimeoutType, Long> appTimeout = new HashMap<>();
    if (lifetime > 0) {
      appTimeout.put(ApplicationTimeoutType.LIFETIME, lifetime);
    }
    amLauncher.submissionContext.setApplicationTimeouts(appTimeout);
    int maxAppAttempts = config.getInt(KEY_AM_RESTART_LIMIT, 0);
    amLauncher.setMaxAppAttempts(maxAppAttempts);

    sliderFileSystem.purgeAppInstanceTempFiles(clustername);
    Path tempPath = sliderFileSystem.createAppInstanceTempPath(
        clustername,
        appId.toString() + "/am");
    String libdir = "lib";
    Path libPath = new Path(tempPath, libdir);
    sliderFileSystem.getFileSystem().mkdirs(libPath);
    log.debug("FS={}, tempPath={}, libdir={}", sliderFileSystem, tempPath, libPath);
 
    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the local resources
    Map<String, LocalResource> localResources = amLauncher.getLocalResources();
    
    // look for the configuration directory named on the command line
    boolean hasServerLog4jProperties = false;
    Path remoteConfPath = null;
    String relativeConfDir = null;
    String confdirProp = System.getProperty(SliderKeys.PROPERTY_CONF_DIR);
    if (isUnset(confdirProp)) {
      log.debug("No local configuration directory provided as system property");
    } else {
      File confDir = new File(confdirProp);
      if (!confDir.exists()) {
        throw new BadConfigException(E_CONFIGURATION_DIRECTORY_NOT_FOUND,
                                     confDir);
      }
      Path localConfDirPath = createLocalPath(confDir);
      remoteConfPath = new Path(clusterDirectory, SliderKeys.SUBMITTED_CONF_DIR);
      log.debug("Slider configuration directory is {}; remote to be {}", 
          localConfDirPath, remoteConfPath);
      copyDirectory(config, localConfDirPath, remoteConfPath, null);

      File log4jserver =
          new File(confDir, SliderKeys.LOG4J_SERVER_PROP_FILENAME);
      hasServerLog4jProperties = log4jserver.isFile();
    }
    if (!hasServerLog4jProperties) {
      // check for log4j properties in hadoop conf dir
      String hadoopConfDir = System.getenv(ApplicationConstants.Environment
          .HADOOP_CONF_DIR.name());
      if (hadoopConfDir != null) {
        File localFile = new File(hadoopConfDir, SliderKeys
            .LOG4J_SERVER_PROP_FILENAME);
        if (localFile.exists()) {
          Path localFilePath = createLocalPath(localFile);
          remoteConfPath = new Path(clusterDirectory,
              SliderKeys.SUBMITTED_CONF_DIR);
          Path remoteFilePath = new Path(remoteConfPath, SliderKeys
              .LOG4J_SERVER_PROP_FILENAME);
          copy(config, localFilePath, remoteFilePath);
          hasServerLog4jProperties = true;
        }
      }
    }
    // the assumption here is that minimr cluster => this is a test run
    // and the classpath can look after itself

    boolean usingMiniMRCluster = getUsingMiniMRCluster();
    if (!usingMiniMRCluster) {

      log.debug("Destination is not a MiniYARNCluster -copying full classpath");

      // insert conf dir first
      if (remoteConfPath != null) {
        relativeConfDir = SliderKeys.SUBMITTED_CONF_DIR;
        Map<String, LocalResource> submittedConfDir =
          sliderFileSystem.submitDirectory(remoteConfPath,
                                         relativeConfDir);
        mergeMaps(localResources, submittedConfDir);
      }
    }
    // build up the configuration 
    // IMPORTANT: it is only after this call that site configurations
    // will be valid.

    propagatePrincipals(config, instanceDefinition);
    // validate security data

/*
    // turned off until tested
    SecurityConfiguration securityConfiguration =
        new SecurityConfiguration(config,
            instanceDefinition, clustername);
    
*/
    Configuration clientConfExtras = new Configuration(false);
    // then build up the generated path.
    FsPermission clusterPerms = getClusterDirectoryPermissions(config);
    copyDirectory(config, snapshotConfPath, generatedConfDirPath,
        clusterPerms);


    // standard AM resources
    sliderAM.prepareAMAndConfigForLaunch(sliderFileSystem,
                                       config,
                                       amLauncher,
                                       instanceDefinition,
                                       snapshotConfPath,
                                       generatedConfDirPath,
                                       clientConfExtras,
                                       libdir,
                                       tempPath,
                                       usingMiniMRCluster);
    //add provider-specific resources
    provider.prepareAMAndConfigForLaunch(sliderFileSystem,
                                         config,
                                         amLauncher,
                                         instanceDefinition,
                                         snapshotConfPath,
                                         generatedConfDirPath,
                                         clientConfExtras,
                                         libdir,
                                         tempPath,
                                         usingMiniMRCluster);

    // now that the site config is fully generated, the provider gets
    // to do a quick review of them.
    log.debug("Preflight validation of cluster configuration");


    sliderAM.preflightValidateClusterConfiguration(sliderFileSystem,
                                                 clustername,
                                                 config,
                                                 instanceDefinition,
                                                 clusterDirectory,
                                                 generatedConfDirPath,
                                                 clusterSecure
                                                );

    provider.preflightValidateClusterConfiguration(sliderFileSystem,
                                                   clustername,
                                                   config,
                                                   instanceDefinition,
                                                   clusterDirectory,
                                                   generatedConfDirPath,
                                                   clusterSecure
                                                  );


    if (!(provider instanceof DockerClientProvider)) {
      Path imagePath =
          extractImagePath(sliderFileSystem, internalOptions);
      if (sliderFileSystem.maybeAddImagePath(localResources, imagePath)) {
        log.debug("Registered image path {}", imagePath);
      }
    }

    // build the environment
    amLauncher.putEnv(
      buildEnvMap(sliderAMResourceComponent));
    ClasspathConstructor classpath = buildClasspath(relativeConfDir,
        libdir,
        getConfig(),
        sliderFileSystem,
        usingMiniMRCluster);
    amLauncher.setClasspath(classpath);
    //add english env
    amLauncher.setEnv("LANG", "en_US.UTF-8");
    amLauncher.setEnv("LC_ALL", "en_US.UTF-8");
    amLauncher.setEnv("LANGUAGE", "en_US.UTF-8");
    amLauncher.maybeSetEnv(HADOOP_JAAS_DEBUG,
        System.getenv(HADOOP_JAAS_DEBUG));
    amLauncher.putEnv(getAmLaunchEnv(config));

    for (Map.Entry<String, String> envs : getSystemEnv().entrySet()) {
      log.debug("System env {}={}", envs.getKey(), envs.getValue());
    }
    if (log.isDebugEnabled()) {
      log.debug("AM classpath={}", classpath);
      log.debug("Environment Map:\n{}",
                stringifyMap(amLauncher.getEnv()));
      log.debug("Files in lib path\n{}", sliderFileSystem.listFSDir(libPath));
    }

    // rm address

    InetSocketAddress rmSchedulerAddress;
    try {
      rmSchedulerAddress = getRmSchedulerAddress(config);
    } catch (IllegalArgumentException e) {
      throw new BadConfigException("%s Address invalid: %s",
               YarnConfiguration.RM_SCHEDULER_ADDRESS,
               config.get(YarnConfiguration.RM_SCHEDULER_ADDRESS));
    }
    String rmAddr = NetUtils.getHostPortString(rmSchedulerAddress);

    JavaCommandLineBuilder commandLine = new JavaCommandLineBuilder();
    // insert any JVM options);
    sliderAM.addJVMOptions(instanceDefinition, commandLine);
    // enable asserts
    commandLine.enableJavaAssertions();
    
    // if the conf dir has a slideram-log4j.properties, switch to that
    if (hasServerLog4jProperties) {
      commandLine.sysprop(SYSPROP_LOG4J_CONFIGURATION, LOG4J_SERVER_PROP_FILENAME);
      commandLine.sysprop(SYSPROP_LOG_DIR, ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    }
    
    // add the AM sevice entry point
    commandLine.add(SliderAppMaster.SERVICE_CLASSNAME);

    // create action and the cluster name
    commandLine.add(ACTION_CREATE, clustername);

    // debug
    if (debugAM) {
      commandLine.add(Arguments.ARG_DEBUG);
    }

    // set the cluster directory path
    commandLine.add(Arguments.ARG_CLUSTER_URI, clusterDirectory.toUri());

    if (!isUnset(rmAddr)) {
      commandLine.add(Arguments.ARG_RM_ADDR, rmAddr);
    }

    if (serviceArgs.getFilesystemBinding() != null) {
      commandLine.add(Arguments.ARG_FILESYSTEM, serviceArgs.getFilesystemBinding());
    }

    // pass the registry binding
    commandLine.addConfOptionToCLI(config, RegistryConstants.KEY_REGISTRY_ZK_ROOT,
        RegistryConstants.DEFAULT_ZK_REGISTRY_ROOT);
    commandLine.addMandatoryConfOption(config, RegistryConstants.KEY_REGISTRY_ZK_QUORUM);

    if (clusterSecure) {
      // if the cluster is secure, make sure that
      // the relevant security settings go over
      commandLine.addConfOption(config, DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY);
    }

    // copy over any/all YARN RM client values, in case the server-side XML conf file
    // has the 0.0.0.0 address
    commandLine.addConfOptions(config,
        YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.RM_CLUSTER_ID,
        YarnConfiguration.RM_HOSTNAME,
        YarnConfiguration.RM_PRINCIPAL);

    // write out the path output
    commandLine.addOutAndErrFiles(STDOUT_AM, STDERR_AM);

    String cmdStr = commandLine.build();
    log.debug("Completed setting up app master command {}", cmdStr);

    amLauncher.addCommandLine(commandLine);

    // the Slider AM gets to configure the AM requirements, not the custom provider
    sliderAM.prepareAMResourceRequirements(sliderAMResourceComponent,
        amLauncher.getResource());


    // Set the priority for the application master
    amLauncher.setPriority(config.getInt(KEY_YARN_QUEUE_PRIORITY,
                                   DEFAULT_YARN_QUEUE_PRIORITY));

    // Set the queue to which this application is to be submitted in the RM
    // Queue for App master
    String amQueue = config.get(KEY_YARN_QUEUE, DEFAULT_YARN_QUEUE);
    String suppliedQueue = internalOperations.getGlobalOptions().get(INTERNAL_QUEUE);
    if(!isUnset(suppliedQueue)) {
      amQueue = suppliedQueue;
      log.info("Using queue {} for the application instance.", amQueue);
    }

    if (isSet(amQueue)) {
      amLauncher.setQueue(amQueue);
    }
    return amLauncher;
  }

  /**
   *
   * @param clustername name of the cluster
   * @param clusterDirectory cluster dir
   * @param instanceDefinition the instance definition
   * @param debugAM enable debug AM options
   * @param lifetime
   * @return the launched application
   * @throws YarnException
   * @throws IOException
   */
  public LaunchedApplication launchApplication(String clustername, Path clusterDirectory,
      AggregateConf instanceDefinition, boolean debugAM, long lifetime)
    throws YarnException, IOException {

    AppMasterLauncher amLauncher = setupAppMasterLauncher(clustername,
        clusterDirectory,
        instanceDefinition,
        debugAM, lifetime);

    applicationId = amLauncher.getApplicationId();
    log.info("Submitting application {}", applicationId);

    // submit the application
    LaunchedApplication launchedApplication = amLauncher.submitApplication();
    return launchedApplication;
  }

  protected Map<String, String> getAmLaunchEnv(Configuration config) {
    String sliderAmLaunchEnv = config.get(KEY_AM_LAUNCH_ENV);
    log.debug("{} = {}", KEY_AM_LAUNCH_ENV, sliderAmLaunchEnv);
    // Multiple env variables can be specified with a comma (,) separator
    String[] envs = StringUtils.isEmpty(sliderAmLaunchEnv) ? null
        : sliderAmLaunchEnv.split(",");
    if (ArrayUtils.isEmpty(envs)) {
      return Collections.emptyMap();
    }
    Map<String, String> amLaunchEnv = new HashMap<>();
    for (String env : envs) {
      if (StringUtils.isNotEmpty(env)) {
        // Each env name/value is separated by equals sign (=)
        String[] tokens = env.split("=");
        if (tokens != null && tokens.length == 2) {
          String envKey = tokens[0];
          String envValue = tokens[1];
          for (Map.Entry<String, String> placeholder : generatePlaceholderKeyValueMap(
              env).entrySet()) {
            if (StringUtils.isNotEmpty(placeholder.getValue())) {
              envValue = envValue.replaceAll(
                  Pattern.quote(placeholder.getKey()), placeholder.getValue());
            }
          }
          if (Shell.WINDOWS) {
            envValue = "%" + envKey + "%;" + envValue;
          } else {
            envValue = "$" + envKey + ":" + envValue;
          }
          log.info("Setting AM launch env {}={}", envKey, envValue);
          amLaunchEnv.put(envKey, envValue);
        }
      }
    }
    return amLaunchEnv;
  }

  protected Map<String, String> generatePlaceholderKeyValueMap(String env) {
    String PLACEHOLDER_PATTERN = "\\$\\{[^{]+\\}";
    Pattern placeholderPattern = Pattern.compile(PLACEHOLDER_PATTERN);
    Matcher placeholderMatcher = placeholderPattern.matcher(env);
    Map<String, String> placeholderKeyValueMap = new HashMap<String, String>();
    if (placeholderMatcher.find()) {
      String placeholderKey = placeholderMatcher.group();
      String systemKey = placeholderKey
          .substring(2, placeholderKey.length() - 1).toUpperCase(Locale.ENGLISH)
          .replaceAll("\\.", "_");
      String placeholderValue = getSystemEnv(systemKey);
      log.debug("Placeholder {}={}", placeholderKey, placeholderValue);
      placeholderKeyValueMap.put(placeholderKey, placeholderValue);
    }
    return placeholderKeyValueMap;
  }

  private void propagatePythonExecutable(Configuration config,
                                         AggregateConf instanceDefinition) {
    String pythonExec = config.get(
        PYTHON_EXECUTABLE_PATH);
    if (pythonExec != null) {
      instanceDefinition.getAppConfOperations().getGlobalOptions().putIfUnset(
          PYTHON_EXECUTABLE_PATH,
          pythonExec);
    }
  }


  /**
   * Wait for the launched app to be accepted in the time  
   * and, optionally running.
   * <p>
   * If the application
   *
   * @param launchedApplication application
   * @param acceptWaitMillis time in millis to wait for accept
   * @param runWaitMillis time in millis to wait for the app to be running.
   * May be null, in which case no wait takes place
   * @return exit code: success
   * @throws YarnException
   * @throws IOException
   */
  public int waitForAppRunning(LaunchedApplication launchedApplication,
      int acceptWaitMillis, int runWaitMillis) throws YarnException, IOException {
    assert launchedApplication != null;
    int exitCode;
    // wait for the submit state to be reached
    ApplicationReport report = launchedApplication.monitorAppToState(
      YarnApplicationState.ACCEPTED,
      new Duration(acceptWaitMillis));

    // may have failed, so check that
    if (hasAppFinished(report)) {
      exitCode = buildExitCode(report);
    } else {
      // exit unless there is a wait


      if (runWaitMillis != 0) {
        // waiting for state to change
        Duration duration = new Duration(runWaitMillis * 1000);
        duration.start();
        report = launchedApplication.monitorAppToState(
          YarnApplicationState.RUNNING, duration);
        if (report != null &&
            report.getYarnApplicationState() == YarnApplicationState.RUNNING) {
          exitCode = EXIT_SUCCESS;
        } else {
          exitCode = buildExitCode(report);
        }
      } else {
        exitCode = EXIT_SUCCESS;
      }
    }
    return exitCode;
  }


  /**
   * Propagate any critical principals from the current site config down to the HBase one.
   * @param config config to read from
   * @param clusterSpec cluster spec
   */
  private void propagatePrincipals(Configuration config,
                                   AggregateConf clusterSpec) {
    String dfsPrincipal = config.get(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY);
    if (dfsPrincipal != null) {
      String siteDfsPrincipal = SITE_XML_PREFIX + DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
      clusterSpec.getAppConfOperations().getGlobalOptions().putIfUnset(
        siteDfsPrincipal,
        dfsPrincipal);
    }
  }

  /**
   * Create a path that must exist in the cluster fs
   * @param uri uri to create
   * @return the path
   * @throws FileNotFoundException if the path does not exist
   */
  public Path createPathThatMustExist(String uri) throws
      SliderException, IOException {
    return sliderFileSystem.createPathThatMustExist(uri);
  }

  /**
   * verify that a live cluster isn't there
   * @param clustername cluster name
   * @param action
   * @throws SliderException with exit code EXIT_CLUSTER_LIVE
   * if a cluster of that name is either live or starting up.
   */
  public void verifyNoLiveClusters(String clustername, String action) throws
                                                       IOException,
                                                       YarnException {
    List<ApplicationReport> existing = findAllLiveInstances(clustername);

    if (!existing.isEmpty()) {
      throw new SliderException(EXIT_APPLICATION_IN_USE,
          action +" failed for "
                              + clustername
                              + ": "
                              + E_CLUSTER_RUNNING + " :" +
                              existing.get(0));
    }
  }

  public String getUsername() throws IOException {
    return RegistryUtils.currentUser();
  }

  /**
   * Get the name of any deployed cluster
   * @return the cluster name
   */
  public String getDeployedClusterName() {
    return deployedClusterName;
  }

  @VisibleForTesting
  public void setDeployedClusterName(String deployedClusterName) {
    this.deployedClusterName = deployedClusterName;
  }

  /**
   * ask if the client is using a mini MR cluster
   * @return true if they are
   */
  private boolean getUsingMiniMRCluster() {
    return getConfig().getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER,
        false);
  }

  /**
   * Get the application name used in the zookeeper root paths
   * @return an application-specific path in ZK
   */
  private String getAppName() {
    return "slider";
  }

  /**
   * Wait for the app to start running (or go past that state)
   * @param duration time to wait
   * @return the app report; null if the duration turned out
   * @throws YarnException YARN or app issues
   * @throws IOException IO problems
   */
  @VisibleForTesting
  public ApplicationReport monitorAppToRunning(Duration duration)
    throws YarnException, IOException {
    return monitorAppToState(YarnApplicationState.RUNNING, duration);
  }

  /**
   * Build an exit code for an application from its report.
   * If the report parameter is null, its interpreted as a timeout
   * @param report report application report
   * @return the exit code
   * @throws IOException
   * @throws YarnException
   */
  private int buildExitCode(ApplicationReport report) throws
                                                      IOException,
                                                      YarnException {
    if (null == report) {
      return EXIT_TIMED_OUT;
    }

    YarnApplicationState state = report.getYarnApplicationState();
    FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
    switch (state) {
      case FINISHED:
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          log.info("Application has completed successfully");
          return EXIT_SUCCESS;
        } else {
          log.info("Application finished unsuccessfully." +
                   "YarnState = {}, DSFinalStatus = {} Breaking monitoring loop",
                   state, dsStatus);
          return EXIT_YARN_SERVICE_FINISHED_WITH_ERROR;
        }

      case KILLED:
        log.info("Application did not finish. YarnState={}, DSFinalStatus={}",
                 state, dsStatus);
        return EXIT_YARN_SERVICE_KILLED;

      case FAILED:
        log.info("Application Failed. YarnState={}, DSFinalStatus={}", state,
                 dsStatus);
        return EXIT_YARN_SERVICE_FAILED;

      default:
        //not in any of these states
        return EXIT_SUCCESS;
    }
  }

  /**
   * Monitor the submitted application for reaching the requested state.
   * Will also report if the app reaches a later state (failed, killed, etc)
   * Kill application if duration!= null & time expires. 
   * Prerequisite: the applicatin was launched.
   * @param desiredState desired state.
   * @param duration how long to wait -must be more than 0
   * @return the application report -null on a timeout
   * @throws YarnException
   * @throws IOException
   */
  @VisibleForTesting
  public ApplicationReport monitorAppToState(
    YarnApplicationState desiredState,
    Duration duration)
    throws YarnException, IOException {
    LaunchedApplication launchedApplication =
      new LaunchedApplication(applicationId, yarnClient);
    return launchedApplication.monitorAppToState(desiredState, duration);
  }

  @Override
  public ApplicationReport getApplicationReport() throws
                                                  IOException,
                                                  YarnException {
    return getApplicationReport(applicationId);
  }

  @Override
  public boolean forceKillApplication(String reason)
    throws YarnException, IOException {
    if (applicationId != null) {
      new LaunchedApplication(applicationId, yarnClient).forceKill(reason);
      return true;
    }
    return false;
  }

  /**
   * List Slider instances belonging to a specific user with a specific app
   * name and within a set of app states.
   * @param user user: "" means all users, null means "default"
   * @param appName name of the application set as a tag
   * @param appStates a set of states the applications should be in
   * @return a possibly empty list of Slider AMs
   */
  public List<ApplicationReport> listSliderInstances(String user,
      String appName, EnumSet<YarnApplicationState> appStates)
      throws YarnException, IOException {
    return yarnAppListClient.listInstances(user, appName, appStates);
  }

  /**
   * A basic list action to list live instances
   * @param clustername cluster name
   * @return success if the listing was considered successful
   * @throws IOException
   * @throws YarnException
   */
  public int actionList(String clustername) throws IOException, YarnException {
    ActionListArgs args = new ActionListArgs();
    args.live = true;
    return actionList(clustername, args);
  }

  /**
   * Implement the list action.
   * @param clustername List out specific instance name
   * @param args Action list arguments
   * @return 0 if one or more entries were listed
   * @throws IOException
   * @throws YarnException
   * @throws UnknownApplicationInstanceException if a specific instance
   * was named but it was not found
   */
  @Override
  public int actionList(String clustername, ActionListArgs args)
      throws IOException, YarnException {
    Set<ApplicationReport> appInstances = getApplicationList(clustername, args);
    if (!appInstances.isEmpty()) {
      return EXIT_SUCCESS;
    } else {
      return EXIT_FALSE;
    }
  }

  /**
   * Retrieve a list of all live instances. If clustername is supplied then it
   * returns this specific cluster, if and only if it exists and is live.
   * 
   * @param clustername
   *          cluster name (if looking for a specific live cluster)
   * @return the list of application names which satisfies the list criteria
   * @throws IOException
   * @throws YarnException
   */
  public Set<ApplicationReport> getApplicationList(String clustername)
      throws IOException, YarnException {
    ActionListArgs args = new ActionListArgs();
    args.live = true;
    return getApplicationList(clustername, args);
  }

  /**
   * Retrieve a list of application instances satisfying the query criteria.
   * 
   * @param clustername
   *          List out specific instance name (set null for all)
   * @param args
   *          Action list arguments
   * @return the list of application names which satisfies the list criteria
   * @throws IOException
   * @throws YarnException
   * @throws UnknownApplicationInstanceException
   *           if a specific instance was named but it was not found
   */
  public Set<ApplicationReport> getApplicationList(String clustername,
      ActionListArgs args) throws IOException, YarnException {
    if (args.help) {
      actionHelp(ACTION_LIST);
      // the above call throws an exception so the return is not really required
      return Collections.emptySet();
    }
    verifyBindingsDefined();

    boolean live = args.live;
    String state = args.state;
    boolean listContainers = args.containers;
    boolean verbose = args.verbose;
    String version = args.version;
    Set<String> components = args.components;

    if (live && !state.isEmpty()) {
      throw new BadCommandArgumentsException(
          Arguments.ARG_LIVE + " and " + Arguments.ARG_STATE + " are exclusive");
    }
    if (listContainers && isUnset(clustername)) {
      throw new BadCommandArgumentsException(
          "Should specify an application instance with "
              + Arguments.ARG_CONTAINERS);
    }
    // specifying both --version and --components with --containers is okay
    if (StringUtils.isNotEmpty(version) && !listContainers) {
      throw new BadCommandArgumentsException(Arguments.ARG_VERSION
          + " can be specified only with " + Arguments.ARG_CONTAINERS);
    }
    if (!components.isEmpty() && !listContainers) {
      throw new BadCommandArgumentsException(Arguments.ARG_COMPONENTS
          + " can be specified only with " + Arguments.ARG_CONTAINERS);
    }

    // flag to indicate only services in a specific state are to be listed
    boolean listOnlyInState = live || !state.isEmpty();

    YarnApplicationState min, max;
    if (live) {
      min = YarnApplicationState.NEW;
      max = YarnApplicationState.RUNNING;
    } else if (!state.isEmpty()) {
      YarnApplicationState stateVal = extractYarnApplicationState(state);
      min = max = stateVal;
    } else {
      min = YarnApplicationState.NEW;
      max = YarnApplicationState.KILLED;
    }
    // get the complete list of persistent instances
    Map<String, Path> persistentInstances = sliderFileSystem.listPersistentInstances();

    if (persistentInstances.isEmpty() && isUnset(clustername)) {
      // an empty listing is a success if no cluster was named
      log.debug("No application instances found");
      return Collections.emptySet();
    }

    // and those the RM knows about
    EnumSet<YarnApplicationState> appStates = EnumSet.range(min, max);
    List<ApplicationReport> instances = listSliderInstances(null, clustername,
        appStates);
    sortApplicationsByMostRecent(instances);
    Map<String, ApplicationReport> reportMap =
        buildApplicationReportMap(instances, min, max);
    log.debug("Persisted {} deployed {} filtered[{}-{}] & de-duped to {}",
        persistentInstances.size(),
        instances.size(),
        min, max,
        reportMap.size() );

    List<ContainerInformation> containers = null;
    if (isSet(clustername)) {
      // only one instance is expected
      // resolve the persistent value
      Path persistent = persistentInstances.get(clustername);
      if (persistent == null) {
        throw unknownClusterException(clustername);
      }
      // create a new map with only that instance in it.
      // this restricts the output of results to this instance
      persistentInstances = new HashMap<>();
      persistentInstances.put(clustername, persistent);
      if (listContainers) {
        containers = getContainers(clustername);
      }
    }
    
    // at this point there is either the entire list or a stripped down instance
    Set<ApplicationReport> listedInstances = new HashSet<ApplicationReport>();
    for (String name : persistentInstances.keySet()) {
      ApplicationReport report = reportMap.get(name);
      if (!listOnlyInState || report != null) {
        // list the details if all were requested, or the filtering contained
        // a report
        listedInstances.add(report);
        // containers will be non-null when only one instance is requested
        String details = instanceDetailsToString(name, report,
            containers, version, components, verbose);
        print(details);
      }
    }
    
    return listedInstances;
  }

  public List<ContainerInformation> getContainers(String name)
      throws YarnException, IOException {
    SliderClusterOperations clusterOps = new SliderClusterOperations(
        bondToCluster(name));
    try {
      return clusterOps.getContainers();
    } catch (NoSuchNodeException e) {
      throw new BadClusterStateException(
          "Containers not found for application instance %s", name);
    }
  }

  /**
   * Enumerate slider instances for the current user, and the
   * most recent app report, where available.
   * @param listOnlyInState boolean to indicate that the instances should
   * only include those in a YARN state
   * <code> minAppState &lt;= currentState &lt;= maxAppState </code>
   *
   * @param minAppState minimum application state to include in enumeration.
   * @param maxAppState maximum application state to include
   * @return a map of application instance name to description
   * @throws IOException Any IO problem
   * @throws YarnException YARN problems
   */
  @Override
  public Map<String, SliderInstanceDescription> enumSliderInstances(
      boolean listOnlyInState,
      YarnApplicationState minAppState,
      YarnApplicationState maxAppState)
      throws IOException, YarnException {
    return yarnAppListClient.enumSliderInstances(listOnlyInState,
        minAppState,
        maxAppState);
  }

  /**
   * Extract the state of a Yarn application --state argument
   * @param state state argument
   * @return the application state
   * @throws BadCommandArgumentsException if the argument did not match
   * any known state
   */
  private YarnApplicationState extractYarnApplicationState(String state) throws
      BadCommandArgumentsException {
    YarnApplicationState stateVal;
    try {
      stateVal = YarnApplicationState.valueOf(state.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new BadCommandArgumentsException("Unknown state: " + state);

    }
    return stateVal;
  }
  
  /**
   * Is an application active: accepted or running
   * @param report the application report
   * @return true if it is running or scheduled to run.
   */
  public boolean isApplicationActive(ApplicationReport report) {
    return report.getYarnApplicationState() == YarnApplicationState.RUNNING
                || report.getYarnApplicationState() == YarnApplicationState.ACCEPTED;
  }

  /**
   * Implement the islive action: probe for a cluster of the given name existing
   * @return exit code
   */

  @Override
  @VisibleForTesting
  public int actionFlex(String name, ActionFlexArgs args) throws YarnException, IOException {
    validateClusterName(name);
    Map<String, String> roleMap = args.getComponentMap();
    // throw usage exception if no changes proposed
    if (roleMap.size() == 0) {
      actionHelp(ACTION_FLEX);
    }
    verifyBindingsDefined();
    log.debug("actionFlex({})", name);
    Map<String, String> roleInstances = new HashMap<>();
    for (Map.Entry<String, String> roleEntry : roleMap.entrySet()) {
      String key = roleEntry.getKey();
      String val = roleEntry.getValue();
      roleInstances.put(key, val);
    }
    return flex(name, roleInstances);
  }

  @Override
  public int actionExists(String name, boolean checkLive) throws YarnException, IOException {
    ActionExistsArgs args = new ActionExistsArgs();
    args.live = checkLive;
    return actionExists(name, args);
  }

  public int actionExists(String name, ActionExistsArgs args) throws YarnException, IOException {
    verifyBindingsDefined();
    validateClusterName(name);
    boolean checkLive = args.live;
    log.debug("actionExists({}, {}, {})", name, checkLive, args.state);

    //initial probe for a cluster in the filesystem
    Path clusterDirectory = sliderFileSystem.buildClusterDirPath(name);
    if (!sliderFileSystem.getFileSystem().exists(clusterDirectory)) {
      throw unknownClusterException(name);
    }
    String state = args.state;
    if (!checkLive && isUnset(state)) {
      log.info("Application {} exists", name);
      return EXIT_SUCCESS;
    }

    //test for liveness/state
    boolean inDesiredState = false;
    ApplicationReport instance;
    instance = findInstance(name);
    if (instance == null) {
      log.info("Application {} not running", name);
      return EXIT_FALSE;
    }
    if (checkLive) {
      // the app exists, check that it is not in any terminated state
      YarnApplicationState appstate = instance.getYarnApplicationState();
      log.debug(" current app state = {}", appstate);
      inDesiredState =
            appstate.ordinal() < YarnApplicationState.FINISHED.ordinal();
    } else {
      // scan for instance in single --state state
      state = state.toUpperCase(Locale.ENGLISH);
      YarnApplicationState desiredState = extractYarnApplicationState(state);
      List<ApplicationReport> userInstances = yarnClient
          .listDeployedInstances("", EnumSet.of(desiredState), name);
      ApplicationReport foundInstance =
          yarnClient.findAppInInstanceList(userInstances, name, desiredState);
      if (foundInstance != null) {
        // found in selected state: success
        inDesiredState = true;
        // mark this as the instance to report
        instance = foundInstance;
      }
    }

    OnDemandReportStringifier report =
        new OnDemandReportStringifier(instance);
    if (!inDesiredState) {
      //cluster in the list of apps but not running
      log.info("Application {} found but is in wrong state {}", name,
          instance.getYarnApplicationState());
      log.debug("State {}", report);
      return EXIT_FALSE;
    } else {
      log.debug("Application instance is in desired state");
      log.info("Application {} is {}\n{}", name,
          instance.getYarnApplicationState(), report);
      return EXIT_SUCCESS;
    }
  }


  @Override
  public int actionKillContainer(String name,
      ActionKillContainerArgs args) throws YarnException, IOException {
    String id = args.id;
    if (isUnset(id)) {
      throw new BadCommandArgumentsException("Missing container id");
    }
    log.info("killingContainer {}:{}", name, id);
    SliderClusterOperations clusterOps =
      new SliderClusterOperations(bondToCluster(name));
    try {
      clusterOps.killContainer(id);
    } catch (NoSuchNodeException e) {
      throw new BadClusterStateException("Container %s not found in cluster %s",
                                         id, name);
    }
    return EXIT_SUCCESS;
  }

  @Override
  public String actionEcho(String name, ActionEchoArgs args) throws
                                                             YarnException,
                                                             IOException {
    String message = args.message;
    if (message == null) {
      throw new BadCommandArgumentsException("missing message");
    }
    SliderClusterOperations clusterOps =
      new SliderClusterOperations(bondToCluster(name));
    return clusterOps.echo(message);
  }

  /**
   * Get at the service registry operations
   * @return registry client -valid after the service is inited.
   */
  public YarnAppListClient getYarnAppListClient() {
    return yarnAppListClient;
  }

  /**
   * Find an instance of an application belonging to the current user.
   * @param appname application name
   * @return the app report or null if none is found
   * @throws YarnException YARN issues
   * @throws IOException IO problems
   */
  public ApplicationReport findInstance(String appname)
      throws YarnException, IOException {
    return findInstance(appname, null);
  }
  
  /**
   * Find an instance of an application belonging to the current user and in
   * specific app states.
   * @param appname application name
   * @param appStates app states in which the application should be in
   * @return the app report or null if none is found
   * @throws YarnException YARN issues
   * @throws IOException IO problems
   */
  public ApplicationReport findInstance(String appname,
      EnumSet<YarnApplicationState> appStates)
      throws YarnException, IOException {
    return yarnAppListClient.findInstance(appname, appStates);
  }

  /**
   * find all live instances of a specific app -if there is >1 in the cluster,
   * this returns them all. State should be running or less
   * @param appname application name
   * @return the list of all matching application instances
   */
  private List<ApplicationReport> findAllLiveInstances(String appname)
    throws YarnException, IOException {
    
    return yarnAppListClient.findAllLiveInstances(appname);
  }

  /**
   * Connect to a Slider AM
   * @param app application report providing the details on the application
   * @return an instance
   * @throws YarnException
   * @throws IOException
   */
  private SliderClusterProtocol connect(ApplicationReport app)
      throws YarnException, IOException {

    try {
      return RpcBinder.getProxy(getConfig(),
                                yarnClient.getRmClient(),
                                app,
                                Constants.CONNECT_TIMEOUT,
                                Constants.RPC_TIMEOUT);
    } catch (InterruptedException e) {
      throw new SliderException(SliderExitCodes.EXIT_TIMED_OUT,
                              e,
                              "Interrupted waiting for communications with the Slider AM");
    }
  }

  @Override
  @VisibleForTesting
  public int actionStatus(String clustername, ActionStatusArgs statusArgs)
      throws YarnException, IOException {
    if (statusArgs.lifetime) {
      queryAndPrintLifetime(clustername);
      return EXIT_SUCCESS;
    }

    ClusterDescription status = verifyAndGetClusterDescription(clustername);
    String outfile = statusArgs.getOutput();
    if (outfile == null) {
      log.info(status.toJsonString());
    } else {
      status.save(new File(outfile).getAbsoluteFile());
    }
    return EXIT_SUCCESS;
  }

  @Override
  public String actionStatus(String clustername)
      throws YarnException, IOException {
    return verifyAndGetClusterDescription(clustername).toJsonString();
  }

  private void queryAndPrintLifetime(String appName)
      throws YarnException, IOException {
    ApplicationReport appReport = findInstance(appName);
    if (appReport == null) {
      throw new YarnException("No application found for " + appName);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter timeoutStr =
        new PrintWriter(new OutputStreamWriter(baos, Charset.forName("UTF-8")));
    try {
      ApplicationTimeout lifetime = appReport.getApplicationTimeouts()
          .get(ApplicationTimeoutType.LIFETIME);
      if (lifetime.getRemainingTime() == -1L) {
        timeoutStr.append(appName + " has no lifetime configured.");
      } else {
        timeoutStr.append("\t" + ApplicationTimeoutType.LIFETIME);
        timeoutStr.print(" expires at : " + lifetime.getExpiryTime());
        timeoutStr.println(
            ".\tRemaining Time : " + lifetime.getRemainingTime() + " seconds");
      }
      System.out.println(baos.toString("UTF-8"));
    } finally {
      timeoutStr.close();
    }
  }

  private ClusterDescription verifyAndGetClusterDescription(String clustername)
      throws YarnException, IOException {
    verifyBindingsDefined();
    validateClusterName(clustername);
    return getClusterDescription(clustername);
  }

  @Override
  public int actionVersion() {
    SliderVersionInfo.loadAndPrintVersionInfo(log);
    return EXIT_SUCCESS;
  }

  @Override
  public int actionFreeze(String clustername,
      ActionFreezeArgs freezeArgs) throws YarnException, IOException {
    verifyBindingsDefined();
    validateClusterName(clustername);
    int waittime = freezeArgs.getWaittime();
    String text = freezeArgs.message;
    boolean forcekill = freezeArgs.force;
    log.debug("actionFreeze({}, reason={}, wait={}, force={})", clustername,
              text,
              waittime,
              forcekill);
    
    //is this actually a known cluster?
    sliderFileSystem.locateInstanceDefinition(clustername);
    ApplicationReport app = findInstance(clustername);
    if (app == null) {
      // exit early
      log.info("Cluster {} not running", clustername);
      // not an error to stop a stopped cluster
      return EXIT_SUCCESS;
    }
    log.debug("App to stop was found: {}:\n{}", clustername,
              new OnDemandReportStringifier(app));
    if (app.getYarnApplicationState().ordinal() >=
        YarnApplicationState.FINISHED.ordinal()) {
      log.info("Cluster {} is in a terminated state {}", clustername,
               app.getYarnApplicationState());
      return EXIT_SUCCESS;
    }

    // IPC request for a managed shutdown is only possible if the app is running.
    // so we need to force kill if the app is accepted or submitted
    if (!forcekill
        && app.getYarnApplicationState().ordinal() < YarnApplicationState.RUNNING.ordinal()) {
      log.info("Cluster {} is in a pre-running state {}. Force killing it", clustername,
          app.getYarnApplicationState());
      forcekill = true;
    }

    LaunchedApplication application = new LaunchedApplication(yarnClient, app);
    applicationId = application.getApplicationId();
    
    if (forcekill) {
      // escalating to forced kill
      application.kill("Forced stop of " + clustername + ": " + text);
    } else {
      try {
        SliderClusterProtocol appMaster = connect(app);
        Messages.StopClusterRequestProto r =
          Messages.StopClusterRequestProto
                  .newBuilder()
                  .setMessage(text)
                  .build();
        appMaster.stopCluster(r);

        log.debug("Cluster stop command issued");

      } catch (YarnException e) {
        log.warn("Exception while trying to terminate {}", clustername, e);
        return EXIT_FALSE;
      } catch (IOException e) {
        log.warn("Exception while trying to terminate {}", clustername, e);
        return EXIT_FALSE;
      }
    }

    //wait for completion. We don't currently return an exception during this process
    //as the stop operation has been issued, this is just YARN.
    try {
      if (waittime > 0) {
        ApplicationReport applicationReport =
          application.monitorAppToState(YarnApplicationState.FINISHED,
                                        new Duration(waittime * 1000));
        if (applicationReport == null) {
          log.info("application did not shut down in time");
          return EXIT_FALSE;
        }
      }
      
    } catch (YarnException | IOException e) {
      log.warn("Exception while waiting for the application {} to shut down: {}",
               clustername, e);
    }

    return EXIT_SUCCESS;
  }

  @Override
  public int actionThaw(String clustername, ActionThawArgs thaw) throws YarnException, IOException {
    validateClusterName(clustername);
    verifyBindingsDefined();
    // see if it is actually running and bail out;
    verifyNoLiveClusters(clustername, "Start");

    //start the cluster
    return startCluster(clustername, thaw, thaw.lifetime);
  }

  /**
   * Implement flexing
   * @param clustername name of the cluster
   * @param roleInstances map of new role instances
   * @return EXIT_SUCCESS if the #of nodes in a live cluster changed
   * @throws YarnException
   * @throws IOException
   */
  public int flex(String clustername, Map<String, String> roleInstances)
      throws YarnException, IOException {
    verifyBindingsDefined();
    validateClusterName(clustername);
    Path clusterDirectory = sliderFileSystem.buildClusterDirPath(clustername);
    AggregateConf instanceDefinition = loadInstanceDefinitionUnresolved(
      clustername,
      clusterDirectory);

    ConfTreeOperations resources =
      instanceDefinition.getResourceOperations();
    for (Map.Entry<String, String> entry : roleInstances.entrySet()) {
      String role = entry.getKey();
      String updateCountStr = entry.getValue();
      int currentCount = 0;
      MapOperations component = resources.getOrAddComponent(role);
      try {
        // check if a relative count is specified
        if (updateCountStr.startsWith("+") || updateCountStr.startsWith("-")) {
          int updateCount = Integer.parseInt(updateCountStr);
          // if component was specified before, get the current count
          if (component.get(COMPONENT_INSTANCES) != null) {
            currentCount = Integer.parseInt(component.get(COMPONENT_INSTANCES));
            if (currentCount + updateCount < 0) {
              throw new BadCommandArgumentsException("The requested count " +
                  "of \"%s\" for role %s makes the total number of " +
                  "instances negative: \"%s\"", updateCount, role,
                  currentCount+updateCount);
            }
            else {
              component.put(COMPONENT_INSTANCES,
                            Integer.toString(currentCount+updateCount));
            }
          }
          else {
            if (updateCount < 0) {
                throw new BadCommandArgumentsException("Invalid to request " +
                    "negative count of \"%s\" for role %s", updateCount, role);
            }
            else {
              Map<String, String> map = new HashMap<>();
              resources.confTree.components.put(role, map);
              component = new MapOperations(role, map);
              component.put(COMPONENT_INSTANCES, Integer.toString(updateCount));
            }
          }
        }
        else {
          int count = Integer.parseInt(updateCountStr);
          resources.getOrAddComponent(role).put(COMPONENT_INSTANCES,
                                                Integer.toString(count));
        }
      }
      catch (NumberFormatException e) {
        throw new BadCommandArgumentsException("Requested count of role %s" +
                                               " is not a number: \"%s\"",
                                               role, updateCountStr);
      }

      log.debug("Flexed cluster specification ( {} -> {}) : \n{}",
                role,
                updateCountStr,
                resources);
    }
    SliderAMClientProvider sliderAM = new SliderAMClientProvider(getConfig());
    AbstractClientProvider provider = createClientProvider(
        instanceDefinition.getInternalOperations().getGlobalOptions().getMandatoryOption(
            INTERNAL_PROVIDER_NAME));
    // slider provider to validate what there is
    validateInstanceDefinition(sliderAM, instanceDefinition, sliderFileSystem);
    validateInstanceDefinition(provider, instanceDefinition, sliderFileSystem);

    int exitCode = EXIT_FALSE;
    // save the specification
    try {
      InstanceIO.saveInstanceDefinition(sliderFileSystem, clusterDirectory,
          instanceDefinition);
    } catch (LockAcquireFailedException e) {
      // lock failure
      log.debug("Failed to lock dir {}", clusterDirectory, e);
      log.warn("Failed to save new resource definition to {} : {}", clusterDirectory, e);
    }

    // now see if it is actually running and tell it about the update if it is
    ApplicationReport instance = findInstance(clustername);
    if (instance != null) {
      log.info("Flexing running cluster");
      SliderClusterProtocol appMaster = connect(instance);
      SliderClusterOperations clusterOps = new SliderClusterOperations(appMaster);
      clusterOps.flex(instanceDefinition.getResources());
      log.info("application instance size updated");
      exitCode = EXIT_SUCCESS;
    } else {
      log.info("No running instance to update");
    }
    return exitCode;
  }

  /**
   * Validate an instance definition against a provider.
   * @param provider the provider performing the validation
   * @param instanceDefinition the instance definition
   * @throws SliderException if invalid.
   */
  protected void validateInstanceDefinition(AbstractClientProvider provider,
      AggregateConf instanceDefinition, SliderFileSystem fs) throws SliderException {
    try {
      provider.validateInstanceDefinition(instanceDefinition, fs);
    } catch (SliderException e) {
      //problem, reject it
      log.info("Error {} validating application instance definition ", e.getMessage());
      log.debug("Error validating application instance definition ", e);
      log.info(instanceDefinition.toString());
      throw e;
    }
  }


  /**
   * Load the persistent cluster description
   * @param clustername name of the cluster
   * @return the description in the filesystem
   * @throws IOException any problems loading -including a missing file
   */
  @VisibleForTesting
  public AggregateConf loadPersistedClusterDescription(String clustername)
      throws IOException, SliderException, LockAcquireFailedException {
    Path clusterDirectory = sliderFileSystem.buildClusterDirPath(clustername);
    ConfPersister persister = new ConfPersister(sliderFileSystem, clusterDirectory);
    AggregateConf instanceDescription = new AggregateConf();
    persister.load(instanceDescription);
    return instanceDescription;
  }

    /**
     * Connect to a live cluster and get its current state
     * @param clustername the cluster name
     * @return its description
     */
  @VisibleForTesting
  public ClusterDescription getClusterDescription(String clustername) throws
                                                                 YarnException,
                                                                 IOException {
    SliderClusterOperations clusterOperations =
      createClusterOperations(clustername);
    return clusterOperations.getClusterDescription();
  }

  /**
   * Connect to the cluster and get its current state
   * @return its description
   */
  @VisibleForTesting
  public ClusterDescription getClusterDescription() throws
                                               YarnException,
                                               IOException {
    return getClusterDescription(getDeployedClusterName());
  }

  /**
   * List all node UUIDs in a role
   * @param role role name or "" for all
   * @return an array of UUID strings
   * @throws IOException
   * @throws YarnException
   */
  @VisibleForTesting
  public String[] listNodeUUIDsByRole(String role) throws
                                               IOException,
                                               YarnException {
    return createClusterOperations()
              .listNodeUUIDsByRole(role);
  }

  /**
   * List all nodes in a role. This is a double round trip: once to list
   * the nodes in a role, another to get their details
   * @param role component/role to look for
   * @return an array of ContainerNode instances
   * @throws IOException
   * @throws YarnException
   */
  @VisibleForTesting
  public List<ClusterNode> listClusterNodesInRole(String role) throws
                                               IOException,
                                               YarnException {
    return createClusterOperations().listClusterNodesInRole(role);
  }

  /**
   * Get the details on a list of uuids
   * @param uuids uuids to ask for 
   * @return a possibly empty list of node details
   * @throws IOException
   * @throws YarnException
   */
  @VisibleForTesting
  public List<ClusterNode> listClusterNodes(String[] uuids) throws
                                               IOException,
                                               YarnException {

    if (uuids.length == 0) {
      // short cut on an empty list
      return new LinkedList<>();
    }
    return createClusterOperations().listClusterNodes(uuids);
  }

  /**
   * Get the instance definition from the far end
   */
  @VisibleForTesting
  public AggregateConf getLiveInstanceDefinition() throws IOException, YarnException {
    return createClusterOperations().getInstanceDefinition();
  }

  /**
   * Bond to a running cluster
   * @param clustername cluster name
   * @return the AM RPC client
   * @throws SliderException if the cluster is unkown
   */
  private SliderClusterProtocol bondToCluster(String clustername) throws
                                                                  YarnException,
                                                                  IOException {
    verifyBindingsDefined();
    if (clustername == null) {
      throw unknownClusterException("(undefined)");
    }
    ApplicationReport instance = findInstance(clustername,
        SliderUtils.getAllLiveAppStates());
    if (null == instance) {
      throw unknownClusterException(clustername);
    }
    return connect(instance);
  }

  /**
   * Create a cluster operations instance against a given cluster
   * @param clustername cluster name
   * @return a bonded cluster operations instance
   * @throws YarnException YARN issues
   * @throws IOException IO problems
   */
  private SliderClusterOperations createClusterOperations(String clustername) throws
                                                                            YarnException,
                                                                            IOException {
    SliderClusterProtocol sliderAM = bondToCluster(clustername);
    return new SliderClusterOperations(sliderAM);
  }

  /**
   * Create a cluster operations instance against the active cluster
   * -returning any previous created one if held.
   * @return a bonded cluster operations instance
   * @throws YarnException YARN issues
   * @throws IOException IO problems
   */
  private SliderClusterOperations createClusterOperations() throws
                                                         YarnException,
                                                         IOException {
    if (sliderClusterOperations == null) {
      sliderClusterOperations =
        createClusterOperations(getDeployedClusterName());
    }
    return sliderClusterOperations;
  }

  /**
   * Wait for an instance of a named role to be live (or past it in the lifecycle)
   * @param role role to look for
   * @param timeout time to wait
   * @return the state. If still in CREATED, the cluster didn't come up
   * in the time period. If LIVE, all is well. If >LIVE, it has shut for a reason
   * @throws IOException IO
   * @throws SliderException Slider
   * @throws WaitTimeoutException if the wait timed out
   */
  @VisibleForTesting
  public int waitForRoleInstanceLive(String role, long timeout)
    throws WaitTimeoutException, IOException, YarnException {
    return createClusterOperations().waitForRoleInstanceLive(role, timeout);
  }

  /**
   * Generate an exception for an unknown cluster
   * @param clustername cluster name
   * @return an exception with text and a relevant exit code
   */
  public UnknownApplicationInstanceException unknownClusterException(String clustername) {
    return UnknownApplicationInstanceException.unknownInstance(clustername);
  }

  @Override
  public String toString() {
    return "Slider Client in state " + getServiceState()
           + " and Slider Application Instance " + deployedClusterName;
  }

  /**
   * Get all YARN applications
   * @return a possibly empty list
   * @throws YarnException
   * @throws IOException
   */
  @VisibleForTesting
  public List<ApplicationReport> getApplications()
      throws YarnException, IOException {
    return yarnClient.getApplications();
  }

  @VisibleForTesting
  public ApplicationReport getApplicationReport(ApplicationId appId)
    throws YarnException, IOException {
    return new LaunchedApplication(appId, yarnClient).getApplicationReport();
  }

  /**
   * The configuration used for deployment (after resolution).
   * Non-null only after the client has launched the application
   * @return the resolved configuration or null
   */
  @VisibleForTesting
  public AggregateConf getLaunchedInstanceDefinition() {
    return launchedInstanceDefinition;
  }


  @Override
  public int actionResolve(ActionResolveArgs args)
      throws YarnException, IOException {
    // as this is an API entry point, validate
    // the arguments
    args.validate();
    String path = SliderRegistryUtils.resolvePath(args.path);
    ServiceRecordMarshal serviceRecordMarshal = new ServiceRecordMarshal();
    try {
      if (args.list) {
        File destDir = args.destdir;
        if (destDir != null && !destDir.exists() && !destDir.mkdirs()) {
          throw new IOException("Failed to create directory: " + destDir);
        }


        Map<String, ServiceRecord> recordMap;
        Map<String, RegistryPathStatus> znodes;
        try {
          znodes = statChildren(registryOperations, path);
          recordMap = extractServiceRecords(registryOperations,
              path,
              znodes.values());
        } catch (PathNotFoundException e) {
          // treat the root directory as if if is always there
        
          if ("/".equals(path)) {
            znodes = new HashMap<>(0);
            recordMap = new HashMap<>(0);
          } else {
            throw e;
          }
        }
        // subtract all records from the znodes map to get pure directories
        log.info("Entries: {}", znodes.size());

        for (String name : znodes.keySet()) {
          println("  " + name);
        }
        println("");

        log.info("Service records: {}", recordMap.size());
        for (Entry<String, ServiceRecord> recordEntry : recordMap.entrySet()) {
          String name = recordEntry.getKey();
          ServiceRecord instance = recordEntry.getValue();
          String json = serviceRecordMarshal.toJson(instance);
          if (destDir == null) {
            println(name);
            println(json);
          } else {
            String filename = RegistryPathUtils.lastPathEntry(name) + ".json";
            File jsonFile = new File(destDir, filename);
            write(jsonFile, serviceRecordMarshal.toBytes(instance));
          }
        }
      } else  {
        // resolve single entry
        ServiceRecord instance = resolve(path);
        File outFile = args.out;
        if (args.destdir != null) {
          outFile = new File(args.destdir, RegistryPathUtils.lastPathEntry(path));
        }
        if (outFile != null) {
          write(outFile, serviceRecordMarshal.toBytes(instance));
        } else {
          println(serviceRecordMarshal.toJson(instance));
        }
      }
    } catch (PathNotFoundException | NoRecordException e) {
      // no record at this path
      throw new NotFoundException(e, path);
    }
    return EXIT_SUCCESS;
  }

  @Override
  public int actionRegistry(ActionRegistryArgs registryArgs) throws
      YarnException,
      IOException {
    // as this is also a test entry point, validate
    // the arguments
    registryArgs.validate();
    try {
      if (registryArgs.list) {
        actionRegistryList(registryArgs);
      } else if (registryArgs.listConf) {
        // list the configurations
        actionRegistryListConfigsYarn(registryArgs);
      } else if (registryArgs.listExports) {
        // list the exports
        actionRegistryListExports(registryArgs);
      } else if (isSet(registryArgs.getConf)) {
        // get a configuration
        PublishedConfiguration publishedConfiguration =
            actionRegistryGetConfig(registryArgs);
        outputConfig(publishedConfiguration, registryArgs);
      } else if (isSet(registryArgs.getExport)) {
        // get a export group
        PublishedExports publishedExports =
            actionRegistryGetExport(registryArgs);
        outputExport(publishedExports, registryArgs);
      } else {
        // it's an unknown command
        log.info(CommonArgs.usage(serviceArgs, ACTION_DIAGNOSTICS));
        return EXIT_USAGE;
      }
//      JDK7
    } catch (FileNotFoundException e) {
      log.info("{}", e.toString());
      log.debug("{}", e, e);
      return EXIT_NOT_FOUND;
    } catch (PathNotFoundException e) {
      log.info("{}", e.toString());
      log.debug("{}", e, e);
      return EXIT_NOT_FOUND;
    }
    return EXIT_SUCCESS;
  }

  /**
   * Registry operation
   *
   * @param registryArgs registry Arguments
   * @return the instances (for tests)
   * @throws YarnException YARN problems
   * @throws IOException Network or other problems
   */
  @VisibleForTesting
  public Collection<ServiceRecord> actionRegistryList(
      ActionRegistryArgs registryArgs)
      throws YarnException, IOException {
    String serviceType = registryArgs.serviceType;
    String name = registryArgs.name;
    RegistryOperations operations = getRegistryOperations();
    Collection<ServiceRecord> serviceRecords;
    if (StringUtils.isEmpty(name)) {
      String path = serviceclassPath(currentUser(), serviceType);

      try {
        Map<String, ServiceRecord> recordMap =
            listServiceRecords(operations, path);
        if (recordMap.isEmpty()) {
          throw new UnknownApplicationInstanceException(
              "No applications registered under " + path);
        }
        serviceRecords = recordMap.values();
      } catch (PathNotFoundException e) {
        throw new NotFoundException(path, e);
      }
    } else {
      ServiceRecord instance = lookupServiceRecord(registryArgs);
      serviceRecords = new ArrayList<>(1);
      serviceRecords.add(instance);
    }

    for (ServiceRecord serviceRecord : serviceRecords) {
      logInstance(serviceRecord, registryArgs.verbose);
    }
    return serviceRecords;
  }

  @Override
  public int actionDiagnostic(ActionDiagnosticArgs diagnosticArgs) {
    try {
      if (diagnosticArgs.client) {
        actionDiagnosticClient(diagnosticArgs);
      } else if (diagnosticArgs.application) {
        actionDiagnosticApplication(diagnosticArgs);
      } else if (diagnosticArgs.yarn) {
        actionDiagnosticYarn(diagnosticArgs);
      } else if (diagnosticArgs.credentials) {
        actionDiagnosticCredentials();
      } else if (diagnosticArgs.all) {
        actionDiagnosticAll(diagnosticArgs);
      } else if (diagnosticArgs.level) {
        actionDiagnosticIntelligent(diagnosticArgs);
      } else {
        // it's an unknown option
        log.info(CommonArgs.usage(serviceArgs, ACTION_DIAGNOSTICS));
        return EXIT_USAGE;
      }
    } catch (Exception e) {
      log.error(e.toString());
      return EXIT_FALSE;
    }
    return EXIT_SUCCESS;
  }

  private void actionDiagnosticIntelligent(ActionDiagnosticArgs diagnosticArgs)
      throws YarnException, IOException, URISyntaxException {
    // not using member variable clustername because we want to place
    // application name after --application option and member variable
    // cluster name has to be put behind action
    String clusterName = diagnosticArgs.name;
    requireArgumentSet(Arguments.ARG_NAME, clusterName);

    try {
      validateClientConfigFile();
      log.info("Slider-client.xml is accessible");
    } catch (IOException e) {
      // we are catching exceptions here because those are indication of
      // validation result, and we need to print them here
      log.error("validation of slider-client.xml fails because: " + e, e);
      return;
    }
    SliderClusterOperations clusterOperations = createClusterOperations(clusterName);
    // cluster not found exceptions will be thrown upstream
    ClusterDescription clusterDescription = clusterOperations
        .getClusterDescription();
    log.info("Slider AppMaster is accessible");

    if (clusterDescription.state == StateValues.STATE_LIVE) {
      AggregateConf instanceDefinition = clusterOperations
          .getInstanceDefinition();
      String imagePath = instanceDefinition.getInternalOperations().get(
          INTERNAL_APPLICATION_IMAGE_PATH);
      // if null, that means slider uploaded the agent tarball for the user
      // and we need to use where slider has put
      if (imagePath == null) {
        ApplicationReport appReport = findInstance(clusterName);
        Path path1 = sliderFileSystem.getTempPathForCluster(clusterName);
        if (appReport != null) {
          Path subPath = new Path(path1, appReport.getApplicationId()
              .toString() + "/agent");
          imagePath = subPath.toString();
          String pathStr = imagePath + "/" + AgentKeys.AGENT_TAR;
          try {
            validateHDFSFile(sliderFileSystem, pathStr);
            log.info("Slider agent package is properly installed at " + pathStr);
          } catch (FileNotFoundException e) {
            log.error("can not find agent package: {}", pathStr, e);
            return;
          } catch (IOException e) {
            log.error("can not open agent package: {}", pathStr, e);
            return;
          }
        }
      }

      String pkgTarballPath = getApplicationDefinitionPath(instanceDefinition
              .getAppConfOperations());
      try {
        validateHDFSFile(sliderFileSystem, pkgTarballPath);
        log.info("Application package is properly installed");
      } catch (FileNotFoundException e) {
        log.error("can not find application package: {}", pkgTarballPath,  e);
      } catch (IOException e) {
        log.error("can not open application package: {} ", pkgTarballPath, e);
      }
    }
  }

  private void actionDiagnosticAll(ActionDiagnosticArgs diagnosticArgs)
      throws IOException, YarnException {
    // assign application name from param to each sub diagnostic function
    actionDiagnosticClient(diagnosticArgs);
    actionDiagnosticApplication(diagnosticArgs);
    actionDiagnosticSlider(diagnosticArgs);
    actionDiagnosticYarn(diagnosticArgs);
    actionDiagnosticCredentials();
  }

  private void actionDiagnosticCredentials() throws BadConfigException,
      IOException {
    if (isHadoopClusterSecure(loadSliderClientXML())) {
      String credentialCacheFileDescription = null;
      try {
        credentialCacheFileDescription = checkCredentialCacheFile();
      } catch (BadConfigException e) {
        log.error("The credential config is not valid: " + e.toString());
        throw e;
      } catch (IOException e) {
        log.error("Unable to read the credential file: " + e.toString());
        throw e;
      }
      log.info("Credential cache file for the current user: "
          + credentialCacheFileDescription);
    } else {
      log.info("the cluster is not in secure mode");
    }
  }

  private void actionDiagnosticYarn(ActionDiagnosticArgs diagnosticArgs)
      throws IOException, YarnException {
    JSONObject converter = null;
    log.info("the node in the YARN cluster has below state: ");
    List<NodeReport> yarnClusterInfo;
    try {
      yarnClusterInfo = yarnClient.getNodeReports(NodeState.RUNNING);
    } catch (YarnException e1) {
      log.error("Exception happened when fetching node report from the YARN cluster: "
          + e1.toString());
      throw e1;
    } catch (IOException e1) {
      log.error("Network problem happened when fetching node report YARN cluster: "
          + e1.toString());
      throw e1;
    }
    for (NodeReport nodeReport : yarnClusterInfo) {
      log.info(nodeReport.toString());
    }

    if (diagnosticArgs.verbose) {
      Writer configWriter = new StringWriter();
      try {
        Configuration.dumpConfiguration(yarnClient.getConfig(), configWriter);
      } catch (IOException e1) {
        log.error("Network problem happened when retrieving YARN config from YARN: "
            + e1.toString());
        throw e1;
      }
      try {
        converter = new JSONObject(configWriter.toString());
        log.info("the configuration of the YARN cluster is: "
            + converter.toString(2));

      } catch (JSONException e) {
        log.error("JSONException happened during parsing response from YARN: "
            + e.toString());
      }
    }
  }

  private void actionDiagnosticSlider(ActionDiagnosticArgs diagnosticArgs)
      throws YarnException, IOException {
    // not using member variable clustername because we want to place
    // application name after --application option and member variable
    // cluster name has to be put behind action
    String clusterName = diagnosticArgs.name;
    if(isUnset(clusterName)){
      throw new BadCommandArgumentsException("application name must be provided with --name option");
    }
    AggregateConf instanceDefinition = fetchInstanceDefinition(clusterName);
    String imagePath = instanceDefinition.getInternalOperations().get(
        INTERNAL_APPLICATION_IMAGE_PATH);
    // if null, it will be uploaded by Slider and thus at slider's path
    if (imagePath == null) {
      ApplicationReport appReport = findInstance(clusterName);
      if (appReport != null) {
        Path path1 = sliderFileSystem.getTempPathForCluster(clusterName);
        Path subPath = new Path(path1, appReport.getApplicationId().toString()
            + "/agent");
        imagePath = subPath.toString();
      }
    }
    log.info("The path of slider agent tarball on HDFS is: " + imagePath);
  }

  private AggregateConf fetchInstanceDefinition(String clusterName)
      throws YarnException, IOException {
    SliderClusterOperations clusterOperations;
    AggregateConf instanceDefinition = null;
    try {
      clusterOperations = createClusterOperations(clusterName);
      instanceDefinition = clusterOperations.getInstanceDefinition();
    } catch (YarnException | IOException e) {
      log.error("Failed to retrieve instance definition from YARN: "
          + e.toString());
      throw e;
    }
    return instanceDefinition;
  }

  private void actionDiagnosticApplication(ActionDiagnosticArgs diagnosticArgs)
      throws YarnException, IOException {
    // not using member variable clustername because we want to place
    // application name after --application option and member variable
    // cluster name has to be put behind action
    String clusterName = diagnosticArgs.name;
    requireArgumentSet(Arguments.ARG_NAME, clusterName);
    AggregateConf instanceDefinition = fetchInstanceDefinition(clusterName);
    String clusterDir = instanceDefinition.getAppConfOperations()
        .getGlobalOptions().get(AgentKeys.APP_ROOT);
    String pkgTarball = getApplicationDefinitionPath(instanceDefinition.getAppConfOperations());
    String runAsUser = instanceDefinition.getAppConfOperations()
        .getGlobalOptions().get(AgentKeys.RUNAS_USER);

    log.info("The location of the cluster instance directory in HDFS is: {}", clusterDir);
    log.info("The name of the application package tarball on HDFS is: {}",pkgTarball);
    log.info("The runas user of the application in the cluster is: {}",runAsUser);

    if (diagnosticArgs.verbose) {
      log.info("App config of the application:\n{}",
          instanceDefinition.getAppConf().toJson());
      log.info("Resource config of the application:\n{}",
          instanceDefinition.getResources().toJson());
    }
  }

  private void actionDiagnosticClient(ActionDiagnosticArgs diagnosticArgs)
      throws SliderException, IOException {
    try {
      String currentCommandPath = getCurrentCommandPath();
      SliderVersionInfo.loadAndPrintVersionInfo(log);
      String clientConfigPath = getClientConfigPath();
      String jdkInfo = getJDKInfo();
      println("The slider command path: %s", currentCommandPath);
      println("The slider-client.xml used by current running command path: %s",
          clientConfigPath);
      println(jdkInfo);

      // security info
      Configuration config = getConfig();
      if (isHadoopClusterSecure(config)) {
        println("Hadoop Cluster is secure");
        println("Login user is %s", UserGroupInformation.getLoginUser());
        println("Current user is %s", UserGroupInformation.getCurrentUser());

      } else {
        println("Hadoop Cluster is insecure");
      }

      // verbose?
      if (diagnosticArgs.verbose) {
        // do the environment
        Map<String, String> env = getSystemEnv();
        Set<String> envList = ConfigHelper.sortedConfigKeys(env.entrySet());
        StringBuilder builder = new StringBuilder("Environment variables:\n");
        for (String key : envList) {
          builder.append(key).append("=").append(env.get(key)).append("\n");
        }
        println(builder.toString());

        // Java properties
        builder = new StringBuilder("JVM Properties\n");
        Map<String, String> props =
            sortedMap(toMap(System.getProperties()));
        for (Entry<String, String> entry : props.entrySet()) {
          builder.append(entry.getKey()).append("=")
                 .append(entry.getValue()).append("\n");
        }
        
        println(builder.toString());

        // then the config
        println("Slider client configuration:\n" + ConfigHelper.dumpConfigToString(config));
      }

      validateSliderClientEnvironment(log);
    } catch (SliderException | IOException e) {
      log.error(e.toString());
      throw e;
    }

  }

  /**
   * Kerberos Diagnostics
   * @param args CLI arguments
   * @return exit code
   * @throws SliderException
   * @throws IOException
   */
  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  private int actionKDiag(ActionKDiagArgs args)
    throws Exception {
    PrintStream out;
    boolean closeStream = false;
    if (args.out != null) {
      out = new PrintStream(args.out, "UTF-8");
      closeStream = true;
    } else {
      out = System.err;
    }
    try {
      KerberosDiags kdiags = new KerberosDiags(getConfig(),
          out,
          args.services,
          args.keytab,
          args.principal,
          args.keylen,
          args.secure);
      kdiags.execute();
    } catch (KerberosDiags.KerberosDiagsFailure e) {
      log.error(e.toString());
      log.debug(e.toString(), e);
      throw e;
    } catch (Exception e) {
      log.error("Kerberos Diagnostics", e);
      throw e;
    } finally {
      if (closeStream) {
        out.flush();
        out.close();
      }
    }
    return 0;
  }

  /**
   * Log a service record instance
   * @param instance record
   * @param verbose verbose logging of all external endpoints
   */
  private void logInstance(ServiceRecord instance,
      boolean verbose) {
    if (!verbose) {
      log.info("{}", instance.get(YarnRegistryAttributes.YARN_ID, ""));
    } else {
      log.info("{}: ", instance.get(YarnRegistryAttributes.YARN_ID, ""));
      logEndpoints(instance);
    }
  }

  /**
   * Log the external endpoints of a service record
   * @param instance service record instance
   */
  private void logEndpoints(ServiceRecord instance) {
    List<Endpoint> endpoints = instance.external;
    for (Endpoint endpoint : endpoints) {
      log.info(endpoint.toString());
    }
  }

 /**
   * list configs available for an instance
   *
   * @param registryArgs registry Arguments
   * @throws YarnException YARN problems
   * @throws IOException Network or other problems
   */
  public void actionRegistryListConfigsYarn(ActionRegistryArgs registryArgs)
      throws YarnException, IOException {

    ServiceRecord instance = lookupServiceRecord(registryArgs);

    RegistryRetriever retriever = new RegistryRetriever(getConfig(), instance);
    PublishedConfigSet configurations =
        retriever.getConfigurations(!registryArgs.internal);
    PrintStream out = null;
    try {
      if (registryArgs.out != null) {
        out = new PrintStream(registryArgs.out, "UTF-8");
      } else {
        out = System.out;
      }
      for (String configName : configurations.keys()) {
        if (!registryArgs.verbose) {
          out.println(configName);
        } else {
          PublishedConfiguration published = configurations.get(configName);
          out.printf("%s: %s%n", configName, published.description);
        }
      }
    } finally {
      if (registryArgs.out != null && out != null) {
        out.flush();
        out.close();
      }
    }
  }

  /**
   * list exports available for an instance
   *
   * @param registryArgs registry Arguments
   * @throws YarnException YARN problems
   * @throws IOException Network or other problems
   */
  public void actionRegistryListExports(ActionRegistryArgs registryArgs)
      throws YarnException, IOException {
    ServiceRecord instance = lookupServiceRecord(registryArgs);

    RegistryRetriever retriever = new RegistryRetriever(getConfig(), instance);
    PublishedExportsSet exports =
        retriever.getExports(!registryArgs.internal);
    PrintStream out = null;
    boolean streaming = false;
    try {
      if (registryArgs.out != null) {
        out = new PrintStream(registryArgs.out, "UTF-8");
        streaming = true;
        log.debug("Saving output to {}", registryArgs.out);
      } else {
        out = System.out;
      }
      log.debug("Number of exports: {}", exports.keys().size());
      for (String exportName : exports.keys()) {
        if (streaming) {
          log.debug(exportName);
        }
        if (!registryArgs.verbose) {
          out.println(exportName);
        } else {
          PublishedExports published = exports.get(exportName);
          out.printf("%s: %s%n", exportName, published.description);
        }
      }
    } finally {
      if (streaming) {
        out.flush();
        out.close();
      }
    }
  }

  /**
   * list configs available for an instance
   *
   * @param registryArgs registry Arguments
   * @throws YarnException YARN problems
   * @throws IOException Network or other problems
   * @throws FileNotFoundException if the config is not found
   */
  @VisibleForTesting
  public PublishedConfiguration actionRegistryGetConfig(ActionRegistryArgs registryArgs)
      throws YarnException, IOException {
    return ClientUtils.getConfigFromRegistry(getRegistryOperations(),
        getConfig(), registryArgs.getConf, registryArgs.name, registryArgs.user,
        !registryArgs.internal);
  }

  /**
   * get a specific export group
   *
   * @param registryArgs registry Arguments
   *
   * @throws YarnException         YARN problems
   * @throws IOException           Network or other problems
   * @throws FileNotFoundException if the config is not found
   */
  @VisibleForTesting
  public PublishedExports actionRegistryGetExport(ActionRegistryArgs registryArgs)
      throws YarnException, IOException {
    ServiceRecord instance = lookupServiceRecord(registryArgs);

    RegistryRetriever retriever = new RegistryRetriever(getConfig(), instance);
    boolean external = !registryArgs.internal;
    PublishedExportsSet exports = retriever.getExports(external);

    PublishedExports published = retriever.retrieveExports(exports,
                                                           registryArgs.getExport,
                                                           external);
    return published;
  }

  /**
   * write out the config. If a destination is provided and that dir is a
   * directory, the entry is written to it with the name provided + extension,
   * else it is printed to standard out.
   * @param published published config
   * @param registryArgs registry Arguments
   * @throws BadCommandArgumentsException
   * @throws IOException
   */
  private void outputConfig(PublishedConfiguration published,
      ActionRegistryArgs registryArgs) throws
      BadCommandArgumentsException,
      IOException {
    // decide whether or not to print
    String entry = registryArgs.getConf;
    String format = registryArgs.format;
    String output = ClientUtils.saveOrReturnConfig(published,
        registryArgs.format, registryArgs.out, entry + "." + format);
    if (output != null) {
      print(output);
    }
  }

  /**
   * write out the config
   * @param published
   * @param registryArgs
   * @throws BadCommandArgumentsException
   * @throws IOException
   */
  private void outputExport(PublishedExports published,
                            ActionRegistryArgs registryArgs) throws
      BadCommandArgumentsException,
      IOException {
    // decide whether or not to print
    String entry = registryArgs.getExport;
    String format = ConfigFormat.JSON.toString();
    ConfigFormat configFormat = ConfigFormat.resolve(format);
    if (configFormat == null || configFormat != ConfigFormat.JSON) {
      throw new BadCommandArgumentsException(
          "Unknown/Unsupported format %s . Only JSON is supported.", format);
    }

    PublishedExportsOutputter outputter =
        PublishedExportsOutputter.createOutputter(configFormat,
                                                  published);
    boolean print = registryArgs.out == null;
    if (!print) {
      File destFile;
      destFile = registryArgs.out;
      if (destFile.isDirectory()) {
        // creating it under a directory
        destFile = new File(destFile, entry + "." + format);
      }
      log.info("Destination path: {}", destFile);
      outputter.save(destFile);
    } else {
      print(outputter.asString());
    }
  }

  /**
   * Look up an instance
   * @return instance data
   * @throws SliderException other failures
   * @throws IOException IO problems or wrapped exceptions
   */
  private ServiceRecord lookupServiceRecord(ActionRegistryArgs registryArgs) throws
      SliderException,
      IOException {
    return ClientUtils.lookupServiceRecord(getRegistryOperations(),
        registryArgs.user, registryArgs.serviceType, registryArgs.name);
  }

  /**
   * 
   * Look up an instance
   * @param path path
   * @return instance data
   * @throws NotFoundException no path/no service record
   * at the end of the path
   * @throws SliderException other failures
   * @throws IOException IO problems or wrapped exceptions
   */
  public ServiceRecord resolve(String path)
      throws IOException, SliderException {
    return ClientUtils.resolve(getRegistryOperations(), path);
  }

  /**
   * List instances in the registry for the current user
   * @return a list of slider registry instances
   * @throws IOException Any IO problem ... including no path in the registry
   * to slider service classes for this user
   * @throws SliderException other failures
   */

  public Map<String, ServiceRecord> listRegistryInstances()
      throws IOException, SliderException {
    Map<String, ServiceRecord> recordMap = listServiceRecords(
        getRegistryOperations(),
        serviceclassPath(currentUser(), SliderKeys.APP_TYPE));
    return recordMap;
  }
  
  /**
   * List instances in the registry
   * @return the instance IDs
   * @throws IOException
   * @throws YarnException
   */
  public List<String> listRegisteredSliderInstances() throws
      IOException,
      YarnException {
    try {
      Map<String, ServiceRecord> recordMap = listServiceRecords(
          getRegistryOperations(),
          serviceclassPath(currentUser(), SliderKeys.APP_TYPE));
      return new ArrayList<>(recordMap.keySet());
    } catch (PathNotFoundException e) {
      log.debug("No registry path for slider instances for current user: {}", e, e);
      // no entries: return an empty list
      return new ArrayList<>(0);
    } catch (IOException | YarnException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Start the registry if it is not there yet
   * @return the registry service
   * @throws SliderException
   * @throws IOException
   */
  private synchronized RegistryOperations maybeStartYarnRegistry()
      throws SliderException, IOException {

    if (registryOperations == null) {
      registryOperations = startRegistryOperationsService();
    }
    return registryOperations;
  }

  @Override
  public RegistryOperations getRegistryOperations()
      throws SliderException, IOException {
    return maybeStartYarnRegistry();
  }

  /**
   * Output to standard out/stderr (implementation specific detail)
   * @param src source
   */
  private static void print(CharSequence src) {
    clientOutputStream.print(src);
  }

  /**
   * Output to standard out/stderr with a newline after
   * @param message message
   */
  private static void println(String message) {
    clientOutputStream.println(message);
  }
  /**
   * Output to standard out/stderr with a newline after, formatted
   * @param message message
   * @param args arguments for string formatting
   */
  private static void println(String message, Object ... args) {
    clientOutputStream.println(String.format(message, args));
  }

  /**
   * Implement the lookup action.
   * @param args Action arguments
   * @return 0 if the entry was found
   * @throws IOException
   * @throws YarnException
   * @throws UnknownApplicationInstanceException if a specific instance
   * was named but it was not found
   */
  @VisibleForTesting
  public int actionLookup(ActionLookupArgs args)
      throws IOException, YarnException {
    verifyBindingsDefined();
    try {
      ApplicationId id = ConverterUtils.toApplicationId(args.id);
      ApplicationReport report = yarnClient.getApplicationReport(id);
      SerializedApplicationReport sar = new SerializedApplicationReport(report);
      ApplicationReportSerDeser serDeser = new ApplicationReportSerDeser();
      if (args.outputFile != null) {
        serDeser.save(sar, args.outputFile);
      } else {
        println(serDeser.toJson(sar));
      }
    } catch (IllegalArgumentException e) {
      throw new BadCommandArgumentsException(e, "%s : %s", args, e);
    } catch (ApplicationAttemptNotFoundException | ApplicationNotFoundException notFound) {
      throw new NotFoundException(notFound, notFound.toString());
    }
    return EXIT_SUCCESS;
  }

  @Override
  public int actionDependency(ActionDependencyArgs args) throws IOException,
      YarnException {
    String currentUser = getUsername();
    log.info("Running command as user {}", currentUser);
    
    String version = getSliderVersion();
    Path dependencyLibTarGzip = sliderFileSystem.getDependencyTarGzip();
    
    // Check if dependency has already been uploaded, in which case log
    // appropriately and exit success (unless overwrite has been requested)
    if (sliderFileSystem.isFile(dependencyLibTarGzip) && !args.overwrite) {
      println(String.format(
          "Dependency libs are already uploaded to %s. Use %s "
              + "if you want to re-upload", dependencyLibTarGzip.toUri(),
          Arguments.ARG_OVERWRITE));
      return EXIT_SUCCESS;
    }
    
    String[] libDirs = SliderUtils.getLibDirs();
    if (libDirs.length > 0) {
      File tempLibTarGzipFile = File.createTempFile(
          SliderKeys.SLIDER_DEPENDENCY_TAR_GZ_FILE_NAME + "_",
          SliderKeys.SLIDER_DEPENDENCY_TAR_GZ_FILE_EXT);
      // copy all jars
      tarGzipFolder(libDirs, tempLibTarGzipFile, createJarFilter());

      log.info("Uploading dependency for AM (version {}) from {} to {}",
          version, tempLibTarGzipFile.toURI(), dependencyLibTarGzip.toUri());
      sliderFileSystem.copyLocalFileToHdfs(tempLibTarGzipFile,
          dependencyLibTarGzip, new FsPermission(
              SliderKeys.SLIDER_DEPENDENCY_DIR_PERMISSIONS));
      return EXIT_SUCCESS;
    } else {
      return EXIT_FALSE;
    }
  }

  private int actionHelp(String actionName) throws YarnException, IOException {
    throw new UsageException(CommonArgs.usage(serviceArgs, actionName));
  }

  /**
   * List the nodes in the cluster, possibly filtering by node state or label.
   *
   * @param args argument list
   * @return a possibly empty list of nodes in the cluster
   * @throws IOException IO problems
   * @throws YarnException YARN problems
   */
  @Override
  public NodeInformationList listYarnClusterNodes(ActionNodesArgs args)
    throws YarnException, IOException {
    return yarnClient.listNodes(args.label, args.healthy);
  }

  /**
   * List the nodes in the cluster, possibly filtering by node state or label.
   *
   * @param args argument list
   * @return a possibly empty list of nodes in the cluster
   * @throws IOException IO problems
   * @throws YarnException YARN problems
   */
  public NodeInformationList listInstanceNodes(String instance, ActionNodesArgs args)
    throws YarnException, IOException {
    // TODO
    log.info("listInstanceNodes {}", instance);
    SliderClusterOperations clusterOps =
      new SliderClusterOperations(bondToCluster(instance));
    return clusterOps.getLiveNodes();
  }

  /**
   * List the nodes in the cluster, possibly filtering by node state or label.
   * Prints them to stdout unless the args names a file instead.
   * @param args argument list
   * @throws IOException IO problems
   * @throws YarnException YARN problems
   */
  public int actionNodes(String instance, ActionNodesArgs args) throws YarnException, IOException {

    args.instance = instance;
    NodeInformationList nodes;
    if (SliderUtils.isUnset(instance)) {
      nodes = listYarnClusterNodes(args);
    } else {
      nodes = listInstanceNodes(instance, args);
    }
    log.debug("Node listing for {} has {} nodes", args, nodes.size());
    JsonSerDeser<NodeInformationList> serDeser = NodeInformationList.createSerializer();
    if (args.outputFile != null) {
      serDeser.save(nodes, args.outputFile);
    } else {
      println(serDeser.toJson(nodes));
    }
    return 0;
  }

  /**
   * Create a new IPC client for talking to slider via what follows the REST API.
   * Client must already be bonded to the cluster
   * @return a new IPC client
   */
  public SliderApplicationApi createIpcClient()
    throws IOException, YarnException {
    return new SliderApplicationIpcClient(createClusterOperations());
  }

  /**
   * Save/list tokens. This is for testing oozie integration
   * @param args commands
   * @return status
   */
  private int actionTokens(ActionTokensArgs args)
      throws IOException, YarnException {
    return new TokensOperation().actionTokens(args,
        sliderFileSystem.getFileSystem(),
        getConfig(),
        yarnClient);
  }

}


