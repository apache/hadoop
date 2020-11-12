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

package org.apache.hadoop.registry.client.impl.zk;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.exceptions.AuthenticationFailedException;
import org.apache.hadoop.registry.client.exceptions.NoChildrenForEphemeralsException;
import org.apache.hadoop.registry.client.exceptions.NoPathPermissionsException;
import org.apache.hadoop.registry.client.exceptions.RegistryIOException;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * This service binds to Zookeeper via Apache Curator. It is more
 * generic than just the YARN service registry; it does not implement
 * any of the Registry Operations API.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CuratorService extends CompositeService
    implements RegistryConstants, RegistryBindingSource {

  private static final Logger LOG =
      LoggerFactory.getLogger(CuratorService.class);

  /**
   * the Curator binding.
   */
  private CuratorFramework curator;

  /**
   * Path to the registry root.
   */
  private String registryRoot;

  /**
   * Supplied binding source. This defaults to being this
   * service itself.
   */
  private final RegistryBindingSource bindingSource;

  /**
   * Security service.
   */
  private RegistrySecurity registrySecurity;

  /**
   * the connection binding text for messages.
   */
  private String connectionDescription;

  /**
   * Security connection diagnostics.
   */
  private String securityConnectionDiagnostics = "";

  /**
   * Provider of curator "ensemble"; offers a basis for
   * more flexible bonding in future.
   */
  private EnsembleProvider ensembleProvider;

  /**
   * Registry tree cache.
   */
  private TreeCache treeCache;

  /**
   * Construct the service.
   *
   * @param name          service name
   * @param bindingSource source of binding information.
   *                      If null: use this instance
   */
  public CuratorService(String name, RegistryBindingSource bindingSource) {
    super(name);
    if (bindingSource != null) {
      this.bindingSource = bindingSource;
    } else {
      this.bindingSource = this;
    }
    registrySecurity = new RegistrySecurity("registry security");
  }

  /**
   * Create an instance using this service as the binding source (i.e. read
   * configuration options from the registry).
   *
   * @param name service name
   */
  public CuratorService(String name) {
    this(name, null);
  }

  /**
   * Init the service.
   * This is where the security bindings are set up.
   *
   * @param conf configuration of the service
   * @throws Exception
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    registryRoot = conf.getTrimmed(KEY_REGISTRY_ZK_ROOT,
        DEFAULT_ZK_REGISTRY_ROOT);

    // add the registy service
    addService(registrySecurity);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating Registry with root {}", registryRoot);
    }

    super.serviceInit(conf);
  }

  public void setKerberosPrincipalAndKeytab(String principal, String keytab) {
    registrySecurity.setKerberosPrincipalAndKeytab(principal, keytab);
  }

  /**
   * Start the service.
   * This is where the curator instance is started.
   *
   * @throws Exception
   */
  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();

    // create the curator; rely on the registry security code
    // to set up the JVM context and curator
    curator = createCurator();
  }

  /**
   * Close the ZK connection if it is open.
   */
  @Override
  protected void serviceStop() throws Exception {
    IOUtils.closeStream(curator);

    if (treeCache != null) {
      treeCache.close();
    }
    super.serviceStop();
  }

  /**
   * Internal check that a service is in the live state.
   *
   * @throws ServiceStateException if not
   */
  private void checkServiceLive() throws ServiceStateException {
    if (!isInState(STATE.STARTED)) {
      throw new ServiceStateException(
          "Service " + getName() + " is in wrong state: "
              + getServiceState());
    }
  }

  /**
   * Flag to indicate whether or not the registry is secure.
   * Valid once the service is inited.
   *
   * @return service security policy
   */
  public boolean isSecure() {
    return registrySecurity.isSecureRegistry();
  }

  /**
   * Get the registry security helper.
   *
   * @return the registry security helper
   */
  protected RegistrySecurity getRegistrySecurity() {
    return registrySecurity;
  }

  /**
   * Build the security diagnostics string.
   *
   * @return a string for diagnostics
   */
  protected String buildSecurityDiagnostics() {
    // build up the security connection diags
    if (!isSecure()) {
      return "security disabled";
    } else {
      StringBuilder builder = new StringBuilder();
      builder.append("secure cluster; ");
      builder.append(registrySecurity.buildSecurityDiagnostics());
      return builder.toString();
    }
  }

  /**
   * Create a new curator instance off the root path; using configuration
   * options provided in the service configuration to set timeouts and
   * retry policy.
   *
   * @return the newly created creator
   */
  private CuratorFramework createCurator() throws IOException {
    Configuration conf = getConfig();
    createEnsembleProvider();
    int sessionTimeout = conf.getInt(KEY_REGISTRY_ZK_SESSION_TIMEOUT,
        DEFAULT_ZK_SESSION_TIMEOUT);
    int connectionTimeout = conf.getInt(KEY_REGISTRY_ZK_CONNECTION_TIMEOUT,
        DEFAULT_ZK_CONNECTION_TIMEOUT);
    int retryTimes = conf.getInt(KEY_REGISTRY_ZK_RETRY_TIMES,
        DEFAULT_ZK_RETRY_TIMES);
    int retryInterval = conf.getInt(KEY_REGISTRY_ZK_RETRY_INTERVAL,
        DEFAULT_ZK_RETRY_INTERVAL);
    int retryCeiling = conf.getInt(KEY_REGISTRY_ZK_RETRY_CEILING,
        DEFAULT_ZK_RETRY_CEILING);

    LOG.info("Creating CuratorService with connection {}",
          connectionDescription);

    CuratorFramework framework;

    synchronized (CuratorService.class) {
      // set the security options

      // build up the curator itself
      CuratorFrameworkFactory.Builder builder =
          CuratorFrameworkFactory.builder();
      builder.ensembleProvider(ensembleProvider)
          .connectionTimeoutMs(connectionTimeout)
          .sessionTimeoutMs(sessionTimeout)

          .retryPolicy(new BoundedExponentialBackoffRetry(retryInterval,
              retryCeiling,
              retryTimes));

      // set up the builder AND any JVM context
      registrySecurity.applySecurityEnvironment(builder);
      //log them
      securityConnectionDiagnostics = buildSecurityDiagnostics();
      if (LOG.isDebugEnabled()) {
        LOG.debug(securityConnectionDiagnostics);
      }
      framework = builder.build();
      framework.start();
    }

    return framework;
  }

  @Override
  public String toString() {
    return super.toString()
        + " " + bindingDiagnosticDetails();
  }

  /**
   * Get the binding diagnostics.
   *
   * @return a diagnostics string valid after the service is started.
   */
  public String bindingDiagnosticDetails() {
    return " Connection=\"" + connectionDescription + "\""
        + " root=\"" + registryRoot + "\""
        + " " + securityConnectionDiagnostics;
  }

  /**
   * Create a full path from the registry root and the supplied subdir.
   *
   * @param path path of operation
   * @return an absolute path
   * @throws IllegalArgumentException if the path is invalide
   */
  protected String createFullPath(String path) throws IOException {
    return RegistryPathUtils.createFullPath(registryRoot, path);
  }

  /**
   * Get the registry binding source ... this can be used to
   * create new ensemble providers
   *
   * @return the registry binding source in use
   */
  public RegistryBindingSource getBindingSource() {
    return bindingSource;
  }

  /**
   * Create the ensemble provider for this registry, by invoking
   * {@link RegistryBindingSource#supplyBindingInformation()} on
   * the provider stored in {@link #bindingSource}.
   * Sets {@link #ensembleProvider} to that value;
   * sets {@link #connectionDescription} to the binding info
   * for use in toString and logging;
   */
  protected void createEnsembleProvider() {
    BindingInformation binding = bindingSource.supplyBindingInformation();
    connectionDescription = binding.description
        + " " + securityConnectionDiagnostics;
    ensembleProvider = binding.ensembleProvider;
  }

  /**
   * Supply the binding information.
   * This implementation returns a fixed ensemble bonded to
   * the quorum supplied by {@link #buildConnectionString()}.
   *
   * @return the binding information
   */
  @Override
  public BindingInformation supplyBindingInformation() {
    BindingInformation binding = new BindingInformation();
    String connectString = buildConnectionString();
    binding.ensembleProvider = new FixedEnsembleProvider(connectString);
    binding.description =
        "fixed ZK quorum \"" + connectString + "\"";
    return binding;
  }

  /**
   * Override point: get the connection string used to connect to
   * the ZK service.
   *
   * @return a registry quorum
   */
  protected String buildConnectionString() {
    return getConfig().getTrimmed(KEY_REGISTRY_ZK_QUORUM,
                                  DEFAULT_REGISTRY_ZK_QUORUM);
  }

  /**
   * Create an IOE when an operation fails.
   *
   * @param path      path of operation
   * @param operation operation attempted
   * @param exception caught the exception caught
   * @return an IOE to throw that contains the path and operation details.
   */
  protected IOException operationFailure(String path,
      String operation,
      Exception exception) {
    return operationFailure(path, operation, exception, null);
  }

  /**
   * Create an IOE when an operation fails.
   *
   * @param path      path of operation
   * @param operation operation attempted
   * @param exception caught the exception caught
   * @return an IOE to throw that contains the path and operation details.
   */
  protected IOException operationFailure(String path,
      String operation,
      Exception exception,
      List<ACL> acls) {
    IOException ioe;
    String aclList = "[" + RegistrySecurity.aclsToString(acls) + "]";
    if (exception instanceof KeeperException.NoNodeException) {
      ioe = new PathNotFoundException(path);
    } else if (exception instanceof KeeperException.NodeExistsException) {
      ioe = new FileAlreadyExistsException(path);
    } else if (exception instanceof KeeperException.NoAuthException) {
      ioe = new NoPathPermissionsException(path,
          "Not authorized to access path; ACLs: " + aclList);
    } else if (exception instanceof KeeperException.NotEmptyException) {
      ioe = new PathIsNotEmptyDirectoryException(path);
    } else if (exception instanceof KeeperException.AuthFailedException) {
      ioe = new AuthenticationFailedException(path,
          "Authentication Failed: " + exception
              + "; " + securityConnectionDiagnostics,
          exception);
    } else if (exception instanceof
        KeeperException.NoChildrenForEphemeralsException) {
      ioe = new NoChildrenForEphemeralsException(path,
          "Cannot create a path under an ephemeral node: " + exception,
          exception);
    } else if (exception instanceof KeeperException.InvalidACLException) {
      // this is a security exception of a kind
      // include the ACLs to help the diagnostics
      StringBuilder builder = new StringBuilder();
      builder.append("Path access failure ").append(aclList);
      builder.append(" ");
      builder.append(securityConnectionDiagnostics);
      ioe = new NoPathPermissionsException(path, builder.toString());
    } else {
      ioe = new RegistryIOException(path,
          "Failure of " + operation + " on " + path + ": " +
              exception.toString(),
          exception);
    }
    if (ioe.getCause() == null) {
      ioe.initCause(exception);
    }
    return ioe;
  }

  /**
   * Create a path if it does not exist.
   * The check is poll + create; there's a risk that another process
   * may create the same path before the create() operation is executed/
   * propagated to the ZK node polled.
   *
   * @param path          path to create
   * @param acl           ACL for path -used when creating a new entry
   * @param createParents flag to trigger parent creation
   * @return true iff the path was created
   * @throws IOException
   */
  @VisibleForTesting
  public boolean maybeCreate(String path,
      CreateMode mode,
      List<ACL> acl,
      boolean createParents) throws IOException {
    return zkMkPath(path, mode, createParents, acl);
  }

  /**
   * Stat the file.
   *
   * @param path path of operation
   * @return a curator stat entry
   * @throws IOException           on a failure
   * @throws PathNotFoundException if the path was not found
   */
  public Stat zkStat(String path) throws IOException {
    checkServiceLive();
    String fullpath = createFullPath(path);
    Stat stat;
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stat {}", fullpath);
      }
      stat = curator.checkExists().forPath(fullpath);
    } catch (Exception e) {
      throw operationFailure(fullpath, "read()", e);
    }
    if (stat == null) {
      throw new PathNotFoundException(path);
    }
    return stat;
  }

  /**
   * Get the ACLs of a path.
   *
   * @param path path of operation
   * @return a possibly empty list of ACLs
   * @throws IOException
   */
  public List<ACL> zkGetACLS(String path) throws IOException {
    checkServiceLive();
    String fullpath = createFullPath(path);
    List<ACL> acls;
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("GetACLS {}", fullpath);
      }
      acls = curator.getACL().forPath(fullpath);
    } catch (Exception e) {
      throw operationFailure(fullpath, "read()", e);
    }
    if (acls == null) {
      throw new PathNotFoundException(path);
    }
    return acls;
  }

  /**
   * Probe for a path existing.
   *
   * @param path path of operation
   * @return true if the path was visible from the ZK server
   * queried.
   * @throws IOException on any exception other than
   *                     {@link PathNotFoundException}
   */
  public boolean zkPathExists(String path) throws IOException {
    checkServiceLive();
    try {
      // if zkStat(path) returns without throwing an exception, the return value
      // is guaranteed to be not null
      zkStat(path);
      return true;
    } catch (PathNotFoundException e) {
      return false;
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Verify a path exists.
   *
   * @param path path of operation
   * @throws PathNotFoundException if the path is absent
   * @throws IOException
   */
  public String zkPathMustExist(String path) throws IOException {
    zkStat(path);
    return path;
  }

  /**
   * Create a directory. It is not an error if it already exists.
   *
   * @param path          path to create
   * @param mode          mode for path
   * @param createParents flag to trigger parent creation
   * @param acls          ACL for path
   * @throws IOException any problem
   */
  public boolean zkMkPath(String path,
      CreateMode mode,
      boolean createParents,
      List<ACL> acls)
      throws IOException {
    checkServiceLive();
    path = createFullPath(path);
    if (acls == null || acls.isEmpty()) {
      throw new NoPathPermissionsException(path, "Empty ACL list");
    }

    try {
      RegistrySecurity.AclListInfo aclInfo =
          new RegistrySecurity.AclListInfo(acls);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating path {} with mode {} and ACL {}",
            path, mode, aclInfo);
      }
      CreateBuilder createBuilder = curator.create();
      createBuilder.withMode(mode).withACL(acls);
      if (createParents) {
        createBuilder.creatingParentsIfNeeded();
      }
      createBuilder.forPath(path);

    } catch (KeeperException.NodeExistsException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("path already present: {}", path, e);
      }
      return false;
    } catch (Exception e) {
      throw operationFailure(path, "mkdir() ", e, acls);
    }
    return true;
  }

  /**
   * Recursively make a path.
   *
   * @param path path to create
   * @param acl  ACL for path
   * @throws IOException any problem
   */
  public void zkMkParentPath(String path,
      List<ACL> acl) throws
      IOException {
    // split path into elements

    zkMkPath(RegistryPathUtils.parentOf(path),
        CreateMode.PERSISTENT, true, acl);
  }

  /**
   * Create a path with given data. byte[0] is used for a path
   * without data.
   *
   * @param path path of operation
   * @param data initial data
   * @param acls
   * @throws IOException
   */
  public void zkCreate(String path,
      CreateMode mode,
      byte[] data,
      List<ACL> acls) throws IOException {
    Preconditions.checkArgument(data != null, "null data");
    checkServiceLive();
    String fullpath = createFullPath(path);
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating {} with {} bytes of data and ACL {}",
            fullpath, data.length,
            new RegistrySecurity.AclListInfo(acls));
      }
      curator.create().withMode(mode).withACL(acls).forPath(fullpath, data);
    } catch (Exception e) {
      throw operationFailure(fullpath, "create()", e, acls);
    }
  }

  /**
   * Update the data for a path.
   *
   * @param path path of operation
   * @param data new data
   * @throws IOException
   */
  public void zkUpdate(String path, byte[] data) throws IOException {
    Preconditions.checkArgument(data != null, "null data");
    checkServiceLive();
    path = createFullPath(path);
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Updating {} with {} bytes", path, data.length);
      }
      curator.setData().forPath(path, data);
    } catch (Exception e) {
      throw operationFailure(path, "update()", e);
    }
  }

  /**
   * Create or update an entry.
   *
   * @param path      path
   * @param data      data
   * @param acl       ACL for path -used when creating a new entry
   * @param overwrite enable overwrite
   * @return true if the entry was created, false if it was simply updated.
   * @throws IOException
   */
  public boolean zkSet(String path,
      CreateMode mode,
      byte[] data,
      List<ACL> acl, boolean overwrite) throws IOException {
    Preconditions.checkArgument(data != null, "null data");
    checkServiceLive();
    if (!zkPathExists(path)) {
      zkCreate(path, mode, data, acl);
      return true;
    } else {
      if (overwrite) {
        zkUpdate(path, data);
        return false;
      } else {
        throw new FileAlreadyExistsException(path);
      }
    }
  }

  /**
   * Delete a directory/directory tree.
   * It is not an error to delete a path that does not exist.
   *
   * @param path               path of operation
   * @param recursive          flag to trigger recursive deletion
   * @param backgroundCallback callback; this being set converts the operation
   *                           into an async/background operation.
   *                           task
   * @throws IOException on problems other than no-such-path
   */
  public void zkDelete(String path,
      boolean recursive,
      BackgroundCallback backgroundCallback) throws IOException {
    checkServiceLive();
    String fullpath = createFullPath(path);
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting {}", fullpath);
      }
      DeleteBuilder delete = curator.delete();
      if (recursive) {
        delete.deletingChildrenIfNeeded();
      }
      if (backgroundCallback != null) {
        delete.inBackground(backgroundCallback);
      }
      delete.forPath(fullpath);
    } catch (KeeperException.NoNodeException e) {
      // not an error
    } catch (Exception e) {
      throw operationFailure(fullpath, "delete()", e);
    }
  }

  /**
   * List all children of a path.
   *
   * @param path path of operation
   * @return a possibly empty list of children
   * @throws IOException
   */
  public List<String> zkList(String path) throws IOException {
    checkServiceLive();
    String fullpath = createFullPath(path);
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("ls {}", fullpath);
      }
      GetChildrenBuilder builder = curator.getChildren();
      List<String> children = builder.forPath(fullpath);
      return children;
    } catch (Exception e) {
      throw operationFailure(path, "ls()", e);
    }
  }

  /**
   * Read data on a path.
   *
   * @param path path of operation
   * @return the data
   * @throws IOException read failure
   */
  public byte[] zkRead(String path) throws IOException {
    checkServiceLive();
    String fullpath = createFullPath(path);
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Reading {}", fullpath);
      }
      return curator.getData().forPath(fullpath);
    } catch (Exception e) {
      throw operationFailure(fullpath, "read()", e);
    }
  }

  /**
   * Return a path dumper instance which can do a full dump
   * of the registry tree in its <code>toString()</code>
   * operation.
   *
   * @param verbose verbose flag - includes more details (such as ACLs)
   * @return a class to dump the registry
   */
  public ZKPathDumper dumpPath(boolean verbose) {
    return new ZKPathDumper(curator, registryRoot, verbose);
  }

  /**
   * Add a new write access entry for all future write operations.
   *
   * @param id   ID to use
   * @param pass password
   * @throws IOException on any failure to build the digest
   */
  public boolean addWriteAccessor(String id, String pass) throws IOException {
    RegistrySecurity security = getRegistrySecurity();
    ACL digestACL = new ACL(ZooDefs.Perms.ALL,
        security.toDigestId(security.digest(id, pass)));
    return security.addDigestACL(digestACL);
  }

  /**
   * Clear all write accessors.
   */
  public void clearWriteAccessors() {
    getRegistrySecurity().resetDigestACLs();
  }

  /**
   * Diagnostics method to dump a registry robustly.
   * Any exception raised is swallowed.
   *
   * @param verbose verbose path dump
   * @return the registry tree
   */
  protected String dumpRegistryRobustly(boolean verbose) {
    try {
      ZKPathDumper pathDumper = dumpPath(verbose);
      return pathDumper.toString();
    } catch (Exception e) {
      // ignore
      LOG.debug("Ignoring exception:  {}", e);
    }
    return "";
  }

  /**
   * Registers a listener to path related events.
   *
   * @param listener the listener.
   * @return a handle allowing for the management of the listener.
   * @throws Exception if registration fails due to error.
   */
  public ListenerHandle registerPathListener(final PathListener listener)
      throws Exception {

    final TreeCacheListener pathChildrenCacheListener =
        new TreeCacheListener() {

          public void childEvent(CuratorFramework curatorFramework,
              TreeCacheEvent event)
              throws Exception {
            String path = null;
            if (event != null && event.getData() != null) {
              path = event.getData().getPath();
            }
            assert event != null;
            switch (event.getType()) {
            case NODE_ADDED:
              LOG.info("Informing listener of added node {}", path);
              listener.nodeAdded(path);

              break;

            case NODE_REMOVED:
              LOG.info("Informing listener of removed node {}", path);
              listener.nodeRemoved(path);

              break;

            case NODE_UPDATED:
              LOG.info("Informing listener of updated node {}", path);
              listener.nodeAdded(path);

              break;

            default:
              // do nothing
              break;

            }
          }
        };
    treeCache.getListenable().addListener(pathChildrenCacheListener);

    return new ListenerHandle() {
      @Override
      public void remove() {
        treeCache.getListenable().removeListener(pathChildrenCacheListener);
      }
    };

  }

  // TODO: should caches be stopped and then restarted if need be?

  /**
   * Create the tree cache that monitors the registry for node addition, update,
   * and deletion.
   *
   * @throws Exception if any issue arises during monitoring.
   */
  public void monitorRegistryEntries()
      throws Exception {
    String registryPath =
        getConfig().get(RegistryConstants.KEY_REGISTRY_ZK_ROOT,
            RegistryConstants.DEFAULT_ZK_REGISTRY_ROOT);
    treeCache = new TreeCache(curator, registryPath);
    treeCache.start();
  }
}
