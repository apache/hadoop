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

package org.apache.hadoop.registry.server.services;


import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.registry.client.exceptions.NoPathPermissionsException;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.registry.client.impl.zk.RegistryBindingSource;
import org.apache.hadoop.registry.client.impl.zk.RegistryOperationsService;
import org.apache.hadoop.registry.client.impl.zk.RegistrySecurity;
import org.apache.hadoop.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Administrator service for the registry. This is the one with
 * permissions to create the base directories and those for users.
 *
 * It also includes support for asynchronous operations, so that
 * zookeeper connectivity problems do not hold up the server code
 * performing the actions.
 *
 * Any action queued via {@link #submit(Callable)} will be
 * run asynchronously. The {@link #createDirAsync(String, List, boolean)}
 * is an example of such an an action
 *
 * A key async action is the depth-first tree purge, which supports
 * pluggable policies for deleting entries. The method
 * {@link #purge(String, NodeSelector, PurgePolicy, BackgroundCallback)}
 * implements the recursive purge operation —the class
 * {{AsyncPurge}} provides the asynchronous scheduling of this.
 */
public class RegistryAdminService extends RegistryOperationsService {

  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryAdminService.class);
  /**
   * The ACL permissions for the user's homedir ACL.
   */
  public static final int USER_HOMEDIR_ACL_PERMISSIONS =
        ZooDefs.Perms.READ | ZooDefs.Perms.WRITE
      | ZooDefs.Perms.CREATE | ZooDefs.Perms.DELETE;

  /**
   * Executor for async operations
   */
  protected final ExecutorService executor;

  /**
   * Construct an instance of the service
   * @param name service name
   */
  public RegistryAdminService(String name) {
    this(name, null);
  }

  /**
   * construct an instance of the service, using the
   * specified binding source to bond to ZK
   * @param name service name
   * @param bindingSource provider of ZK binding information
   */
  public RegistryAdminService(String name,
      RegistryBindingSource bindingSource) {
    super(name, bindingSource);
    executor = HadoopExecutors.newCachedThreadPool(
        new ThreadFactory() {
          private AtomicInteger counter = new AtomicInteger(1);

          @Override
          public Thread newThread(Runnable r) {
            return new Thread(r,
                "RegistryAdminService " + counter.getAndIncrement());
          }
        });
  }

  /**
   * Stop the service: halt the executor.
   * @throws Exception exception.
   */
  @Override
  protected void serviceStop() throws Exception {
    stopExecutor();
    super.serviceStop();
  }

  /**
   * Stop the executor if it is not null.
   * This uses {@link ExecutorService#shutdownNow()}
   * and so does not block until they have completed.
   */
  protected synchronized void stopExecutor() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  /**
   * Get the executor
   * @return the executor
   */
  protected ExecutorService getExecutor() {
    return executor;
  }

  /**
   * Submit a callable
   * @param callable callable
   * @param <V> type of the final get
   * @return a future to wait on
   */
  public <V> Future<V> submit(Callable<V> callable) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Submitting {}", callable);
    }
    return getExecutor().submit(callable);
  }

  /**
   * Asynchronous operation to create a directory
   * @param path path
   * @param acls ACL list
   * @param createParents flag to indicate parent dirs should be created
   * as needed
   * @return the future which will indicate whether or not the operation
   * succeeded —and propagate any exceptions
   * @throws IOException
   */
  public Future<Boolean> createDirAsync(final String path,
      final List<ACL> acls,
      final boolean createParents) throws IOException {
    return submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return maybeCreate(path, CreateMode.PERSISTENT,
            acls, createParents);
      }
    });
  }

  /**
   * Init operation sets up the system ACLs.
   * @param conf configuration of the service
   * @throws Exception
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    RegistrySecurity registrySecurity = getRegistrySecurity();
    if (registrySecurity.isSecureRegistry()) {
      ACL sasl = registrySecurity.createSaslACLFromCurrentUser(ZooDefs.Perms.ALL);
      registrySecurity.addSystemACL(sasl);
      LOG.info("Registry System ACLs:",
          RegistrySecurity.aclsToString(
          registrySecurity.getSystemACLs()));
    }
  }

  /**
   * Start the service, including creating base directories with permissions
   * @throws Exception
   */
  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    // create the root directories
    try {
      createRootRegistryPaths();
    } catch (NoPathPermissionsException e) {

      String message = String.format(Locale.ENGLISH,
          "Failed to create root paths {%s};" +
          "%ndiagnostics={%s}" +
          "%ncurrent registry is:" +
          "%n{%s}",
          e,
          bindingDiagnosticDetails(),
          dumpRegistryRobustly(true));

      LOG.error(" Failure {}", e, e);
      LOG.error(message);

      // TODO: this is something temporary to deal with the problem
      // that jenkins is failing this test
      throw new NoPathPermissionsException(e.getPath().toString(), message, e);
    }
  }

  /**
   * Create the initial registry paths
   * @throws IOException any failure
   */
  @VisibleForTesting
  public void createRootRegistryPaths() throws IOException {

    List<ACL> systemACLs = getRegistrySecurity().getSystemACLs();
    LOG.info("System ACLs {}",
        RegistrySecurity.aclsToString(systemACLs));
    maybeCreate("", CreateMode.PERSISTENT, systemACLs, false);
    maybeCreate(PATH_USERS, CreateMode.PERSISTENT,
        systemACLs, false);
    maybeCreate(PATH_SYSTEM_SERVICES,
        CreateMode.PERSISTENT,
        systemACLs, false);
  }

  /**
   * Get the path to a user's home dir
   * @param username username
   * @return a path for services underneath
   */
  protected String homeDir(String username) {
    return RegistryUtils.homePathForUser(username);
  }

  /**
   * Set up the ACL for the user.
   * <b>Important: this must run client-side as it needs
   * to know the id:pass tuple for a user</b>
   * @param username user name
   * @param perms permissions
   * @return an ACL list
   * @throws IOException ACL creation/parsing problems
   */
  public List<ACL> aclsForUser(String username, int perms) throws IOException {
    List<ACL> clientACLs = getClientAcls();
    RegistrySecurity security = getRegistrySecurity();
    if (security.isSecureRegistry()) {
      clientACLs.add(security.createACLfromUsername(username, perms));
    }
    return clientACLs;
  }

  /**
   * Start an async operation to create the home path for a user
   * if it does not exist
   * @param shortname username, without any @REALM in kerberos
   * @return the path created
   * @throws IOException any failure while setting up the operation
   *
   */
  public Future<Boolean> initUserRegistryAsync(final String shortname)
      throws IOException {

    String homeDir = homeDir(shortname);
    if (!exists(homeDir)) {
      // create the directory. The user does not
      return createDirAsync(homeDir,
          aclsForUser(shortname,
              USER_HOMEDIR_ACL_PERMISSIONS),
          false);
    }
    return null;
  }

  /**
   * Create the home path for a user if it does not exist.
   *
   * This uses {@link #initUserRegistryAsync(String)} and then waits for the
   * result ... the code path is the same as the async operation; this just
   * picks up and relays/converts exceptions
   * @param username username
   * @return the path created
   * @throws IOException any failure
   *
   */
  public String initUserRegistry(final String username)
      throws IOException {

    try {
      Future<Boolean> future = initUserRegistryAsync(username);
      future.get();
    } catch (InterruptedException e) {
      throw (InterruptedIOException)
          (new InterruptedIOException(e.toString()).initCause(e));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) (cause);
      } else {
        throw new IOException(cause.toString(), cause);
      }
    }

    return homeDir(username);
  }

  /**
   * Method to validate the validity of the kerberos realm.
   * <ul>
   *   <li>Insecure: not needed.</li>
   *   <li>Secure: must have been determined.</li>
   * </ul>
   */
  protected void verifyRealmValidity() throws ServiceStateException {
    if (isSecure()) {
      String realm = getRegistrySecurity().getKerberosRealm();
      if (StringUtils.isEmpty(realm)) {
        throw new ServiceStateException("Cannot determine service realm");
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Started Registry operations in realm {}", realm);
      }
    }
  }

  /**
   * Policy to purge entries
   */
  public enum PurgePolicy {
    PurgeAll,
    FailOnChildren,
    SkipOnChildren
  }

  /**
   * Recursive operation to purge all matching records under a base path.
   * <ol>
   *   <li>Uses a depth first search</li>
   *   <li>A match is on ID and persistence policy, or, if policy==-1, any match</li>
   *   <li>If a record matches then it is deleted without any child searches</li>
   *   <li>Deletions will be asynchronous if a callback is provided</li>
   * </ol>
   *
   * The code is designed to be robust against parallel deletions taking place;
   * in such a case it will stop attempting that part of the tree. This
   * avoid the situation of more than 1 purge happening in parallel and
   * one of the purge operations deleteing the node tree above the other.
   * @param path base path
   * @param selector selector for the purge policy
   * @param purgePolicy what to do if there is a matching record with children
   * @param callback optional curator callback
   * @return the number of delete operations perfomed. As deletes may be for
   * everything under a path, this may be less than the number of records
   * actually deleted
   * @throws IOException problems
   * @throws PathIsNotEmptyDirectoryException if an entry cannot be deleted
   * as it has children and the purge policy is FailOnChildren
   */
  @VisibleForTesting
  public int purge(String path,
      NodeSelector selector,
      PurgePolicy purgePolicy,
      BackgroundCallback callback) throws IOException {


    boolean toDelete = false;
    // look at self to see if it has a service record
    Map<String, RegistryPathStatus> childEntries;
    Collection<RegistryPathStatus> entries;
    try {
      // list this path's children
      childEntries = RegistryUtils.statChildren(this, path);
      entries = childEntries.values();
    } catch (PathNotFoundException e) {
      // there's no record here, it may have been deleted already.
      // exit
      return 0;
    }

    try {
      RegistryPathStatus registryPathStatus = stat(path);
      ServiceRecord serviceRecord = resolve(path);
      // there is now an entry here.
      toDelete = selector.shouldSelect(path, registryPathStatus, serviceRecord);
    } catch (EOFException ignored) {
      // ignore
    } catch (InvalidRecordException ignored) {
      // ignore
    } catch (NoRecordException ignored) {
      // ignore
    } catch (PathNotFoundException e) {
      // there's no record here, it may have been deleted already.
      // exit
      return 0;
    }

    if (toDelete && !entries.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Match on record @ {} with children ", path);
      }
      // there's children
      switch (purgePolicy) {
        case SkipOnChildren:
          // don't do the deletion... continue to next record
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping deletion");
          }
          toDelete = false;
          break;
        case PurgeAll:
          // mark for deletion
          if (LOG.isDebugEnabled()) {
            LOG.debug("Scheduling for deletion with children");
          }
          toDelete = true;
          entries = new ArrayList<RegistryPathStatus>(0);
          break;
        case FailOnChildren:
          if (LOG.isDebugEnabled()) {
            LOG.debug("Failing deletion operation");
          }
          throw new PathIsNotEmptyDirectoryException(path);
      }
    }

    int deleteOps = 0;
    if (toDelete) {
      try {
        zkDelete(path, true, callback);
      } catch (PathNotFoundException e) {
        // sign that the path was deleted during the operation.
        // this is a no-op, and all children can be skipped
        return deleteOps;
      }
      deleteOps++;
    }

    // now go through the children
    for (RegistryPathStatus status : entries) {
      String childname = status.path;
      String childpath = RegistryPathUtils.join(path, childname);
      deleteOps += purge(childpath,
          selector,
          purgePolicy,
          callback);
    }

    return deleteOps;
  }

  /**
   * Comparator used for purge logic
   */
  public interface NodeSelector {

    boolean shouldSelect(String path,
        RegistryPathStatus registryPathStatus,
        ServiceRecord serviceRecord);
  }

  /**
   * An async registry purge action taking
   * a selector which decides what to delete
   */
  public class AsyncPurge implements Callable<Integer> {

    private final BackgroundCallback callback;
    private final NodeSelector selector;
    private final String path;
    private final PurgePolicy purgePolicy;

    public AsyncPurge(String path,
        NodeSelector selector,
        PurgePolicy purgePolicy,
        BackgroundCallback callback) {
      this.callback = callback;
      this.selector = selector;
      this.path = path;
      this.purgePolicy = purgePolicy;
    }

    @Override
    public Integer call() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Executing {}", this);
      }
      return purge(path,
          selector,
          purgePolicy,
          callback);
    }

    @Override
    public String toString() {
      return String.format(
          "Record purge under %s with selector %s",
          path, selector);
    }
  }

}
