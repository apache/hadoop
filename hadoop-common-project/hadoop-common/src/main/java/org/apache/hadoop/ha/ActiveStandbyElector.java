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

package org.apache.hadoop.ha;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.ZKUtil.ZKAuthInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.AsyncCallback.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException.Code;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * This class implements a simple library to perform leader election on top of
 * Apache Zookeeper. Using Zookeeper as a coordination service, leader election
 * can be performed by atomically creating an ephemeral lock file (znode) on
 * Zookeeper. The service instance that successfully creates the znode becomes
 * active and the rest become standbys. <br/>
 * This election mechanism is only efficient for small number of election
 * candidates (order of 10's) because contention on single znode by a large
 * number of candidates can result in Zookeeper overload. <br/>
 * The elector does not guarantee fencing (protection of shared resources) among
 * service instances. After it has notified an instance about becoming a leader,
 * then that instance must ensure that it meets the service consistency
 * requirements. If it cannot do so, then it is recommended to quit the
 * election. The application implements the {@link ActiveStandbyElectorCallback}
 * to interact with the elector
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ActiveStandbyElector implements StatCallback, StringCallback {

  /**
   * Callback interface to interact with the ActiveStandbyElector object. <br/>
   * The application will be notified with a callback only on state changes
   * (i.e. there will never be successive calls to becomeActive without an
   * intermediate call to enterNeutralMode). <br/>
   * The callbacks will be running on Zookeeper client library threads. The
   * application should return from these callbacks quickly so as not to impede
   * Zookeeper client library performance and notifications. The app will
   * typically remember the state change and return from the callback. It will
   * then proceed with implementing actions around that state change. It is
   * possible to be called back again while these actions are in flight and the
   * app should handle this scenario.
   */
  public interface ActiveStandbyElectorCallback {
    /**
     * This method is called when the app becomes the active leader.
     * If the service fails to become active, it should throw
     * ServiceFailedException. This will cause the elector to
     * sleep for a short period, then re-join the election.
     * 
     * Callback implementations are expected to manage their own
     * timeouts (e.g. when making an RPC to a remote node).
     */
    void becomeActive() throws ServiceFailedException;

    /**
     * This method is called when the app becomes a standby
     */
    void becomeStandby();

    /**
     * If the elector gets disconnected from Zookeeper and does not know about
     * the lock state, then it will notify the service via the enterNeutralMode
     * interface. The service may choose to ignore this or stop doing state
     * changing operations. Upon reconnection, the elector verifies the leader
     * status and calls back on the becomeActive and becomeStandby app
     * interfaces. <br/>
     * Zookeeper disconnects can happen due to network issues or loss of
     * Zookeeper quorum. Thus enterNeutralMode can be used to guard against
     * split-brain issues. In such situations it might be prudent to call
     * becomeStandby too. However, such state change operations might be
     * expensive and enterNeutralMode can help guard against doing that for
     * transient issues.
     */
    void enterNeutralMode();

    /**
     * If there is any fatal error (e.g. wrong ACL's, unexpected Zookeeper
     * errors or Zookeeper persistent unavailability) then notifyFatalError is
     * called to notify the app about it.
     */
    void notifyFatalError(String errorMessage);

    /**
     * If an old active has failed, rather than exited gracefully, then
     * the new active may need to take some fencing actions against it
     * before proceeding with failover.
     * 
     * @param oldActiveData the application data provided by the prior active
     */
    void fenceOldActive(byte[] oldActiveData);
  }

  /**
   * Name of the lock znode used by the library. Protected for access in test
   * classes
   */
  @VisibleForTesting
  protected static final String LOCK_FILENAME = "ActiveStandbyElectorLock";
  @VisibleForTesting
  protected static final String BREADCRUMB_FILENAME = "ActiveBreadCrumb";

  public static final Logger LOG =
      LoggerFactory.getLogger(ActiveStandbyElector.class);

  private static final int SLEEP_AFTER_FAILURE_TO_BECOME_ACTIVE = 1000;

  private enum ConnectionState {
    DISCONNECTED, CONNECTED, TERMINATED
  };

  enum State {
    INIT, ACTIVE, STANDBY, NEUTRAL
  };

  private State state = State.INIT;
  private int createRetryCount = 0;
  private int statRetryCount = 0;
  private ZooKeeper zkClient;
  private WatcherWithClientRef watcher;
  private ConnectionState zkConnectionState = ConnectionState.TERMINATED;

  private final ActiveStandbyElectorCallback appClient;
  private final String zkHostPort;
  private final int zkSessionTimeout;
  private final List<ACL> zkAcl;
  private final List<ZKAuthInfo> zkAuthInfo;
  private byte[] appData;
  private final String zkLockFilePath;
  private final String zkBreadCrumbPath;
  private final String znodeWorkingDir;
  private final int maxRetryNum;

  private Lock sessionReestablishLockForTests = new ReentrantLock();
  private boolean wantToBeInElection;
  private boolean monitorLockNodePending = false;
  private ZooKeeper monitorLockNodeClient;

  /**
   * Create a new ActiveStandbyElector object <br/>
   * The elector is created by providing to it the Zookeeper configuration, the
   * parent znode under which to create the znode and a reference to the
   * callback interface. <br/>
   * The parent znode name must be the same for all service instances and
   * different across services. <br/>
   * After the leader has been lost, a new leader will be elected after the
   * session timeout expires. Hence, the app must set this parameter based on
   * its needs for failure response time. The session timeout must be greater
   * than the Zookeeper disconnect timeout and is recommended to be 3X that
   * value to enable Zookeeper to retry transient disconnections. Setting a very
   * short session timeout may result in frequent transitions between active and
   * standby states during issues like network outages/GS pauses.
   * 
   * @param zookeeperHostPorts
   *          ZooKeeper hostPort for all ZooKeeper servers
   * @param zookeeperSessionTimeout
   *          ZooKeeper session timeout
   * @param parentZnodeName
   *          znode under which to create the lock
   * @param acl
   *          ZooKeeper ACL's
   * @param authInfo a list of authentication credentials to add to the
   *                 ZK connection
   * @param app
   *          reference to callback interface object
   * @throws IOException
   * @throws HadoopIllegalArgumentException
   */
  public ActiveStandbyElector(String zookeeperHostPorts,
      int zookeeperSessionTimeout, String parentZnodeName, List<ACL> acl,
      List<ZKAuthInfo> authInfo, ActiveStandbyElectorCallback app,
      int maxRetryNum) throws IOException, HadoopIllegalArgumentException,
      KeeperException {
    this(zookeeperHostPorts, zookeeperSessionTimeout, parentZnodeName, acl,
      authInfo, app, maxRetryNum, true);
  }

  /**
   * Create a new ActiveStandbyElector object <br/>
   * The elector is created by providing to it the Zookeeper configuration, the
   * parent znode under which to create the znode and a reference to the
   * callback interface. <br/>
   * The parent znode name must be the same for all service instances and
   * different across services. <br/>
   * After the leader has been lost, a new leader will be elected after the
   * session timeout expires. Hence, the app must set this parameter based on
   * its needs for failure response time. The session timeout must be greater
   * than the Zookeeper disconnect timeout and is recommended to be 3X that
   * value to enable Zookeeper to retry transient disconnections. Setting a very
   * short session timeout may result in frequent transitions between active and
   * standby states during issues like network outages/GS pauses.
   * 
   * @param zookeeperHostPorts
   *          ZooKeeper hostPort for all ZooKeeper servers
   * @param zookeeperSessionTimeout
   *          ZooKeeper session timeout
   * @param parentZnodeName
   *          znode under which to create the lock
   * @param acl
   *          ZooKeeper ACL's
   * @param authInfo a list of authentication credentials to add to the
   *                 ZK connection
   * @param app
   *          reference to callback interface object
   * @param failFast
   *          whether need to add the retry when establishing ZK connection.
   * @throws IOException
   * @throws HadoopIllegalArgumentException
   */
  public ActiveStandbyElector(String zookeeperHostPorts,
      int zookeeperSessionTimeout, String parentZnodeName, List<ACL> acl,
      List<ZKAuthInfo> authInfo, ActiveStandbyElectorCallback app,
      int maxRetryNum, boolean failFast) throws IOException,
      HadoopIllegalArgumentException, KeeperException {
    if (app == null || acl == null || parentZnodeName == null
        || zookeeperHostPorts == null || zookeeperSessionTimeout <= 0) {
      throw new HadoopIllegalArgumentException("Invalid argument");
    }
    zkHostPort = zookeeperHostPorts;
    zkSessionTimeout = zookeeperSessionTimeout;
    zkAcl = acl;
    zkAuthInfo = authInfo;
    appClient = app;
    znodeWorkingDir = parentZnodeName;
    zkLockFilePath = znodeWorkingDir + "/" + LOCK_FILENAME;
    zkBreadCrumbPath = znodeWorkingDir + "/" + BREADCRUMB_FILENAME;
    this.maxRetryNum = maxRetryNum;

    // establish the ZK Connection for future API calls
    if (failFast) {
      createConnection();
    } else {
      reEstablishSession();
    }
  }

  /**
   * To participate in election, the app will call joinElection. The result will
   * be notified by a callback on either the becomeActive or becomeStandby app
   * interfaces. <br/>
   * After this the elector will automatically monitor the leader status and
   * perform re-election if necessary<br/>
   * The app could potentially start off in standby mode and ignore the
   * becomeStandby call.
   * 
   * @param data
   *          to be set by the app. non-null data must be set.
   * @throws HadoopIllegalArgumentException
   *           if valid data is not supplied
   */
  public synchronized void joinElection(byte[] data)
      throws HadoopIllegalArgumentException {
    
    if (data == null) {
      throw new HadoopIllegalArgumentException("data cannot be null");
    }
    
    if (wantToBeInElection) {
      LOG.info("Already in election. Not re-connecting.");
      return;
    }

    appData = new byte[data.length];
    System.arraycopy(data, 0, appData, 0, data.length);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Attempting active election for " + this);
    }
    joinElectionInternal();
  }
  
  /**
   * @return true if the configured parent znode exists
   */
  public synchronized boolean parentZNodeExists()
      throws IOException, InterruptedException {
    Preconditions.checkState(zkClient != null);
    try {
      return zkClient.exists(znodeWorkingDir, false) != null;
    } catch (KeeperException e) {
      throw new IOException("Couldn't determine existence of znode '" +
          znodeWorkingDir + "'", e);
    }
  }

  /**
   * Utility function to ensure that the configured base znode exists.
   * This recursively creates the znode as well as all of its parents.
   */
  public synchronized void ensureParentZNode()
      throws IOException, InterruptedException {
    Preconditions.checkState(!wantToBeInElection,
        "ensureParentZNode() may not be called while in the election");

    String pathParts[] = znodeWorkingDir.split("/");
    Preconditions.checkArgument(pathParts.length >= 1 &&
        pathParts[0].isEmpty(),
        "Invalid path: %s", znodeWorkingDir);
    
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i < pathParts.length; i++) {
      sb.append("/").append(pathParts[i]);
      String prefixPath = sb.toString();
      LOG.debug("Ensuring existence of " + prefixPath);
      try {
        createWithRetries(prefixPath, new byte[]{}, zkAcl, CreateMode.PERSISTENT);
      } catch (KeeperException e) {
        if (isNodeExists(e.code())) {
          // Set ACLs for parent node, if they do not exist or are different
          try {
            setAclsWithRetries(prefixPath);
          } catch (KeeperException e1) {
            throw new IOException("Couldn't set ACLs on parent ZNode: " +
                prefixPath, e1);
          }
        } else {
          throw new IOException("Couldn't create " + prefixPath, e);
        }
      }
    }
    
    LOG.info("Successfully created " + znodeWorkingDir + " in ZK.");
  }
  
  /**
   * Clear all of the state held within the parent ZNode.
   * This recursively deletes everything within the znode as well as the
   * parent znode itself. It should only be used when it's certain that
   * no electors are currently participating in the election.
   */
  public synchronized void clearParentZNode()
      throws IOException, InterruptedException {
    Preconditions.checkState(!wantToBeInElection,
        "clearParentZNode() may not be called while in the election");

    try {
      LOG.info("Recursively deleting " + znodeWorkingDir + " from ZK...");

      zkDoWithRetries(new ZKAction<Void>() {
        @Override
        public Void run() throws KeeperException, InterruptedException {
          ZKUtil.deleteRecursive(zkClient, znodeWorkingDir);
          return null;
        }
      });
    } catch (KeeperException e) {
      throw new IOException("Couldn't clear parent znode " + znodeWorkingDir,
          e);
    }
    LOG.info("Successfully deleted " + znodeWorkingDir + " from ZK.");
  }


  /**
   * Any service instance can drop out of the election by calling quitElection. 
   * <br/>
   * This will lose any leader status, if held, and stop monitoring of the lock
   * node. <br/>
   * If the instance wants to participate in election again, then it needs to
   * call joinElection(). <br/>
   * This allows service instances to take themselves out of rotation for known
   * impending unavailable states (e.g. long GC pause or software upgrade).
   * 
   * @param needFence true if the underlying daemon may need to be fenced
   * if a failover occurs due to dropping out of the election.
   */
  public synchronized void quitElection(boolean needFence) {
    LOG.info("Yielding from election");
    if (!needFence && state == State.ACTIVE) {
      // If active is gracefully going back to standby mode, remove
      // our permanent znode so no one fences us.
      tryDeleteOwnBreadCrumbNode();
    }
    reset();
    wantToBeInElection = false;
  }

  /**
   * Exception thrown when there is no active leader
   */
  public static class ActiveNotFoundException extends Exception {
    private static final long serialVersionUID = 3505396722342846462L;
  }

  /**
   * get data set by the active leader
   * 
   * @return data set by the active instance
   * @throws ActiveNotFoundException
   *           when there is no active leader
   * @throws KeeperException
   *           other zookeeper operation errors
   * @throws InterruptedException
   * @throws IOException
   *           when ZooKeeper connection could not be established
   */
  public synchronized byte[] getActiveData() throws ActiveNotFoundException,
      KeeperException, InterruptedException, IOException {
    try {
      if (zkClient == null) {
        createConnection();
      }
      Stat stat = new Stat();
      return getDataWithRetries(zkLockFilePath, false, stat);
    } catch(KeeperException e) {
      Code code = e.code();
      if (isNodeDoesNotExist(code)) {
        // handle the commonly expected cases that make sense for us
        throw new ActiveNotFoundException();
      } else {
        throw e;
      }
    }
  }

  /**
   * interface implementation of Zookeeper callback for create
   */
  @Override
  public synchronized void processResult(int rc, String path, Object ctx,
      String name) {
    if (isStaleClient(ctx)) return;
    if (LOG.isDebugEnabled()) {
      LOG.debug("CreateNode result: " + rc + " for path: " + path
          + " connectionState: " + zkConnectionState +
          "  for " + this);
    }

    Code code = Code.get(rc);
    if (isSuccess(code)) {
      // we successfully created the znode. we are the leader. start monitoring
      if (becomeActive()) {
        monitorActiveStatus();
      } else {
        reJoinElectionAfterFailureToBecomeActive();
      }
      return;
    }

    if (isNodeExists(code)) {
      if (createRetryCount == 0) {
        // znode exists and we did not retry the operation. so a different
        // instance has created it. become standby and monitor lock.
        becomeStandby();
      }
      // if we had retried then the znode could have been created by our first
      // attempt to the server (that we lost) and this node exists response is
      // for the second attempt. verify this case via ephemeral node owner. this
      // will happen on the callback for monitoring the lock.
      monitorActiveStatus();
      return;
    }

    String errorMessage = "Received create error from Zookeeper. code:"
        + code.toString() + " for path " + path;
    LOG.debug(errorMessage);

    if (shouldRetry(code)) {
      if (createRetryCount < maxRetryNum) {
        LOG.debug("Retrying createNode createRetryCount: " + createRetryCount);
        ++createRetryCount;
        createLockNodeAsync();
        return;
      }
      errorMessage = errorMessage
          + ". Not retrying further znode create connection errors.";
    } else if (isSessionExpired(code)) {
      // This isn't fatal - the client Watcher will re-join the election
      LOG.warn("Lock acquisition failed because session was lost");
      return;
    }

    fatalError(errorMessage);
  }

  /**
   * interface implementation of Zookeeper callback for monitor (exists)
   */
  @Override
  public synchronized void processResult(int rc, String path, Object ctx,
      Stat stat) {
    if (isStaleClient(ctx)) return;
    monitorLockNodePending = false;

    assert wantToBeInElection :
        "Got a StatNode result after quitting election";

    if (LOG.isDebugEnabled()) {
      LOG.debug("StatNode result: " + rc + " for path: " + path
          + " connectionState: " + zkConnectionState + " for " + this);
    }

    Code code = Code.get(rc);
    if (isSuccess(code)) {
      // the following owner check completes verification in case the lock znode
      // creation was retried
      if (stat.getEphemeralOwner() == zkClient.getSessionId()) {
        // we own the lock znode. so we are the leader
        if (!becomeActive()) {
          reJoinElectionAfterFailureToBecomeActive();
        }
      } else {
        // we dont own the lock znode. so we are a standby.
        becomeStandby();
      }
      // the watch set by us will notify about changes
      return;
    }

    if (isNodeDoesNotExist(code)) {
      // the lock znode disappeared before we started monitoring it
      enterNeutralMode();
      joinElectionInternal();
      return;
    }

    String errorMessage = "Received stat error from Zookeeper. code:"
        + code.toString();
    LOG.debug(errorMessage);

    if (shouldRetry(code)) {
      if (statRetryCount < maxRetryNum) {
        ++statRetryCount;
        monitorLockNodeAsync();
        return;
      }
      errorMessage = errorMessage
          + ". Not retrying further znode monitoring connection errors.";
    } else if (isSessionExpired(code)) {
      // This isn't fatal - the client Watcher will re-join the election
      LOG.warn("Lock monitoring failed because session was lost");
      return;
    }

    fatalError(errorMessage);
  }

  /**
   * We failed to become active. Re-join the election, but
   * sleep for a few seconds after terminating our existing
   * session, so that other nodes have a chance to become active.
   * The failure to become active is already logged inside
   * becomeActive().
   */
  private void reJoinElectionAfterFailureToBecomeActive() {
    reJoinElection(SLEEP_AFTER_FAILURE_TO_BECOME_ACTIVE);
  }

  /**
   * interface implementation of Zookeeper watch events (connection and node),
   * proxied by {@link WatcherWithClientRef}.
   */
  synchronized void processWatchEvent(ZooKeeper zk, WatchedEvent event) {
    Event.EventType eventType = event.getType();
    if (isStaleClient(zk)) return;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Watcher event type: " + eventType + " with state:"
          + event.getState() + " for path:" + event.getPath()
          + " connectionState: " + zkConnectionState
          + " for " + this);
    }

    if (eventType == Event.EventType.None) {
      // the connection state has changed
      switch (event.getState()) {
      case SyncConnected:
        LOG.info("Session connected.");
        // if the listener was asked to move to safe state then it needs to
        // be undone
        ConnectionState prevConnectionState = zkConnectionState;
        zkConnectionState = ConnectionState.CONNECTED;
        if (prevConnectionState == ConnectionState.DISCONNECTED &&
            wantToBeInElection) {
          monitorActiveStatus();
        }
        break;
      case Disconnected:
        LOG.info("Session disconnected. Entering neutral mode...");

        // ask the app to move to safe state because zookeeper connection
        // is not active and we dont know our state
        zkConnectionState = ConnectionState.DISCONNECTED;
        enterNeutralMode();
        break;
      case Expired:
        // the connection got terminated because of session timeout
        // call listener to reconnect
        LOG.info("Session expired. Entering neutral mode and rejoining...");
        enterNeutralMode();
        reJoinElection(0);
        break;
      case SaslAuthenticated:
        LOG.info("Successfully authenticated to ZooKeeper using SASL.");
        break;
      default:
        fatalError("Unexpected Zookeeper watch event state: "
            + event.getState());
        break;
      }

      return;
    }

    // a watch on lock path in zookeeper has fired. so something has changed on
    // the lock. ideally we should check that the path is the same as the lock
    // path but trusting zookeeper for now
    String path = event.getPath();
    if (path != null) {
      switch (eventType) {
      case NodeDeleted:
        if (state == State.ACTIVE) {
          enterNeutralMode();
        }
        joinElectionInternal();
        break;
      case NodeDataChanged:
        monitorActiveStatus();
        break;
      default:
        if (LOG.isDebugEnabled()) {
          LOG.debug("Unexpected node event: " + eventType + " for path: " + path);
        }
        monitorActiveStatus();
      }

      return;
    }

    // some unexpected error has occurred
    fatalError("Unexpected watch error from Zookeeper");
  }

  /**
   * Get a new zookeeper client instance. protected so that test class can
   * inherit and mock out the zookeeper instance
   * 
   * @return new zookeeper client instance
   * @throws IOException
   * @throws KeeperException zookeeper connectionloss exception
   */
  protected synchronized ZooKeeper connectToZooKeeper() throws IOException,
      KeeperException {
    
    // Unfortunately, the ZooKeeper constructor connects to ZooKeeper and
    // may trigger the Connected event immediately. So, if we register the
    // watcher after constructing ZooKeeper, we may miss that event. Instead,
    // we construct the watcher first, and have it block any events it receives
    // before we can set its ZooKeeper reference.
    watcher = new WatcherWithClientRef();
    ZooKeeper zk = createZooKeeper();
    watcher.setZooKeeperRef(zk);

    // Wait for the asynchronous success/failure. This may throw an exception
    // if we don't connect within the session timeout.
    watcher.waitForZKConnectionEvent(zkSessionTimeout);
    
    for (ZKAuthInfo auth : zkAuthInfo) {
      zk.addAuthInfo(auth.getScheme(), auth.getAuth());
    }
    return zk;
  }

  /**
   * Get a new zookeeper client instance. protected so that test class can
   * inherit and pass in a mock object for zookeeper
   *
   * @return new zookeeper client instance
   * @throws IOException
   */
  protected ZooKeeper createZooKeeper() throws IOException {
    return new ZooKeeper(zkHostPort, zkSessionTimeout, watcher);
  }

  private void fatalError(String errorMessage) {
    LOG.error(errorMessage);
    reset();
    appClient.notifyFatalError(errorMessage);
  }

  private void monitorActiveStatus() {
    assert wantToBeInElection;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Monitoring active leader for " + this);
    }
    statRetryCount = 0;
    monitorLockNodeAsync();
  }

  private void joinElectionInternal() {
    Preconditions.checkState(appData != null,
        "trying to join election without any app data");
    if (zkClient == null) {
      if (!reEstablishSession()) {
        fatalError("Failed to reEstablish connection with ZooKeeper");
        return;
      }
    }

    createRetryCount = 0;
    wantToBeInElection = true;
    createLockNodeAsync();
  }

  private void reJoinElection(int sleepTime) {
    LOG.info("Trying to re-establish ZK session");
    
    // Some of the test cases rely on expiring the ZK sessions and
    // ensuring that the other node takes over. But, there's a race
    // where the original lease holder could reconnect faster than the other
    // thread manages to take the lock itself. This lock allows the
    // tests to block the reconnection. It's a shame that this leaked
    // into non-test code, but the lock is only acquired here so will never
    // be contended.
    sessionReestablishLockForTests.lock();
    try {
      terminateConnection();
      sleepFor(sleepTime);
      // Should not join election even before the SERVICE is reported
      // as HEALTHY from ZKFC monitoring.
      if (appData != null) {
        joinElectionInternal();
      } else {
        LOG.info("Not joining election since service has not yet been " +
            "reported as healthy.");
      }
    } finally {
      sessionReestablishLockForTests.unlock();
    }
  }

  /**
   * Sleep for the given number of milliseconds.
   * This is non-static, and separated out, so that unit tests
   * can override the behavior not to sleep.
   */
  @VisibleForTesting
  protected void sleepFor(int sleepMs) {
    if (sleepMs > 0) {
      try {
        Thread.sleep(sleepMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @VisibleForTesting
  void preventSessionReestablishmentForTests() {
    sessionReestablishLockForTests.lock();
  }
  
  @VisibleForTesting
  void allowSessionReestablishmentForTests() {
    sessionReestablishLockForTests.unlock();
  }
  
  @VisibleForTesting
  synchronized long getZKSessionIdForTests() {
    if (zkClient != null) {
      return zkClient.getSessionId();
    } else {
      return -1;
    }
  }
  
  @VisibleForTesting
  synchronized State getStateForTests() {
    return state;
  }

  @VisibleForTesting
  synchronized boolean isMonitorLockNodePending() {
    return monitorLockNodePending;
  }

  private boolean reEstablishSession() {
    int connectionRetryCount = 0;
    boolean success = false;
    while(!success && connectionRetryCount < maxRetryNum) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Establishing zookeeper connection for " + this);
      }
      try {
        createConnection();
        success = true;
      } catch(IOException e) {
        LOG.warn(e.toString());
        sleepFor(5000);
      } catch(KeeperException e) {
        LOG.warn(e.toString());
        sleepFor(5000);
      }
      ++connectionRetryCount;
    }
    return success;
  }

  private void createConnection() throws IOException, KeeperException {
    if (zkClient != null) {
      try {
        zkClient.close();
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while closing ZK",
            e);
      }
      zkClient = null;
      watcher = null;
    }
    zkClient = connectToZooKeeper();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created new connection for " + this);
    }
  }

  @InterfaceAudience.Private
  public synchronized void terminateConnection() {
    if (zkClient == null) {
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Terminating ZK connection for " + this);
    }
    ZooKeeper tempZk = zkClient;
    zkClient = null;
    watcher = null;
    try {
      tempZk.close();
    } catch(InterruptedException e) {
      LOG.warn(e.toString());
    }
    zkConnectionState = ConnectionState.TERMINATED;
    wantToBeInElection = false;
  }

  private void reset() {
    state = State.INIT;
    terminateConnection();
  }

  private boolean becomeActive() {
    assert wantToBeInElection;
    if (state == State.ACTIVE) {
      // already active
      return true;
    }
    try {
      Stat oldBreadcrumbStat = fenceOldActive();
      writeBreadCrumbNode(oldBreadcrumbStat);

      LOG.debug("Becoming active for {}", this);

      appClient.becomeActive();
      state = State.ACTIVE;
      return true;
    } catch (Exception e) {
      LOG.warn("Exception handling the winning of election", e);
      // Caller will handle quitting and rejoining the election.
      return false;
    }
  }

  /**
   * Write the "ActiveBreadCrumb" node, indicating that this node may need
   * to be fenced on failover.
   * @param oldBreadcrumbStat 
   */
  private void writeBreadCrumbNode(Stat oldBreadcrumbStat)
      throws KeeperException, InterruptedException {
    Preconditions.checkState(appData != null, "no appdata");
    
    LOG.info("Writing znode {} to indicate that the local " +
        "node is the most recent active...", zkBreadCrumbPath);
    if (oldBreadcrumbStat == null) {
      // No previous active, just create the node
      createWithRetries(zkBreadCrumbPath, appData, zkAcl,
        CreateMode.PERSISTENT);
    } else {
      // There was a previous active, update the node
      setDataWithRetries(zkBreadCrumbPath, appData, oldBreadcrumbStat.getVersion());
    }
  }
  
  /**
   * Try to delete the "ActiveBreadCrumb" node when gracefully giving up
   * active status.
   * If this fails, it will simply warn, since the graceful release behavior
   * is only an optimization.
   */
  private void tryDeleteOwnBreadCrumbNode() {
    assert state == State.ACTIVE;
    LOG.info("Deleting bread-crumb of active node...");
    
    // Sanity check the data. This shouldn't be strictly necessary,
    // but better to play it safe.
    Stat stat = new Stat();
    byte[] data = null;
    try {
      data = zkClient.getData(zkBreadCrumbPath, false, stat);

      if (!Arrays.equals(data, appData)) {
        throw new IllegalStateException(
            "We thought we were active, but in fact " +
            "the active znode had the wrong data: " +
            StringUtils.byteToHexString(data) + " (stat=" + stat + ")");
      }
      
      deleteWithRetries(zkBreadCrumbPath, stat.getVersion());
    } catch (Exception e) {
      LOG.warn("Unable to delete our own bread-crumb of being active at {}." +
          ". Expecting to be fenced by the next active.", zkBreadCrumbPath, e);
    }
  }

  /**
   * If there is a breadcrumb node indicating that another node may need
   * fencing, try to fence that node.
   * @return the Stat of the breadcrumb node that was read, or null
   * if no breadcrumb node existed
   */
  private Stat fenceOldActive() throws InterruptedException, KeeperException {
    final Stat stat = new Stat();
    byte[] data;
    LOG.info("Checking for any old active which needs to be fenced...");
    try {
      data = zkDoWithRetries(new ZKAction<byte[]>() {
        @Override
        public byte[] run() throws KeeperException, InterruptedException {
          return zkClient.getData(zkBreadCrumbPath, false, stat);
        }
      });
    } catch (KeeperException ke) {
      if (isNodeDoesNotExist(ke.code())) {
        LOG.info("No old node to fence");
        return null;
      }
      
      // If we failed to read for any other reason, then likely we lost
      // our session, or we don't have permissions, etc. In any case,
      // we probably shouldn't become active, and failing the whole
      // thing is the best bet.
      throw ke;
    }

    LOG.info("Old node exists: {}", StringUtils.byteToHexString(data));
    if (Arrays.equals(data, appData)) {
      LOG.info("But old node has our own data, so don't need to fence it.");
    } else {
      appClient.fenceOldActive(data);
    }
    return stat;
  }

  private void becomeStandby() {
    if (state != State.STANDBY) {
      LOG.debug("Becoming standby for {}", this);
      state = State.STANDBY;
      appClient.becomeStandby();
    }
  }

  private void enterNeutralMode() {
    if (state != State.NEUTRAL) {
      LOG.debug("Entering neutral mode for {}", this);
      state = State.NEUTRAL;
      appClient.enterNeutralMode();
    }
  }

  private void createLockNodeAsync() {
    zkClient.create(zkLockFilePath, appData, zkAcl, CreateMode.EPHEMERAL,
        this, zkClient);
  }

  private void monitorLockNodeAsync() {
    if (monitorLockNodePending && monitorLockNodeClient == zkClient) {
      LOG.info("Ignore duplicate monitor lock-node request.");
      return;
    }
    monitorLockNodePending = true;
    monitorLockNodeClient = zkClient;
    zkClient.exists(zkLockFilePath,
        watcher, this,
        zkClient);
  }

  private String createWithRetries(final String path, final byte[] data,
      final List<ACL> acl, final CreateMode mode)
      throws InterruptedException, KeeperException {
    return zkDoWithRetries(new ZKAction<String>() {
      @Override
      public String run() throws KeeperException, InterruptedException {
        return zkClient.create(path, data, acl, mode);
      }
    });
  }

  private byte[] getDataWithRetries(final String path, final boolean watch,
      final Stat stat) throws InterruptedException, KeeperException {
    return zkDoWithRetries(new ZKAction<byte[]>() {
      @Override
      public byte[] run() throws KeeperException, InterruptedException {
        return zkClient.getData(path, watch, stat);
      }
    });
  }

  private Stat setDataWithRetries(final String path, final byte[] data,
      final int version) throws InterruptedException, KeeperException {
    return zkDoWithRetries(new ZKAction<Stat>() {
      @Override
      public Stat run() throws KeeperException, InterruptedException {
        return zkClient.setData(path, data, version);
      }
    });
  }
  
  private void deleteWithRetries(final String path, final int version)
      throws KeeperException, InterruptedException {
    zkDoWithRetries(new ZKAction<Void>() {
      @Override
      public Void run() throws KeeperException, InterruptedException {
        zkClient.delete(path, version);
        return null;
      }
    });
  }

  private void setAclsWithRetries(final String path)
      throws KeeperException, InterruptedException {
    Stat stat = new Stat();
    zkDoWithRetries(new ZKAction<Void>() {
      @Override
      public Void run() throws KeeperException, InterruptedException {
        List<ACL> acl = zkClient.getACL(path, stat);
        if (acl == null || !acl.containsAll(zkAcl) ||
            !zkAcl.containsAll(acl)) {
          zkClient.setACL(path, zkAcl, stat.getAversion());
        }
        return null;
      }
    }, Code.BADVERSION);
  }

  private <T> T zkDoWithRetries(ZKAction<T> action) throws KeeperException,
      InterruptedException {
    return zkDoWithRetries(action, null);
  }

  private <T> T zkDoWithRetries(ZKAction<T> action, Code retryCode)
      throws KeeperException, InterruptedException {
    int retry = 0;
    while (true) {
      try {
        return action.run();
      } catch (KeeperException ke) {
        if ((shouldRetry(ke.code()) || shouldRetry(ke.code(), retryCode))
            && ++retry < maxRetryNum) {
          continue;
        }
        throw ke;
      }
    }
  }

  private interface ZKAction<T> {
    T run() throws KeeperException, InterruptedException; 
  }
  
  /**
   * The callbacks and watchers pass a reference to the ZK client
   * which made the original call. We don't want to take action
   * based on any callbacks from prior clients after we quit
   * the election.
   * @param ctx the ZK client passed into the watcher
   * @return true if it matches the current client
   */
  private synchronized boolean isStaleClient(Object ctx) {
    Preconditions.checkNotNull(ctx);
    if (zkClient != (ZooKeeper)ctx) {
      LOG.warn("Ignoring stale result from old client with sessionId {}",
          String.format("0x%08x", ((ZooKeeper)ctx).getSessionId()));
      return true;
    }
    return false;
  }

  /**
   * Watcher implementation which keeps a reference around to the
   * original ZK connection, and passes it back along with any
   * events.
   */
  private final class WatcherWithClientRef implements Watcher {
    private ZooKeeper zk;
    
    /**
     * Latch fired whenever any event arrives. This is used in order
     * to wait for the Connected event when the client is first created.
     */
    private CountDownLatch hasReceivedEvent = new CountDownLatch(1);

    /**
     * Latch used to wait until the reference to ZooKeeper is set.
     */
    private CountDownLatch hasSetZooKeeper = new CountDownLatch(1);

    /**
     * Waits for the next event from ZooKeeper to arrive.
     * 
     * @param connectionTimeoutMs zookeeper connection timeout in milliseconds
     * @throws KeeperException if the connection attempt times out. This will
     * be a ZooKeeper ConnectionLoss exception code.
     * @throws IOException if interrupted while connecting to ZooKeeper
     */
    private void waitForZKConnectionEvent(int connectionTimeoutMs)
        throws KeeperException, IOException {
      try {
        if (!hasReceivedEvent.await(connectionTimeoutMs, TimeUnit.MILLISECONDS)) {
          LOG.error("Connection timed out: couldn't connect to ZooKeeper in " +
              "{} milliseconds", connectionTimeoutMs);
          zk.close();
          throw KeeperException.create(Code.CONNECTIONLOSS);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(
            "Interrupted when connecting to zookeeper server", e);
      }
    }

    private void setZooKeeperRef(ZooKeeper zk) {
      Preconditions.checkState(this.zk == null,
          "zk already set -- must be set exactly once");
      this.zk = zk;
      hasSetZooKeeper.countDown();
    }

    @Override
    public void process(WatchedEvent event) {
      hasReceivedEvent.countDown();
      try {
        if (!hasSetZooKeeper.await(zkSessionTimeout, TimeUnit.MILLISECONDS)) {
          LOG.debug("Event received with stale zk");
        }
        ActiveStandbyElector.this.processWatchEvent(
            zk, event);
      } catch (Throwable t) {
        fatalError(
            "Failed to process watcher event " + event + ": " +
            StringUtils.stringifyException(t));
      }
    }
  }

  private static boolean isSuccess(Code code) {
    return (code == Code.OK);
  }

  private static boolean isNodeExists(Code code) {
    return (code == Code.NODEEXISTS);
  }

  private static boolean isNodeDoesNotExist(Code code) {
    return (code == Code.NONODE);
  }
  
  private static boolean isSessionExpired(Code code) {
    return (code == Code.SESSIONEXPIRED);
  }

  private static boolean shouldRetry(Code code) {
    return code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT;
  }

  private static boolean shouldRetry(Code code, Code retryIfCode) {
    return (retryIfCode == null ? false : retryIfCode == code);
  }
  
  @Override
  public String toString() {
    return "elector id=" + System.identityHashCode(this) +
      " appData=" +
      ((appData == null) ? "null" : StringUtils.byteToHexString(appData)) + 
      " cb=" + appClient;
  }

  public String getHAZookeeperConnectionState() {
    return this.zkConnectionState.name();
  }
}
