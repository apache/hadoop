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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.AsyncCallback.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException.Code;

import com.google.common.annotations.VisibleForTesting;

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
public class ActiveStandbyElector implements Watcher, StringCallback,
    StatCallback {

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
     * This method is called when the app becomes the active leader
     */
    void becomeActive();

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
  }

  /**
   * Name of the lock znode used by the library. Protected for access in test
   * classes
   */
  @VisibleForTesting
  protected static final String LOCKFILENAME = "ActiveStandbyElectorLock";

  public static final Log LOG = LogFactory.getLog(ActiveStandbyElector.class);

  private static final int NUM_RETRIES = 3;

  private enum ConnectionState {
    DISCONNECTED, CONNECTED, TERMINATED
  };

  private enum State {
    INIT, ACTIVE, STANDBY, NEUTRAL
  };

  private State state = State.INIT;
  private int createRetryCount = 0;
  private int statRetryCount = 0;
  private ZooKeeper zkClient;
  private ConnectionState zkConnectionState = ConnectionState.TERMINATED;

  private final ActiveStandbyElectorCallback appClient;
  private final String zkHostPort;
  private final int zkSessionTimeout;
  private final List<ACL> zkAcl;
  private byte[] appData;
  private final String zkLockFilePath;
  private final String znodeWorkingDir;

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
   * @param app
   *          reference to callback interface object
   * @throws IOException
   * @throws HadoopIllegalArgumentException
   */
  public ActiveStandbyElector(String zookeeperHostPorts,
      int zookeeperSessionTimeout, String parentZnodeName, List<ACL> acl,
      ActiveStandbyElectorCallback app) throws IOException,
      HadoopIllegalArgumentException {
    if (app == null || acl == null || parentZnodeName == null
        || zookeeperHostPorts == null || zookeeperSessionTimeout <= 0) {
      throw new HadoopIllegalArgumentException("Invalid argument");
    }
    zkHostPort = zookeeperHostPorts;
    zkSessionTimeout = zookeeperSessionTimeout;
    zkAcl = acl;
    appClient = app;
    znodeWorkingDir = parentZnodeName;
    zkLockFilePath = znodeWorkingDir + "/" + LOCKFILENAME;

    // createConnection for future API calls
    createConnection();
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
    LOG.debug("Attempting active election");

    if (data == null) {
      throw new HadoopIllegalArgumentException("data cannot be null");
    }

    appData = new byte[data.length];
    System.arraycopy(data, 0, appData, 0, data.length);

    joinElectionInternal();
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
   */
  public synchronized void quitElection() {
    LOG.debug("Yielding from election");
    reset();
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
      return zkClient.getData(zkLockFilePath, false, stat);
    } catch(KeeperException e) {
      Code code = e.code();
      if (operationNodeDoesNotExist(code)) {
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
    LOG.debug("CreateNode result: " + rc + " for path: " + path
        + " connectionState: " + zkConnectionState);
    if (zkClient == null) {
      // zkClient is nulled before closing the connection
      // this is the callback with session expired after we closed the session
      return;
    }

    Code code = Code.get(rc);
    if (operationSuccess(code)) {
      // we successfully created the znode. we are the leader. start monitoring
      becomeActive();
      monitorActiveStatus();
      return;
    }

    if (operationNodeExists(code)) {
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
        + code.toString();
    LOG.debug(errorMessage);

    if (operationRetry(code)) {
      if (createRetryCount < NUM_RETRIES) {
        LOG.debug("Retrying createNode createRetryCount: " + createRetryCount);
        ++createRetryCount;
        createNode();
        return;
      }
      errorMessage = errorMessage
          + ". Not retrying further znode create connection errors.";
    }

    fatalError(errorMessage);
  }

  /**
   * interface implementation of Zookeeper callback for monitor (exists)
   */
  @Override
  public synchronized void processResult(int rc, String path, Object ctx,
      Stat stat) {
    LOG.debug("StatNode result: " + rc + " for path: " + path
        + " connectionState: " + zkConnectionState);
    if (zkClient == null) {
      // zkClient is nulled before closing the connection
      // this is the callback with session expired after we closed the session
      return;
    }

    Code code = Code.get(rc);
    if (operationSuccess(code)) {
      // the following owner check completes verification in case the lock znode
      // creation was retried
      if (stat.getEphemeralOwner() == zkClient.getSessionId()) {
        // we own the lock znode. so we are the leader
        becomeActive();
      } else {
        // we dont own the lock znode. so we are a standby.
        becomeStandby();
      }
      // the watch set by us will notify about changes
      return;
    }

    if (operationNodeDoesNotExist(code)) {
      // the lock znode disappeared before we started monitoring it
      enterNeutralMode();
      joinElectionInternal();
      return;
    }

    String errorMessage = "Received stat error from Zookeeper. code:"
        + code.toString();
    LOG.debug(errorMessage);

    if (operationRetry(code)) {
      if (statRetryCount < NUM_RETRIES) {
        ++statRetryCount;
        monitorNode();
        return;
      }
      errorMessage = errorMessage
          + ". Not retrying further znode monitoring connection errors.";
    }

    fatalError(errorMessage);
  }

  /**
   * interface implementation of Zookeeper watch events (connection and node)
   */
  @Override
  public synchronized void process(WatchedEvent event) {
    Event.EventType eventType = event.getType();
    LOG.debug("Watcher event type: " + eventType + " with state:"
        + event.getState() + " for path:" + event.getPath()
        + " connectionState: " + zkConnectionState);
    if (zkClient == null) {
      // zkClient is nulled before closing the connection
      // this is the callback with session expired after we closed the session
      return;
    }

    if (eventType == Event.EventType.None) {
      // the connection state has changed
      switch (event.getState()) {
      case SyncConnected:
        // if the listener was asked to move to safe state then it needs to
        // be undone
        ConnectionState prevConnectionState = zkConnectionState;
        zkConnectionState = ConnectionState.CONNECTED;
        if (prevConnectionState == ConnectionState.DISCONNECTED) {
          monitorActiveStatus();
        }
        break;
      case Disconnected:
        // ask the app to move to safe state because zookeeper connection
        // is not active and we dont know our state
        zkConnectionState = ConnectionState.DISCONNECTED;
        enterNeutralMode();
        break;
      case Expired:
        // the connection got terminated because of session timeout
        // call listener to reconnect
        enterNeutralMode();
        reJoinElection();
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
        LOG.debug("Unexpected node event: " + eventType + " for path: " + path);
        monitorActiveStatus();
      }

      return;
    }

    // some unexpected error has occurred
    fatalError("Unexpected watch error from Zookeeper");
  }

  /**
   * Get a new zookeeper client instance. protected so that test class can
   * inherit and pass in a mock object for zookeeper
   * 
   * @return new zookeeper client instance
   * @throws IOException
   */
  protected synchronized ZooKeeper getNewZooKeeper() throws IOException {
    return new ZooKeeper(zkHostPort, zkSessionTimeout, this);
  }

  private void fatalError(String errorMessage) {
    reset();
    appClient.notifyFatalError(errorMessage);
  }

  private void monitorActiveStatus() {
    LOG.debug("Monitoring active leader");
    statRetryCount = 0;
    monitorNode();
  }

  private void joinElectionInternal() {
    if (zkClient == null) {
      if (!reEstablishSession()) {
        fatalError("Failed to reEstablish connection with ZooKeeper");
        return;
      }
    }

    createRetryCount = 0;
    createNode();
  }

  private void reJoinElection() {
    LOG.debug("Trying to re-establish ZK session");
    terminateConnection();
    joinElectionInternal();
  }

  private boolean reEstablishSession() {
    int connectionRetryCount = 0;
    boolean success = false;
    while(!success && connectionRetryCount < NUM_RETRIES) {
      LOG.debug("Establishing zookeeper connection");
      try {
        createConnection();
        success = true;
      } catch(IOException e) {
        LOG.warn(e);
        try {
          Thread.sleep(5000);
        } catch(InterruptedException e1) {
          LOG.warn(e1);
        }
      }
      ++connectionRetryCount;
    }
    return success;
  }

  private void createConnection() throws IOException {
    zkClient = getNewZooKeeper();
  }

  private void terminateConnection() {
    if (zkClient == null) {
      return;
    }
    LOG.debug("Terminating ZK connection");
    ZooKeeper tempZk = zkClient;
    zkClient = null;
    try {
      tempZk.close();
    } catch(InterruptedException e) {
      LOG.warn(e);
    }
    zkConnectionState = ConnectionState.TERMINATED;
  }

  private void reset() {
    state = State.INIT;
    terminateConnection();
  }

  private void becomeActive() {
    if (state != State.ACTIVE) {
      LOG.debug("Becoming active");
      state = State.ACTIVE;
      appClient.becomeActive();
    }
  }

  private void becomeStandby() {
    if (state != State.STANDBY) {
      LOG.debug("Becoming standby");
      state = State.STANDBY;
      appClient.becomeStandby();
    }
  }

  private void enterNeutralMode() {
    if (state != State.NEUTRAL) {
      LOG.debug("Entering neutral mode");
      state = State.NEUTRAL;
      appClient.enterNeutralMode();
    }
  }

  private void createNode() {
    zkClient.create(zkLockFilePath, appData, zkAcl, CreateMode.EPHEMERAL, this,
        null);
  }

  private void monitorNode() {
    zkClient.exists(zkLockFilePath, true, this, null);
  }

  private boolean operationSuccess(Code code) {
    return (code == Code.OK);
  }

  private boolean operationNodeExists(Code code) {
    return (code == Code.NODEEXISTS);
  }

  private boolean operationNodeDoesNotExist(Code code) {
    return (code == Code.NONODE);
  }

  private boolean operationRetry(Code code) {
    switch (code) {
    case CONNECTIONLOSS:
    case OPERATIONTIMEOUT:
      return true;
    }
    return false;
  }

}
