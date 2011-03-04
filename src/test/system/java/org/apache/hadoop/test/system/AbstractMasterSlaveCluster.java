package org.apache.hadoop.test.system;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.system.process.ClusterProcessManager;
import org.apache.hadoop.test.system.process.RemoteProcess;

/**
 * Abstract class which Represents a cluster, which contains a single master and
 * one or more slave.<br/>
 * 
 * @param Master
 *          daemon client type.
 * @param Slave
 *          daemon client type.
 */
public abstract class AbstractMasterSlaveCluster
    <MASTER extends AbstractDaemonClient, SLAVE extends AbstractDaemonClient> {

  public static final String WAITFORMASTERKEY = 
    "test.system.abstractmasterslavecluster.waitformaster";
  
  private static final Log LOG = 
    LogFactory.getLog(AbstractMasterSlaveCluster.class);

  private Configuration conf;
  protected ClusterProcessManager clusterManager;
  private MASTER master;
  private Map<String, SLAVE> slaves = new HashMap<String, SLAVE>();
  private boolean waitformaster = false;

  /**
   * Constructor to create a master slave cluster.<br/>
   * 
   * @param conf
   *          Configuration to be used while constructing the cluster.
   * @param rcluster
   *          process manger instance to be used for managing the daemons.
   * 
   * @throws IOException
   */
  public AbstractMasterSlaveCluster(Configuration conf,
      ClusterProcessManager rcluster) throws IOException {
    this.conf = conf;
    this.clusterManager = rcluster;
    this.master = createMaster(clusterManager.getMaster());
    Iterator<Map.Entry<String, RemoteProcess>> it = clusterManager.getSlaves()
        .entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, RemoteProcess> entry = it.next();
      slaves.put(entry.getKey(), createSlave(entry.getValue()));
    }
    this.waitformaster = conf.getBoolean(WAITFORMASTERKEY, true);
  }

  /**
   * Method to create the master daemon client.<br/>
   * 
   * @param remoteprocess
   *          to manage the master daemon.
   * @return instance of the daemon client of master daemon.
   * 
   * @throws IOException
   */
  protected abstract MASTER createMaster(RemoteProcess masterDaemon)
      throws IOException;

  /**
   * Method to create the slave daemons clients.<br/>
   * 
   * @param remoteprocess
   *          to manage the slave daemons.
   * @return instance of the daemon clients of slave daemons.
   * 
   * @throws IOException
   */
  protected abstract SLAVE createSlave(RemoteProcess slaveDaemon)
      throws IOException;

  /**
   * Get the global cluster configuration which was used to create the 
   * cluster. <br/>
   * 
   * @return global configuration of the cluster.
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Return the client handle of the master Daemon.<br/>
   * 
   * @return master daemon client handle.
   */
  public MASTER getMaster() {
    return master;
  }

  /**
   * Return the client handle of the slave Daemons.<br/>
   * 
   * @return map of host to slave daemon clients.
   */
  public Map<String, SLAVE> getSlaves() {
    return slaves;
  }

  /**
   * Checks if the master slave cluster is ready for testing. <br/>
   * Algorithm for checking is as follows : <br/>
   * <ul>
   * <li> Wait for Daemon to come up </li>
   * <li> Check if daemon is ready </li>
   * <li> If one of the daemon is not ready, return false </li>
   * </ul> 
   * 
   * @return true if whole cluster is ready.
   * 
   * @throws IOException
   */
  public boolean isReady() throws IOException {
    LOG.info("Check if master is up and running");
    waitForDaemon(master);
    if (!master.isReady()) {
      return false;
    }
    LOG.info("Check if slaves are up and running");
    for (SLAVE slave : slaves.values()) {
      waitForDaemon(slave);
      if (!slave.isReady()) {
        return false;
      }
    }
    return true;
  }

  private void waitForDaemon(AbstractDaemonClient d) {
    while(true) {
      try {
        LOG.info("Waiting for daemon in host to come up : " + d.getHostName());
        d.connect();
        break;
      } catch (IOException e) {
        try {
          Thread.sleep(10000);
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  /**
   * Start the master slave cluster. <br/>
   * The startup behavior is controlled by the {@code WAITFORMASTERKEY}.
   * <ul>
   * <li>If{@code WAITFORMASTERKEY} is set to true then start up of slaves are
   * done after master daemon comes up and is ready to accept the RPC connection
   * </li>
   * <li>Else the daemons are started up sequentially without waiting for master
   * daemon to be ready.</li>
   * </ul>
   * 
   * @throws IOException
   */
  public void start() throws IOException {
    if (waitformaster) {
      this.master.start();
      waitForMaster();
      startSlaves();
    } else {
      clusterManager.start();
    }
  }

  private void waitForMaster() throws IOException {
    waitForDaemon(master);
    while (!master.isReady()) {
      try {
        LOG.info("Waiting for master daemon to be ready to accept " +
        		"RPC connection");
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private void startSlaves() throws IOException {
    Map<String, RemoteProcess> slaves = clusterManager.getSlaves();
    for (RemoteProcess s : slaves.values()) {
      s.start();
    }
  }

  /**
   * Stops the master slave cluster.<br/>
   * 
   * @throws IOException
   */
  public void stop() throws IOException {
    clusterManager.stop();
  }

  /**
   * Connect to master and slave RPC ports.
   * @throws IOException
   */
  public void connect() throws IOException {
    LOG.info("Connecting to the cluster..." + getClass().getName());
    master.connect();
    for (SLAVE slave : slaves.values()) {
      slave.connect();
    }
  }

  /**
   * Disconnect to master and slave RPC ports.
   * @throws IOException
   */
  public void disconnect() throws IOException {
    LOG.info("Disconnecting to the cluster..." + 
        getClass().getName());
    master.disconnect();
    for (SLAVE slave : slaves.values()) {
      slave.disconnect();
    }
    LOG.info("Disconnected!!");
  }

  /**
   * Ping all the daemons of the cluster.
   * @throws IOException
   */
  public void ping() throws IOException {
    MASTER master = getMaster();
    LOG.info("Master is :" + master.getHostName() + " pinging ...");
    master.ping();
    Collection<SLAVE> slaves = getSlaves().values();
    for (SLAVE slave : slaves) {
      LOG.info("Slave is : " + slave.getHostName() + " pinging....");
      slave.ping();
    }
  }

  /**
   * Connect to the cluster and ensure that it is clean to run tests.
   * @throws Exception
   */
  public void setUp() throws Exception {
    while (!isReady()) {
      Thread.sleep(1000);
    }
    connect();
    ping();
    clearAllControlActions();
    ensureClean();
  }

  /**
   * Ensure that the cluster is clean to run tests.
   * @throws IOException
   */
  public void ensureClean() throws IOException {
  }

  /**
   * Clears all the pending control actions in the cluster.<br/>
   * @throws IOException
   */
  public void clearAllControlActions() throws IOException {
    master.getProxy().clearActions();
    for (SLAVE slave : getSlaves().values()) {
      slave.getProxy().clearActions();
    }
  }
  /**
   * Ensure that cluster is clean. Disconnect from the RPC ports of the daemons.
   * @throws IOException
   */
  public void tearDown() throws IOException {
    ensureClean();
    clearAllControlActions();
    disconnect();
  }
}
