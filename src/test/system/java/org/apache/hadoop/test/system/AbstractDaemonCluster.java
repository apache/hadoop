package org.apache.hadoop.test.system;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.system.process.ClusterProcessManager;
import org.apache.hadoop.test.system.process.RemoteProcess;

/**
 * Abstract class which represent the cluster having multiple daemons.
 */
@SuppressWarnings("unchecked")
public abstract class AbstractDaemonCluster {

  private static final Log LOG = LogFactory.getLog(AbstractDaemonCluster.class);

  private Configuration conf;
  protected ClusterProcessManager clusterManager;
  private Map<Enum<?>, List<AbstractDaemonClient>> daemons = 
    new LinkedHashMap<Enum<?>, List<AbstractDaemonClient>>();
  
  /**
   * Constructor to create a cluster client.<br/>
   * 
   * @param conf
   *          Configuration to be used while constructing the cluster.
   * @param rcluster
   *          process manger instance to be used for managing the daemons.
   * 
   * @throws IOException
   */
  public AbstractDaemonCluster(Configuration conf,
      ClusterProcessManager rcluster) throws IOException {
    this.conf = conf;
    this.clusterManager = rcluster;
    createAllClients();
  }

  protected void createAllClients() throws IOException {
    for (RemoteProcess p : clusterManager.getAllProcesses()) {
      List<AbstractDaemonClient> dms = daemons.get(p.getRole());
      if (dms == null) {
        dms = new ArrayList<AbstractDaemonClient>();
        daemons.put(p.getRole(), dms);
      }
      dms.add(createClient(p));
    }
  }
  
  /**
   * Method to create the daemon client.<br/>
   * 
   * @param remoteprocess
   *          to manage the daemon.
   * @return instance of the daemon client
   * 
   * @throws IOException
   */
  protected abstract AbstractDaemonClient<DaemonProtocol> 
    createClient(RemoteProcess process) throws IOException;

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
   *

  /**
   * Return the client handle of all the Daemons.<br/>
   * 
   * @return map of role to daemon clients' list.
   */
  public Map<Enum<?>, List<AbstractDaemonClient>> getDaemons() {
    return daemons;
  }

  /**
   * Checks if the cluster is ready for testing. <br/>
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
    for (List<AbstractDaemonClient> set : daemons.values()) {
      for (AbstractDaemonClient daemon : set) {
        waitForDaemon(daemon);
        if (!daemon.isReady()) {
          return false;
        }
      }
    }
    return true;
  }

  protected void waitForDaemon(AbstractDaemonClient d) {
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
   * Starts the cluster daemons.
   * @throws IOException
   */
  public void start() throws IOException {
    clusterManager.start();
  }

  /**
   * Stops the cluster daemons.
   * @throws IOException
   */
  public void stop() throws IOException {
    clusterManager.stop();
  }

  /**
   * Connect to daemon RPC ports.
   * @throws IOException
   */
  public void connect() throws IOException {
    for (List<AbstractDaemonClient> set : daemons.values()) {
      for (AbstractDaemonClient daemon : set) {
        daemon.connect();
      }
    }
  }

  /**
   * Disconnect to daemon RPC ports.
   * @throws IOException
   */
  public void disconnect() throws IOException {
    for (List<AbstractDaemonClient> set : daemons.values()) {
      for (AbstractDaemonClient daemon : set) {
        daemon.disconnect();
      }
    }
  }

  /**
   * Ping all the daemons of the cluster.
   * @throws IOException
   */
  public void ping() throws IOException {
    for (List<AbstractDaemonClient> set : daemons.values()) {
      for (AbstractDaemonClient daemon : set) {
        LOG.info("Daemon is : " + daemon.getHostName() + " pinging....");
        daemon.ping();
      }
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
    populateExceptionCounts();
  }

  public void clearAllControlActions() throws IOException {
    for (List<AbstractDaemonClient> set : daemons.values()) {
      for (AbstractDaemonClient daemon : set) {
        LOG.info("Daemon is : " + daemon.getHostName() + " pinging....");
        daemon.getProxy().clearActions();
      }
    }
  }

  /**
   * Ensure that the cluster is clean to run tests.
   * @throws IOException
   */
  public void ensureClean() throws IOException {
  }

  /**
   * Ensure that cluster is clean. Disconnect from the RPC ports of the daemons.
   * @throws IOException
   */
  public void tearDown() throws IOException {
    ensureClean();
    clearAllControlActions();
    assertNoExceptionMessages();
    disconnect();
  }

  /**
   * Populate the exception counts in all the daemons so that it can be checked when 
   * the testcase has finished running.<br/>
   * @throws IOException
   */
  protected void populateExceptionCounts() throws IOException {
    for(List<AbstractDaemonClient> lst : daemons.values()) {
      for(AbstractDaemonClient d : lst) {
        d.populateExceptionCount();
      }
    }
  }

  /**
   * Assert no exception has been thrown during the sequence of the actions.
   * <br/>
   * @throws IOException
   */
  protected void assertNoExceptionMessages() throws IOException {
    for(List<AbstractDaemonClient> lst : daemons.values()) {
      for(AbstractDaemonClient d : lst) {
        d.assertNoExceptionsOccurred();
      }
    }
  }
}

