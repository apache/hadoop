package org.apache.hadoop.test.system.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/**
 * The concrete class which implements the start up and shut down based routines
 * based on the hadoop-daemon.sh. <br/>
 * 
 * Class requires two keys to be present in the Configuration objects passed to
 * it. Look at <code>CONF_HADOOPHOME</code> and
 * <code>CONF_HADOOPCONFDIR</code> for the names of the
 * configuration keys.
 * 
 * Following will be the format which the final command execution would look : 
 * <br/>
 * <code>
 *  ssh master-host 'hadoop-home/bin/hadoop-daemon.sh --script scriptName 
 *  --config HADOOP_CONF_DIR (start|stop) masterCommand'
 * </code>
 */
public class HadoopDaemonRemoteCluster implements ClusterProcessManager {

  private static final Log LOG = LogFactory
      .getLog(HadoopDaemonRemoteCluster.class.getName());

  /**
   * Key used to configure the HADOOP_HOME to be used by the
   * HadoopDaemonRemoteCluster.
   */
  public final static String CONF_HADOOPHOME = "test.system.hdrc.hadoophome";
  /**
   * Key used to configure the HADOOP_CONF_DIR to be used by the
   * HadoopDaemonRemoteCluster.
   */
  public final static String CONF_HADOOPCONFDIR = 
    "test.system.hdrc.hadoopconfdir";

  public final static String CONF_DEPLOYED_HADOOPCONFDIR =
    "test.system.hdrc.deployed.hadoopconfdir";

  private String hadoopHome;
  private String hadoopConfDir;
  private String deployed_hadoopConfDir;
  private String masterCommand;
  private String slaveCommand;

  private RemoteProcess master;
  private Map<String, RemoteProcess> slaves;

  @Override
  public void init(ClusterType t, Configuration conf) throws Exception {
    /*
     * Initialization strategy of the HadoopDaemonRemoteCluster is three staged
     * process: 1. Populate script names based on the type of passed cluster. 2.
     * Populate the required directories. 3. Populate the master and slaves.
     */
    populateScriptNames(t);
    populateDirectories(conf);
    this.slaves = new HashMap<String, RemoteProcess>();
    populateDaemons(deployed_hadoopConfDir);
  }

  /**
   * Method to populate the required master and slave commands which are used to
   * manage the cluster.<br/>
   * 
   * @param t
   *          type of cluster to be initialized.
   * 
   * @throws UnsupportedOperationException
   *           if the passed cluster type is not MAPRED or HDFS
   */
  private void populateScriptNames(ClusterType t) {
    switch (t) {
    case MAPRED:
      masterCommand = "jobtracker";
      slaveCommand = "tasktracker";
      if (LOG.isDebugEnabled()) {
        LOG.debug("Created mapred hadoop daemon remote cluster manager with "
            + "scriptName: mapred, masterCommand: jobtracker, "
            + "slaveCommand: tasktracker");
      }
      break;
    case HDFS:
      masterCommand = "namenode";
      slaveCommand = "datanode";
      if (LOG.isDebugEnabled()) {
        LOG.debug("Created hdfs hadoop daemon remote cluster manager with "
            + "scriptName: hdfs, masterCommand: namenode, "
            + "slaveCommand: datanode");
      }
      break;
    default:
      LOG.error("Cluster type :" + t
          + "is not supported currently by HadoopDaemonRemoteCluster");
      throw new UnsupportedOperationException(
          "The specified cluster type is not supported by the " +
          "HadoopDaemonRemoteCluster");
    }
  }

  /**
   * Method to populate the hadoop home and hadoop configuration directories.
   * 
   * @param conf
   *          Configuration object containing values for
   *          TEST_SYSTEM_HADOOPHOME_CONF_KEY and
   *          TEST_SYSTEM_HADOOPCONFDIR_CONF_KEY
   * 
   * @throws IllegalArgumentException
   *           if the configuration or system property set does not contain
   *           values for the required keys.
   */
  private void populateDirectories(Configuration conf) {
    hadoopHome = conf.get(CONF_HADOOPHOME, System
        .getProperty(CONF_HADOOPHOME));
    hadoopConfDir = conf.get(CONF_HADOOPCONFDIR, System
        .getProperty(CONF_HADOOPCONFDIR));

    deployed_hadoopConfDir = conf.get(CONF_DEPLOYED_HADOOPCONFDIR,
      System.getProperty(CONF_DEPLOYED_HADOOPCONFDIR));
    if (deployed_hadoopConfDir == null || deployed_hadoopConfDir.isEmpty()) {
      deployed_hadoopConfDir = hadoopConfDir;
    }

    if (hadoopHome == null || hadoopConfDir == null || hadoopHome.isEmpty()
        || hadoopConfDir.isEmpty()) {
      LOG.error("No configuration "
          + "for the HADOOP_HOME and HADOOP_CONF_DIR passed");
      throw new IllegalArgumentException(
          "No Configuration passed for hadoop home " +
          "and hadoop conf directories");
    }

  }

  @Override
  public RemoteProcess getMaster() {
    return master;
  }

  @Override
  public Map<String, RemoteProcess> getSlaves() {
    return slaves;
  }

  @Override
  public void start() throws IOException {
    // start master first.
    master.start();
    for (RemoteProcess slave : slaves.values()) {
      slave.start();
    }
  }

  @Override
  public void stop() throws IOException {
    master.kill();
    for (RemoteProcess slave : slaves.values()) {
      slave.kill();
    }
  }

  private void populateDaemons(String confLocation) throws IOException {
    File mastersFile = new File(confLocation, "masters");
    File slavesFile = new File(confLocation, "slaves");
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(mastersFile));
      String masterHost = null;
      masterHost = reader.readLine();
      if (masterHost != null && !masterHost.trim().isEmpty()) {
        master = new ScriptDaemon(masterCommand, masterHost);
      }
    } finally {
      try {
        reader.close();
      } catch (Exception e) {
        LOG.error("Can't read masters file from " + confLocation);
      }

    }
    try {
      reader = new BufferedReader(new FileReader(slavesFile));
      String slaveHost = null;
      while ((slaveHost = reader.readLine()) != null) {
        RemoteProcess slave = new ScriptDaemon(slaveCommand, slaveHost);
        slaves.put(slaveHost, slave);
      }
    } finally {
      try {
        reader.close();
      } catch (Exception e) {
        LOG.error("Can't read slaves file from " + confLocation);
      }
    }
  }

  /**
   * The core daemon class which actually implements the remote process
   * management of actual daemon processes in the cluster.
   * 
   */
  class ScriptDaemon implements RemoteProcess {

    private static final String STOP_COMMAND = "stop";
    private static final String START_COMMAND = "start";
    private static final String SCRIPT_NAME = "hadoop-daemon.sh";
    private final String daemonName;
    private final String hostName;

    public ScriptDaemon(String daemonName, String hostName) {
      this.daemonName = daemonName;
      this.hostName = hostName;
    }

    @Override
    public String getHostName() {
      return hostName;
    }

    private ShellCommandExecutor buildCommandExecutor(String command) {
      String[] commandArgs = getCommand(command);
      File binDir = getBinDir();
      HashMap<String, String> env = new HashMap<String, String>();
      env.put("HADOOP_CONF_DIR", hadoopConfDir);
      ShellCommandExecutor executor = new ShellCommandExecutor(commandArgs,
          binDir, env);
      LOG.info(executor.toString());
      return executor;
    }

    private File getBinDir() {
      File binDir = new File(hadoopHome, "bin");
      return binDir;
    }

    private String[] getCommand(String command) {
      ArrayList<String> cmdArgs = new ArrayList<String>();
      File binDir = getBinDir();
      cmdArgs.add("ssh");
      cmdArgs.add(hostName);
      cmdArgs.add(binDir.getAbsolutePath() + File.separator + SCRIPT_NAME);
      cmdArgs.add("--config");
      cmdArgs.add(hadoopConfDir);
      // XXX Twenty internal version does not support --script option.
      cmdArgs.add(command);
      cmdArgs.add(daemonName);
      return (String[]) cmdArgs.toArray(new String[cmdArgs.size()]);
    }

    @Override
    public void kill() throws IOException {
      buildCommandExecutor(STOP_COMMAND).execute();
    }

    @Override
    public void start() throws IOException {
      buildCommandExecutor(START_COMMAND).execute();
    }
  }

}
