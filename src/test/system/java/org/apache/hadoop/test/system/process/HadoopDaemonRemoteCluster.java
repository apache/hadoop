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

package org.apache.hadoop.test.system.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
 *  ssh host 'hadoop-home/bin/hadoop-daemon.sh --script scriptName 
 *  --config HADOOP_CONF_DIR (start|stop) command'
 * </code>
 */
public abstract class HadoopDaemonRemoteCluster 
    implements ClusterProcessManager {

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
  private final Set<Enum<?>> roles;

  private final List<HadoopDaemonInfo> daemonInfos;
  private List<RemoteProcess> processes;

  public static class HadoopDaemonInfo {
    public final String cmd;
    public final Enum<?> role;
    public final String hostFile;
    public HadoopDaemonInfo(String cmd, Enum<?> role, String hostFile) {
      super();
      this.cmd = cmd;
      this.role = role;
      this.hostFile = hostFile;
    }
  }

  public HadoopDaemonRemoteCluster(List<HadoopDaemonInfo> daemonInfos) {
    this.daemonInfos = daemonInfos;
    this.roles = new HashSet<Enum<?>>();
    for (HadoopDaemonInfo info : daemonInfos) {
      this.roles.add(info.role);
    }
  }

  @Override
  public void init(Configuration conf) throws IOException {
    populateDirectories(conf);
    this.processes = new ArrayList<RemoteProcess>();
    populateDaemons(deployed_hadoopConfDir);
  }

  @Override
  public List<RemoteProcess> getAllProcesses() {
    return processes;
  }

  @Override
  public Set<Enum<?>> getRoles() {
    return roles;
  }

  /**
   * Method to populate the hadoop home and hadoop configuration directories.
   * 
   * @param conf
   *          Configuration object containing values for
   *          CONF_HADOOPHOME and
   *          CONF_HADOOPCONFDIR
   * 
   * @throws IllegalArgumentException
   *           if the configuration or system property set does not contain
   *           values for the required keys.
   */
  protected void populateDirectories(Configuration conf) {
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
  public void start() throws IOException {
    for (RemoteProcess process : processes) {
      process.start();
    }
  }

  @Override
  public void stop() throws IOException {
    for (RemoteProcess process : processes) {
      process.kill();
    }
  }

  protected void populateDaemon(String confLocation, 
      HadoopDaemonInfo info) throws IOException {
    File hostFile = new File(confLocation, info.hostFile);
    BufferedReader reader = null;
    reader = new BufferedReader(new FileReader(hostFile));
    String host = null;
    try {
      boolean foundAtLeastOne = false;
      while ((host = reader.readLine()) != null) {
        if (host.trim().isEmpty()) {
          throw new IllegalArgumentException(
          "Hostname could not be found in file " + info.hostFile);
        }
        InetAddress addr = InetAddress.getByName(host);
        RemoteProcess process = new ScriptDaemon(info.cmd, 
            addr.getCanonicalHostName(), info.role);
        processes.add(process);
        foundAtLeastOne = true;
      }
      if (!foundAtLeastOne) {
        throw new IllegalArgumentException("Alteast one hostname " +
          "is required to be present in file - " + info.hostFile);
      }
    } finally {
      try {
        reader.close();
      } catch (Exception e) {
        LOG.warn("Could not close reader");
      }
    }
  }

  protected void populateDaemons(String confLocation) throws IOException {
   for (HadoopDaemonInfo info : daemonInfos) {
     populateDaemon(confLocation, info);
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
    private final Enum<?> role;

    public ScriptDaemon(String daemonName, String hostName, Enum<?> role) {
      this.daemonName = daemonName;
      this.hostName = hostName;
      this.role = role;
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

    @Override
    public Enum<?> getRole() {
      return role;
    }
  }
}
