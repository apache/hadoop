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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.impl.zk.BindingInformation;
import org.apache.hadoop.registry.client.impl.zk.RegistryBindingSource;
import org.apache.hadoop.registry.client.impl.zk.RegistryInternalConstants;
import org.apache.hadoop.registry.client.impl.zk.RegistrySecurity;
import org.apache.hadoop.registry.client.impl.zk.ZookeeperConfigOptions;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

/**
 * This is a small, localhost Zookeeper service instance that is contained
 * in a YARN service...it's been derived from Apache Twill.
 * <p>
 * It implements {@link RegistryBindingSource} and provides binding information,
 * <i>once started</i>. Until {@link #start()} is called, the hostname and
 * port may be undefined. Accordingly, the service raises an exception in this
 * condition.
 * <p>
 * If you wish to chain together a registry service with this one under
 * the same {@code CompositeService}, this service must be added
 * as a child first.
 * <p>
 * It also sets the configuration parameter
 * {@link RegistryConstants#KEY_REGISTRY_ZK_QUORUM}
 * to its connection string. Any code with access to the service configuration
 * can view it.
 */
@InterfaceStability.Evolving
public class MicroZookeeperService
    extends AbstractService
    implements RegistryBindingSource, RegistryConstants,
    ZookeeperConfigOptions,
    MicroZookeeperServiceKeys{


  private static final Logger
      LOG = LoggerFactory.getLogger(MicroZookeeperService.class);

  private File instanceDir;
  private File dataDir;
  private int tickTime;
  private int port;
  private String host;
  private boolean secureServer;

  private ServerCnxnFactory factory;
  private BindingInformation binding;
  private File confDir;
  private StringBuilder diagnostics = new StringBuilder();

  /**
   * Create an instance
   * @param name service name
   */
  public MicroZookeeperService(String name) {
    super(name);
  }

  /**
   * Get the connection string.
   * @return the string
   * @throws IllegalStateException if the connection is not yet valid
   */
  public String getConnectionString() {
    Preconditions.checkState(factory != null, "service not started");
    InetSocketAddress addr = factory.getLocalAddress();
    return String.format("%s:%d", addr.getHostName(), addr.getPort());
  }

  /**
   * Get the connection address
   * @return the connection as an address
   * @throws IllegalStateException if the connection is not yet valid
   */
  public InetSocketAddress getConnectionAddress() {
    Preconditions.checkState(factory != null, "service not started");
    return factory.getLocalAddress();
  }

  /**
   * Create an inet socket addr from the local host + port number
   * @param port port to use
   * @return a (hostname, port) pair
   * @throws UnknownHostException if the server cannot resolve the host
   */
  private InetSocketAddress getAddress(int port) throws UnknownHostException {
    return new InetSocketAddress(host, port <= 0 ? getRandomAvailablePort() : port);
  }

  /**
   * Initialize the service, including choosing a path for the data
   * @param conf configuration
   * @throws Exception
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    port = conf.getInt(KEY_ZKSERVICE_PORT, 0);
    tickTime = conf.getInt(KEY_ZKSERVICE_TICK_TIME,
        ZooKeeperServer.DEFAULT_TICK_TIME);
    String instancedirname = conf.getTrimmed(
        KEY_ZKSERVICE_DIR, "");
    host = conf.getTrimmed(KEY_ZKSERVICE_HOST, DEFAULT_ZKSERVICE_HOST);
    if (instancedirname.isEmpty()) {
      File testdir = new File(System.getProperty("test.dir", "target"));
      instanceDir = new File(testdir, "zookeeper" + getName());
    } else {
      instanceDir = new File(instancedirname);
      FileUtil.fullyDelete(instanceDir);
    }
    LOG.debug("Instance directory is {}", instanceDir);
    mkdirStrict(instanceDir);
    dataDir = new File(instanceDir, "data");
    confDir = new File(instanceDir, "conf");
    mkdirStrict(dataDir);
    mkdirStrict(confDir);
    super.serviceInit(conf);
  }

  /**
   * Create a directory, ignoring if the dir is already there,
   * and failing if a file or something else was at the end of that
   * path
   * @param dir dir to guarantee the existence of
   * @throws IOException IO problems, or path exists but is not a dir
   */
  private void mkdirStrict(File dir) throws IOException {
    if (!dir.mkdirs()) {
      if (!dir.isDirectory()) {
        throw new IOException("Failed to mkdir " + dir);
      }
    }
  }

  /**
   * Append a formatted string to the diagnostics.
   * <p>
   * A newline is appended afterwards.
   * @param text text including any format commands
   * @param args arguments for the forma operation.
   */
  protected void addDiagnostics(String text, Object ... args) {
    diagnostics.append(String.format(text, args)).append('\n');
  }

  /**
   * Get the diagnostics info
   * @return the diagnostics string built up
   */
  public String getDiagnostics() {
    return diagnostics.toString();
  }

  /**
   * set up security. this must be done prior to creating
   * the ZK instance, as it sets up JAAS if that has not been done already.
   *
   * @return true if the cluster has security enabled.
   */
  public boolean setupSecurity() throws IOException {
    Configuration conf = getConfig();
    String jaasContext = conf.getTrimmed(KEY_REGISTRY_ZKSERVICE_JAAS_CONTEXT);
    secureServer = StringUtils.isNotEmpty(jaasContext);
    if (secureServer) {
      RegistrySecurity.validateContext(jaasContext);
      RegistrySecurity.bindZKToServerJAASContext(jaasContext);
      // policy on failed auth
      System.setProperty(PROP_ZK_ALLOW_FAILED_SASL_CLIENTS,
        conf.get(KEY_ZKSERVICE_ALLOW_FAILED_SASL_CLIENTS,
            "true"));

      //needed so that you can use sasl: strings in the registry
      System.setProperty(RegistryInternalConstants.ZOOKEEPER_AUTH_PROVIDER +".1",
          RegistryInternalConstants.SASLAUTHENTICATION_PROVIDER);
      String serverContext =
          System.getProperty(PROP_ZK_SERVER_SASL_CONTEXT);
      addDiagnostics("Server JAAS context s = %s", serverContext);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Startup: start ZK. It is only after this that
   * the binding information is valid.
   * @throws Exception
   */
  @Override
  protected void serviceStart() throws Exception {

    setupSecurity();

    FileTxnSnapLog ftxn = new FileTxnSnapLog(dataDir, dataDir);
    ZooKeeperServer zkServer = new ZooKeeperServer(ftxn, tickTime);

    LOG.info("Starting Local Zookeeper service");
    factory = ServerCnxnFactory.createFactory();
    factory.configure(getAddress(port), -1);
    factory.startup(zkServer);

    String connectString = getConnectionString();
    LOG.info("In memory ZK started at {}\n", connectString);

    if (LOG.isDebugEnabled()) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      zkServer.dumpConf(pw);
      pw.flush();
      LOG.debug("ZooKeeper config:\n" + sw.toString());
    }
    binding = new BindingInformation();
    binding.ensembleProvider = new FixedEnsembleProvider(connectString);
    binding.description =
        getName() + " reachable at \"" + connectString + "\"";

    addDiagnostics(binding.description);
    // finally: set the binding information in the config
    getConfig().set(KEY_REGISTRY_ZK_QUORUM, connectString);
  }

  /**
   * When the service is stopped, it deletes the data directory
   * and its contents
   * @throws Exception
   */
  @Override
  protected void serviceStop() throws Exception {
    if (factory != null) {
      factory.shutdown();
      factory = null;
    }
    if (dataDir != null) {
      FileUtil.fullyDelete(dataDir);
    }
  }

  @Override
  public BindingInformation supplyBindingInformation() {
    Preconditions.checkNotNull(binding,
        "Service is not started: binding information undefined");
    return binding;
  }

  /**
   * Returns with a random open port can be used to set as server port for ZooKeeper.
   * @return a random open port or 0 (in case of error)
   */
  private int getRandomAvailablePort() {
      port = 0;
      try {
        final ServerSocket s = new ServerSocket(0);
        port = s.getLocalPort();
        s.close();
      } catch (IOException e) {
        LOG.warn("ERROR during selecting random port for ZooKeeper server to bind." , e);
      }
      return port;
  }
}
