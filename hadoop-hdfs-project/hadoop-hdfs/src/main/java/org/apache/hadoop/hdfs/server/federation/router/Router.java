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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.hdfs.server.federation.router.FederationUtil.newActiveNamenodeResolver;
import static org.apache.hadoop.hdfs.server.federation.router.FederationUtil.newFileSubclusterResolver;
import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;

/**
 * Router that provides a unified view of multiple federated HDFS clusters. It
 * has two main roles: (1) federated interface and (2) NameNode heartbeat.
 * <p>
 * For the federated interface, the Router receives a client request, checks the
 * State Store for the correct subcluster, and forwards the request to the
 * active Namenode of that subcluster. The reply from the Namenode then flows in
 * the opposite direction. The Routers are stateless and can be behind a load
 * balancer. HDFS clients connect to the router using the same interfaces as are
 * used to communicate with a namenode, namely the ClientProtocol RPC interface
 * and the WebHdfs HTTP interface exposed by the router. {@link RouterRpcServer}
 * {@link RouterHttpServer}
 * <p>
 * For NameNode heartbeat, the Router periodically checks the state of a
 * NameNode (usually on the same server) and reports their high availability
 * (HA) state and load/space status to the State Store. Note that this is an
 * optional role as a Router can be independent of any subcluster.
 * {@link StateStoreService} {@link NamenodeHeartbeatService}
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Router extends CompositeService {

  private static final Log LOG = LogFactory.getLog(Router.class);


  /** Configuration for the Router. */
  private Configuration conf;

  /** Router address/identifier. */
  private String routerId;

  /** RPC interface to the client. */
  private RouterRpcServer rpcServer;

  /** Interface with the State Store. */
  private StateStoreService stateStore;

  /** Interface to map global name space to HDFS subcluster name spaces. */
  private FileSubclusterResolver subclusterResolver;

  /** Interface to identify the active NN for a nameservice or blockpool ID. */
  private ActiveNamenodeResolver namenodeResolver;


  /** Usage string for help message. */
  private static final String USAGE = "Usage: java Router";

  /** Priority of the Router shutdown hook. */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;


  /////////////////////////////////////////////////////////
  // Constructor
  /////////////////////////////////////////////////////////

  public Router() {
    super(Router.class.getName());
  }

  /////////////////////////////////////////////////////////
  // Service management
  /////////////////////////////////////////////////////////

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {
    this.conf = configuration;

    // TODO Interface to the State Store
    this.stateStore = null;

    // Resolver to track active NNs
    this.namenodeResolver = newActiveNamenodeResolver(
        this.conf, this.stateStore);
    if (this.namenodeResolver == null) {
      throw new IOException("Cannot find namenode resolver.");
    }

    // Lookup interface to map between the global and subcluster name spaces
    this.subclusterResolver = newFileSubclusterResolver(
        this.conf, this.stateStore);
    if (this.subclusterResolver == null) {
      throw new IOException("Cannot find subcluster resolver");
    }

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {

    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {

    super.serviceStop();
  }

  /**
   * Shutdown the router.
   */
  public void shutDown() {
    new Thread() {
      @Override
      public void run() {
        Router.this.stop();
      }
    }.start();
  }

  /**
   * Main run loop for the router.
   *
   * @param argv parameters.
   */
  public static void main(String[] argv) {
    if (DFSUtil.parseHelpArgument(argv, Router.USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      StringUtils.startupShutdownMessage(Router.class, argv, LOG);

      Router router = new Router();

      ShutdownHookManager.get().addShutdownHook(
          new CompositeServiceShutdownHook(router), SHUTDOWN_HOOK_PRIORITY);

      Configuration conf = new HdfsConfiguration();
      router.init(conf);
      router.start();
    } catch (Throwable e) {
      LOG.error("Failed to start router.", e);
      terminate(1, e);
    }
  }

  /////////////////////////////////////////////////////////
  // RPC Server
  /////////////////////////////////////////////////////////

  /**
   * Create a new Router RPC server to proxy ClientProtocol requests.
   *
   * @return RouterRpcServer
   * @throws IOException If the router RPC server was not started.
   */
  protected RouterRpcServer createRpcServer() throws IOException {
    return new RouterRpcServer(this.conf, this, this.getNamenodeResolver(),
        this.getSubclusterResolver());
  }

  /**
   * Get the Router RPC server.
   *
   * @return Router RPC server.
   */
  public RouterRpcServer getRpcServer() {
    return this.rpcServer;
  }

  /////////////////////////////////////////////////////////
  // Submodule getters
  /////////////////////////////////////////////////////////

  /**
   * Get the State Store service.
   *
   * @return State Store service.
   */
  public StateStoreService getStateStore() {
    return this.stateStore;
  }

  /**
   * Get the subcluster resolver for files.
   *
   * @return Subcluster resolver for files.
   */
  public FileSubclusterResolver getSubclusterResolver() {
    return this.subclusterResolver;
  }

  /**
   * Get the namenode resolver for a subcluster.
   *
   * @return The namenode resolver for a subcluster.
   */
  public ActiveNamenodeResolver getNamenodeResolver() {
    return this.namenodeResolver;
  }

  /////////////////////////////////////////////////////////
  // Router info
  /////////////////////////////////////////////////////////

  /**
   * Unique ID for the router, typically the hostname:port string for the
   * router's RPC server. This ID may be null on router startup before the RPC
   * server has bound to a port.
   *
   * @return Router identifier.
   */
  public String getRouterId() {
    return this.routerId;
  }

  /**
   * Sets a unique ID for this router.
   *
   * @param router Identifier of the Router.
   */
  public void setRouterId(String id) {
    this.routerId = id;
    if (this.stateStore != null) {
      this.stateStore.setIdentifier(this.routerId);
    }
    if (this.namenodeResolver != null) {
      this.namenodeResolver.setRouterId(this.routerId);
    }
  }
}
