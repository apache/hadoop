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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_KERBEROS_PRINCIPAL_HOSTNAME_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_KEYTAB_FILE_KEY;

import static org.apache.hadoop.hdfs.server.federation.router.FederationUtil.newActiveNamenodeResolver;
import static org.apache.hadoop.hdfs.server.federation.router.FederationUtil.newFileSubclusterResolver;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.TokenVerifier;
import org.apache.hadoop.hdfs.server.federation.metrics.RBFMetrics;
import org.apache.hadoop.hdfs.server.federation.metrics.NamenodeBeanMetrics;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.store.MountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.RouterStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.net.DomainNameResolver;
import org.apache.hadoop.net.DomainNameResolverFactory;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

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
public class Router extends CompositeService implements
    TokenVerifier<DelegationTokenIdentifier> {

  private static final Logger LOG = LoggerFactory.getLogger(Router.class);


  /** Configuration for the Router. */
  private Configuration conf;

  /** Router address/identifier. */
  private String routerId;

  /** RPC interface to the client. */
  private RouterRpcServer rpcServer;
  private InetSocketAddress rpcAddress;

  /** RPC interface for the admin. */
  private RouterAdminServer adminServer;
  private InetSocketAddress adminAddress;

  /** HTTP interface and web application. */
  private RouterHttpServer httpServer;

  /** Interface with the State Store. */
  private StateStoreService stateStore;

  /** Interface to map global name space to HDFS subcluster name spaces. */
  private FileSubclusterResolver subclusterResolver;

  /** Interface to identify the active NN for a nameservice or blockpool ID. */
  private ActiveNamenodeResolver namenodeResolver;
  /** Updates the namenode status in the namenode resolver. */
  private Collection<NamenodeHeartbeatService> namenodeHeartbeatServices;

  /** Router metrics. */
  private RouterMetricsService metrics;

  /** JVM pauses (GC and others). */
  private JvmPauseMonitor pauseMonitor;

  /** Quota usage update service. */
  private RouterQuotaUpdateService quotaUpdateService;
  /** Quota cache manager. */
  private RouterQuotaManager quotaManager;

  /** Manages the current state of the router. */
  private RouterStore routerStateManager;
  /** Heartbeat our run status to the router state manager. */
  private RouterHeartbeatService routerHeartbeatService;
  /** Enter/exit safemode. */
  private RouterSafemodeService safemodeService;

  /** The start time of the namesystem. */
  private final long startTime = Time.now();

  /** State of the Router. */
  private RouterServiceState state = RouterServiceState.UNINITIALIZED;


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
    updateRouterState(RouterServiceState.INITIALIZING);

    // Enable the security for the Router
    UserGroupInformation.setConfiguration(conf);
    SecurityUtil.login(conf, DFS_ROUTER_KEYTAB_FILE_KEY,
        DFS_ROUTER_KERBEROS_PRINCIPAL_KEY, getHostName(conf));

    if (conf.getBoolean(
        RBFConfigKeys.DFS_ROUTER_STORE_ENABLE,
        RBFConfigKeys.DFS_ROUTER_STORE_ENABLE_DEFAULT)) {
      // Service that maintains the State Store connection
      this.stateStore = new StateStoreService();
      addService(this.stateStore);
    }

    // Resolver to track active NNs
    this.namenodeResolver = newActiveNamenodeResolver(
        this.conf, this.stateStore);
    if (this.namenodeResolver == null) {
      throw new IOException("Cannot find namenode resolver.");
    }

    // Lookup interface to map between the global and subcluster name spaces
    this.subclusterResolver = newFileSubclusterResolver(this.conf, this);
    if (this.subclusterResolver == null) {
      throw new IOException("Cannot find subcluster resolver");
    }

    if (conf.getBoolean(
        RBFConfigKeys.DFS_ROUTER_RPC_ENABLE,
        RBFConfigKeys.DFS_ROUTER_RPC_ENABLE_DEFAULT)) {
      // Create RPC server
      this.rpcServer = createRpcServer();
      addService(this.rpcServer);
      this.setRpcServerAddress(rpcServer.getRpcAddress());
    }

    checkRouterId();

    if (conf.getBoolean(
        RBFConfigKeys.DFS_ROUTER_ADMIN_ENABLE,
        RBFConfigKeys.DFS_ROUTER_ADMIN_ENABLE_DEFAULT)) {
      // Create admin server
      this.adminServer = createAdminServer();
      addService(this.adminServer);
    }

    if (conf.getBoolean(
        RBFConfigKeys.DFS_ROUTER_HTTP_ENABLE,
        RBFConfigKeys.DFS_ROUTER_HTTP_ENABLE_DEFAULT)) {
      // Create HTTP server
      this.httpServer = createHttpServer();
      addService(this.httpServer);
    }

    boolean isRouterHeartbeatEnabled = conf.getBoolean(
        RBFConfigKeys.DFS_ROUTER_HEARTBEAT_ENABLE,
        RBFConfigKeys.DFS_ROUTER_HEARTBEAT_ENABLE_DEFAULT);
    boolean isNamenodeHeartbeatEnable = conf.getBoolean(
        RBFConfigKeys.DFS_ROUTER_NAMENODE_HEARTBEAT_ENABLE,
        isRouterHeartbeatEnabled);
    if (isNamenodeHeartbeatEnable) {

      // Create status updater for each monitored Namenode
      this.namenodeHeartbeatServices = createNamenodeHeartbeatServices();
      for (NamenodeHeartbeatService heartbeatService :
          this.namenodeHeartbeatServices) {
        addService(heartbeatService);
      }

      if (this.namenodeHeartbeatServices.isEmpty()) {
        LOG.error("Heartbeat is enabled but there are no namenodes to monitor");
      }
    }
    if (isRouterHeartbeatEnabled) {
      // Periodically update the router state
      this.routerHeartbeatService = new RouterHeartbeatService(this);
      addService(this.routerHeartbeatService);
    }

    // Router metrics system
    if (conf.getBoolean(
        RBFConfigKeys.DFS_ROUTER_METRICS_ENABLE,
        RBFConfigKeys.DFS_ROUTER_METRICS_ENABLE_DEFAULT)) {

      DefaultMetricsSystem.initialize("Router");

      this.metrics = new RouterMetricsService(this);
      addService(this.metrics);

      // JVM pause monitor
      this.pauseMonitor = new JvmPauseMonitor();
      this.pauseMonitor.init(conf);
    }

    // Initial quota relevant service
    if (conf.getBoolean(RBFConfigKeys.DFS_ROUTER_QUOTA_ENABLE,
        RBFConfigKeys.DFS_ROUTER_QUOTA_ENABLED_DEFAULT)) {
      this.quotaManager = new RouterQuotaManager();
      this.quotaUpdateService = new RouterQuotaUpdateService(this);
      addService(this.quotaUpdateService);
    }

    // Safemode service to refuse RPC calls when the router is out of sync
    if (conf.getBoolean(
        RBFConfigKeys.DFS_ROUTER_SAFEMODE_ENABLE,
        RBFConfigKeys.DFS_ROUTER_SAFEMODE_ENABLE_DEFAULT)) {
      // Create safemode monitoring service
      this.safemodeService = new RouterSafemodeService(this);
      addService(this.safemodeService);
    }

    /*
     * Refresh mount table cache immediately after adding, modifying or deleting
     * the mount table entries. If this service is not enabled mount table cache
     * are refreshed periodically by StateStoreCacheUpdateService
     */
    if (conf.getBoolean(RBFConfigKeys.MOUNT_TABLE_CACHE_UPDATE,
        RBFConfigKeys.MOUNT_TABLE_CACHE_UPDATE_DEFAULT)) {
      // There is no use of starting refresh service if state store and admin
      // servers are not enabled
      String disabledDependentServices = getDisabledDependentServices();
      /*
       * disabledDependentServices null means all dependent services are
       * enabled.
       */
      if (disabledDependentServices == null) {

        MountTableRefresherService refreshService =
            new MountTableRefresherService(this);
        addService(refreshService);
        LOG.info("Service {} is enabled.",
            MountTableRefresherService.class.getSimpleName());
      } else {
        LOG.warn(
            "Service {} not enabled: dependent service(s) {} not enabled.",
            MountTableRefresherService.class.getSimpleName(),
            disabledDependentServices);
      }
    }

    super.serviceInit(conf);

    // Set quota manager in mount store to update quota usage in mount table.
    if (stateStore != null) {
      MountTableStore mountstore =
          this.stateStore.getRegisteredRecordStore(MountTableStore.class);
      mountstore.setQuotaManager(this.quotaManager);
    }
  }

  /**
   * Set the router id if not set to prevent RouterHeartbeatService
   * update state store with a null router id.
   */
  private void checkRouterId() {
    if (this.routerId == null) {
      InetSocketAddress confRpcAddress = conf.getSocketAddr(
          RBFConfigKeys.DFS_ROUTER_RPC_BIND_HOST_KEY,
          RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY,
          RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_DEFAULT,
          RBFConfigKeys.DFS_ROUTER_RPC_PORT_DEFAULT);
      setRpcServerAddress(confRpcAddress);
    }
  }

  private String getDisabledDependentServices() {
    if (this.stateStore == null && this.adminServer == null) {
      return StateStoreService.class.getSimpleName() + ","
          + RouterAdminServer.class.getSimpleName();
    } else if (this.stateStore == null) {
      return StateStoreService.class.getSimpleName();
    } else if (this.adminServer == null) {
      return RouterAdminServer.class.getSimpleName();
    }
    return null;
  }

  /**
   * Returns the hostname for this Router. If the hostname is not
   * explicitly configured in the given config, then it is determined.
   *
   * @param config configuration
   * @return the hostname (NB: may not be a FQDN)
   * @throws UnknownHostException if the hostname cannot be determined
   */
  private static String getHostName(Configuration config)
      throws UnknownHostException {
    String name = config.get(DFS_ROUTER_KERBEROS_PRINCIPAL_HOSTNAME_KEY);
    if (name == null) {
      name = InetAddress.getLocalHost().getHostName();
    }
    return name;
  }

  @Override
  protected void serviceStart() throws Exception {

    if (this.safemodeService == null) {
      // Router is running now
      updateRouterState(RouterServiceState.RUNNING);
    }

    if (this.pauseMonitor != null) {
      this.pauseMonitor.start();
      JvmMetrics jvmMetrics = this.metrics.getJvmMetrics();
      if (jvmMetrics != null) {
        jvmMetrics.setPauseMonitor(pauseMonitor);
      }
    }

    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {

    // Update state
    updateRouterState(RouterServiceState.SHUTDOWN);

    // JVM pause monitor
    if (this.pauseMonitor != null) {
      this.pauseMonitor.stop();
    }

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

  /////////////////////////////////////////////////////////
  // RPC Server
  /////////////////////////////////////////////////////////

  /**
   * Create a new Router RPC server to proxy ClientProtocol requests.
   *
   * @return New Router RPC Server.
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

  /**
   * Set the current RPC socket for the router.
   *
   * @param address RPC address.
   */
  protected void setRpcServerAddress(InetSocketAddress address) {
    this.rpcAddress = address;

    // Use the RPC address as our unique router Id
    if (this.rpcAddress != null) {
      try {
        String hostname = InetAddress.getLocalHost().getHostName();
        setRouterId(hostname + ":" + this.rpcAddress.getPort());
      } catch (UnknownHostException ex) {
        LOG.error("Cannot set unique router ID, address not resolvable {}",
            this.rpcAddress);
      }
    }
  }

  /**
   * Get the current RPC socket address for the router.
   *
   * @return InetSocketAddress
   */
  public InetSocketAddress getRpcServerAddress() {
    return this.rpcAddress;
  }

  /////////////////////////////////////////////////////////
  // Admin server
  /////////////////////////////////////////////////////////

  /**
   * Create a new router admin server to handle the router admin interface.
   *
   * @return RouterAdminServer
   * @throws IOException If the admin server was not successfully started.
   */
  protected RouterAdminServer createAdminServer() throws IOException {
    return new RouterAdminServer(this.conf, this);
  }

  /**
   * Set the current Admin socket for the router.
   *
   * @param address Admin RPC address.
   */
  protected void setAdminServerAddress(InetSocketAddress address) {
    this.adminAddress = address;
  }

  /**
   * Get the current Admin socket address for the router.
   *
   * @return InetSocketAddress Admin address.
   */
  public InetSocketAddress getAdminServerAddress() {
    return adminAddress;
  }

  /////////////////////////////////////////////////////////
  // HTTP server
  /////////////////////////////////////////////////////////

  /**
   * Create an HTTP server for this Router.
   *
   * @return HTTP server for this Router.
   */
  protected RouterHttpServer createHttpServer() {
    return new RouterHttpServer(this);
  }

  /**
   * Get the current HTTP socket address for the router.
   *
   * @return InetSocketAddress HTTP address.
   */
  public InetSocketAddress getHttpServerAddress() {
    if (httpServer != null) {
      return httpServer.getHttpAddress();
    }
    return null;
  }

  @Override
  public void verifyToken(DelegationTokenIdentifier tokenId, byte[] password)
      throws IOException {
    getRpcServer().getRouterSecurityManager().verifyToken(tokenId, password);
  }

  /////////////////////////////////////////////////////////
  // Namenode heartbeat monitors
  /////////////////////////////////////////////////////////

  /**
   * Create each of the services that will monitor a Namenode.
   *
   * @return List of heartbeat services.
   */
  protected Collection<NamenodeHeartbeatService>
      createNamenodeHeartbeatServices() {

    Map<String, NamenodeHeartbeatService> ret = new HashMap<>();

    if (conf.getBoolean(
        RBFConfigKeys.DFS_ROUTER_MONITOR_LOCAL_NAMENODE,
        RBFConfigKeys.DFS_ROUTER_MONITOR_LOCAL_NAMENODE_DEFAULT)) {
      // Create a local heartbeat service
      NamenodeHeartbeatService localHeartbeatService =
          createLocalNamenodeHeartbeatService();
      if (localHeartbeatService != null) {
        String nnDesc = localHeartbeatService.getNamenodeDesc();
        ret.put(nnDesc, localHeartbeatService);
      }
    }

    // Create heartbeat services for a list specified by the admin
    Collection<String> namenodes = this.conf.getTrimmedStringCollection(
        RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE);
    for (String namenode : namenodes) {
      String[] namenodeSplit = namenode.split("\\.");
      String nsId = null;
      String nnId = null;
      if (namenodeSplit.length == 2) {
        nsId = namenodeSplit[0];
        nnId = namenodeSplit[1];
      } else if (namenodeSplit.length == 1) {
        nsId = namenode;
      } else {
        LOG.error("Wrong Namenode to monitor: {}", namenode);
      }
      if (nsId != null) {
        String configKeyWithHost =
            RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE_RESOLUTION_ENABLED + "." + nsId;
        boolean resolveNeeded = conf.getBoolean(configKeyWithHost,
            RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE_RESOLUTION_ENABLED_DEFAULT);

        if (nnId != null && resolveNeeded) {
          DomainNameResolver dnr = DomainNameResolverFactory.newInstance(
              conf, nsId, RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE_RESOLVER_IMPL);

          Map<String, InetSocketAddress> hosts = Maps.newLinkedHashMap();
          Map<String, InetSocketAddress> resolvedHosts =
              DFSUtilClient.getResolvedAddressesForNnId(conf, nsId, nnId, dnr,
                  null, DFS_NAMENODE_RPC_ADDRESS_KEY,
                  DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY);
          hosts.putAll(resolvedHosts);
          for (InetSocketAddress isa : hosts.values()) {
            NamenodeHeartbeatService heartbeatService =
                createNamenodeHeartbeatService(nsId, nnId, isa.getHostName());
            if (heartbeatService != null) {
              ret.put(heartbeatService.getNamenodeDesc(), heartbeatService);
            }
          }
        } else {
          NamenodeHeartbeatService heartbeatService =
              createNamenodeHeartbeatService(nsId, nnId);
          if (heartbeatService != null) {
            ret.put(heartbeatService.getNamenodeDesc(), heartbeatService);
          }
        }
      }
    }

    return ret.values();
  }

  /**
   * Create a new status updater for the local Namenode.
   *
   * @return Updater of the status for the local Namenode.
   */
  @VisibleForTesting
  public NamenodeHeartbeatService createLocalNamenodeHeartbeatService() {
    // Detect NN running in this machine
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    if (nsId == null) {
      LOG.error("Cannot find local nameservice id");
      return null;
    }
    String nnId = null;
    if (HAUtil.isHAEnabled(conf, nsId)) {
      nnId = HAUtil.getNameNodeId(conf, nsId);
      if (nnId == null) {
        LOG.error("Cannot find namenode id for local {}", nsId);
        return null;
      }
    }

    return createNamenodeHeartbeatService(nsId, nnId);
  }

  /**
   * Create a heartbeat monitor for a particular Namenode.
   *
   * @param nsId Identifier of the nameservice to monitor.
   * @param nnId Identifier of the namenode (HA) to monitor.
   * @return Updater of the status for the specified Namenode.
   */
  protected NamenodeHeartbeatService createNamenodeHeartbeatService(
      String nsId, String nnId) {

    LOG.info("Creating heartbeat service for Namenode {} in {}", nnId, nsId);
    NamenodeHeartbeatService ret = new NamenodeHeartbeatService(
        namenodeResolver, nsId, nnId);
    return ret;
  }

  protected NamenodeHeartbeatService createNamenodeHeartbeatService(
      String nsId, String nnId, String resolvedHost) {

    LOG.info("Creating heartbeat service for" +
        " Namenode {}, resolved host {}, in {}", nnId, resolvedHost, nsId);
    NamenodeHeartbeatService ret = new NamenodeHeartbeatService(
        namenodeResolver, nsId, nnId, resolvedHost);
    return ret;
  }

  /////////////////////////////////////////////////////////
  // Router State Management
  /////////////////////////////////////////////////////////

  /**
   * Update the router state and heartbeat to the state store.
   *
   * @param newState The new router state.
   */
  public void updateRouterState(RouterServiceState newState) {
    this.state = newState;
    if (this.routerHeartbeatService != null) {
      this.routerHeartbeatService.updateStateAsync();
    }
  }

  /**
   * Get the status of the router.
   *
   * @return Status of the router.
   */
  public RouterServiceState getRouterState() {
    return this.state;
  }

  /**
   * Compare router state.
   *
   * @param routerState the router service state.
   * @return true if the given router state is same as the state maintained by the router object.
   */
  public boolean isRouterState(RouterServiceState routerState) {
    return routerState.equals(this.state);
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
   * Get the metrics system for the Router.
   *
   * @return Router metrics.
   */
  public RouterMetrics getRouterMetrics() {
    if (this.metrics != null) {
      return this.metrics.getRouterMetrics();
    }
    return null;
  }

  /**
   * Get the metrics system for the Router Client.
   *
   * @return Router Client metrics.
   */
  public RouterClientMetrics getRouterClientMetrics() {
    if (this.metrics != null) {
      return this.metrics.getRouterClientMetrics();
    }
    return null;
  }

  /**
   * Get the federation metrics.
   *
   * @return Federation metrics.
   */
  public RBFMetrics getMetrics() {
    if (this.metrics != null) {
      return this.metrics.getRBFMetrics();
    }
    return null;
  }

  /**
   * Get the namenode metrics.
   *
   * @return the namenode metrics.
   * @throws IOException if the namenode metrics are not initialized.
   */
  public NamenodeBeanMetrics getNamenodeMetrics() throws IOException {
    if (this.metrics == null) {
      throw new IOException("Namenode metrics is not initialized");
    }
    return this.metrics.getNamenodeMetrics();
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

  /**
   * Get the state store interface for the router heartbeats.
   *
   * @return RouterStore state store API handle.
   */
  public RouterStore getRouterStateManager() {
    if (this.routerStateManager == null && this.stateStore != null) {
      this.routerStateManager = this.stateStore.getRegisteredRecordStore(
          RouterStore.class);
    }
    return this.routerStateManager;
  }

  /////////////////////////////////////////////////////////
  // Router info
  /////////////////////////////////////////////////////////

  /**
   * Get the start date of the Router.
   *
   * @return Start date of the router.
   */
  public long getStartTime() {
    return this.startTime;
  }

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
   * @param id Identifier of the Router.
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

  /**
   * Check if the quota system is enabled in Router.
   * @return True if the quota system is enabled in Router.
   */
  public boolean isQuotaEnabled() {
    return this.quotaManager != null;
  }

  /**
   * Get route quota manager.
   * @return RouterQuotaManager Quota manager.
   */
  public RouterQuotaManager getQuotaManager() {
    return this.quotaManager;
  }

  /**
   * Get quota cache update service.
   */
  @VisibleForTesting
  RouterQuotaUpdateService getQuotaCacheUpdateService() {
    return this.quotaUpdateService;
  }

  /**
   * Get the list of namenode heartbeat service.
   */
  @VisibleForTesting
  Collection<NamenodeHeartbeatService> getNamenodeHeartbeatServices() {
    return this.namenodeHeartbeatServices;
  }

  /**
   * Get this router heartbeat service.
   */
  @VisibleForTesting
  RouterHeartbeatService getRouterHeartbeatService() {
    return this.routerHeartbeatService;
  }

  /**
   * Get the Router safe mode service.
   */
  RouterSafemodeService getSafemodeService() {
    return this.safemodeService;
  }

  /**
   * Get router admin server.
   *
   * @return Null if admin is not enabled.
   */
  public RouterAdminServer getAdminServer() {
    return adminServer;
  }

  /**
   * Set router configuration.
   *
   * @param conf the configuration.
   */
  @VisibleForTesting
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

}
