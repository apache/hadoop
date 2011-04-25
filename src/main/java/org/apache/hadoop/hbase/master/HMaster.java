/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorService.ExecutorType;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HMasterRegionInterface;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.master.LoadBalancer.RegionPlan;
import org.apache.hadoop.hbase.master.handler.DeleteTableHandler;
import org.apache.hadoop.hbase.master.handler.DisableTableHandler;
import org.apache.hadoop.hbase.master.handler.EnableTableHandler;
import org.apache.hadoop.hbase.master.handler.ModifyTableHandler;
import org.apache.hadoop.hbase.master.handler.TableAddFamilyHandler;
import org.apache.hadoop.hbase.master.handler.TableDeleteFamilyHandler;
import org.apache.hadoop.hbase.master.handler.TableModifyFamilyHandler;
import org.apache.hadoop.hbase.master.metrics.MasterMetrics;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ClusterId;
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.RegionServerTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.DNS;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

/**
 * HMaster is the "master server" for HBase. An HBase cluster has one active
 * master.  If many masters are started, all compete.  Whichever wins goes on to
 * run the cluster.  All others park themselves in their constructor until
 * master or cluster shutdown or until the active master loses its lease in
 * zookeeper.  Thereafter, all running master jostle to take over master role.
 *
 * <p>The Master can be asked shutdown the cluster. See {@link #shutdown()}.  In
 * this case it will tell all regionservers to go down and then wait on them
 * all reporting in that they are down.  This master will then shut itself down.
 *
 * <p>You can also shutdown just this master.  Call {@link #stopMaster()}.
 *
 * @see HMasterInterface
 * @see HMasterRegionInterface
 * @see Watcher
 */
public class HMaster extends Thread
implements HMasterInterface, HMasterRegionInterface, MasterServices, Server {
  private static final Log LOG = LogFactory.getLog(HMaster.class.getName());

  // MASTER is name of the webapp and the attribute name used stuffing this
  //instance into web context.
  public static final String MASTER = "master";

  // The configuration for the Master
  private final Configuration conf;
  // server for the web ui
  private InfoServer infoServer;

  // Our zk client.
  private ZooKeeperWatcher zooKeeper;
  // Manager and zk listener for master election
  private ActiveMasterManager activeMasterManager;
  // Region server tracker
  private RegionServerTracker regionServerTracker;

  // RPC server for the HMaster
  private final RpcServer rpcServer;
  // Address of the HMaster
  private final HServerAddress address;
  // Metrics for the HMaster
  private final MasterMetrics metrics;
  // file system manager for the master FS operations
  private MasterFileSystem fileSystemManager;

  private HConnection connection;

  // server manager to deal with region server info
  private ServerManager serverManager;

  // manager of assignment nodes in zookeeper
  AssignmentManager assignmentManager;
  // manager of catalog regions
  private CatalogTracker catalogTracker;
  // Cluster status zk tracker and local setter
  private ClusterStatusTracker clusterStatusTracker;

  // This flag is for stopping this Master instance.  Its set when we are
  // stopping or aborting
  private volatile boolean stopped = false;
  // Set on abort -- usually failure of our zk session.
  private volatile boolean abort = false;
  // flag set after we become the active master (used for testing)
  private volatile boolean isActiveMaster = false;
  // flag set after we complete initialization once active (used for testing)
  private volatile boolean initialized = false;

  // Instance of the hbase executor service.
  ExecutorService executorService;

  private LoadBalancer balancer;
  private Thread balancerChore;
  // If 'true', the balancer is 'on'.  If 'false', the balancer will not run.
  private volatile boolean balanceSwitch = true;

  private Thread catalogJanitorChore;
  private LogCleaner logCleaner;

  private MasterCoprocessorHost cpHost;

  /**
   * Initializes the HMaster. The steps are as follows:
   * <p>
   * <ol>
   * <li>Initialize HMaster RPC and address
   * <li>Connect to ZooKeeper.
   * </ol>
   * <p>
   * Remaining steps of initialization occur in {@link #run()} so that they
   * run in their own thread rather than within the context of the constructor.
   * @throws InterruptedException
   */
  public HMaster(final Configuration conf)
  throws IOException, KeeperException, InterruptedException {
    this.conf = conf;

    /*
     * Determine address and initialize RPC server (but do not start).
     * The RPC server ports can be ephemeral. Create a ZKW instance.
     */
    HServerAddress a = new HServerAddress(getMyAddress(this.conf));
    int numHandlers = conf.getInt("hbase.regionserver.handler.count", 10);
    this.rpcServer = HBaseRPC.getServer(this,
      new Class<?>[]{HMasterInterface.class, HMasterRegionInterface.class},
      a.getBindAddress(), a.getPort(),
      numHandlers,
      0, // we dont use high priority handlers in master
      false, conf,
      0); // this is a DNC w/o high priority handlers
    this.address = new HServerAddress(rpcServer.getListenerAddress());

    // initialize server principal (if using secure Hadoop)
    User.login(conf, "hbase.master.keytab.file",
        "hbase.master.kerberos.principal", this.address.getHostname());

    // set the thread name now we have an address
    setName(MASTER + "-" + this.address);

    Replication.decorateMasterConfiguration(this.conf);

    this.rpcServer.startThreads();

    // Hack! Maps DFSClient => Master for logs.  HDFS made this
    // config param for task trackers, but we can piggyback off of it.
    if (this.conf.get("mapred.task.id") == null) {
      this.conf.set("mapred.task.id", "hb_m_" + this.address.toString() +
        "_" + System.currentTimeMillis());
    }

    this.zooKeeper = new ZooKeeperWatcher(conf, MASTER + ":" +
        address.getPort(), this);

    this.metrics = new MasterMetrics(getServerName());
  }

  /**
   * Stall startup if we are designated a backup master; i.e. we want someone
   * else to become the master before proceeding.
   * @param c
   * @param amm
   * @throws InterruptedException
   */
  private static void stallIfBackupMaster(final Configuration c,
      final ActiveMasterManager amm)
  throws InterruptedException {
    // If we're a backup master, stall until a primary to writes his address
    if (!c.getBoolean(HConstants.MASTER_TYPE_BACKUP,
      HConstants.DEFAULT_MASTER_TYPE_BACKUP)) {
      return;
    }
    LOG.debug("HMaster started in backup mode.  " +
      "Stalling until master znode is written.");
    // This will only be a minute or so while the cluster starts up,
    // so don't worry about setting watches on the parent znode
    while (!amm.isActiveMaster()) {
      LOG.debug("Waiting for master address ZNode to be written " +
        "(Also watching cluster state node)");
      Thread.sleep(c.getInt("zookeeper.session.timeout", 180 * 1000));
    }
  }

  /**
   * Main processing loop for the HMaster.
   * <ol>
   * <li>Block until becoming active master
   * <li>Finish initialization via {@link #finishInitialization()}
   * <li>Enter loop until we are stopped
   * <li>Stop services and perform cleanup once stopped
   * </ol>
   */
  @Override
  public void run() {
    try {
      /*
       * Block on becoming the active master.
       *
       * We race with other masters to write our address into ZooKeeper.  If we
       * succeed, we are the primary/active master and finish initialization.
       *
       * If we do not succeed, there is another active master and we should
       * now wait until it dies to try and become the next active master.  If we
       * do not succeed on our first attempt, this is no longer a cluster startup.
       */
      this.activeMasterManager = new ActiveMasterManager(zooKeeper, address, this);
      this.zooKeeper.registerListener(activeMasterManager);
      stallIfBackupMaster(this.conf, this.activeMasterManager);
      this.activeMasterManager.blockUntilBecomingActiveMaster();
      // We are either the active master or we were asked to shutdown
      if (!this.stopped) {
        finishInitialization();
        loop();
      }
    } catch (Throwable t) {
      abort("Unhandled exception. Starting shutdown.", t);
    } finally {
      stopChores();
      // Wait for all the remaining region servers to report in IFF we were
      // running a cluster shutdown AND we were NOT aborting.
      if (!this.abort && this.serverManager != null &&
          this.serverManager.isClusterShutdown()) {
        this.serverManager.letRegionServersShutdown();
      }
      stopServiceThreads();
      // Stop services started for both backup and active masters
      if (this.activeMasterManager != null) this.activeMasterManager.stop();
      if (this.catalogTracker != null) this.catalogTracker.stop();
      if (this.serverManager != null) this.serverManager.stop();
      if (this.assignmentManager != null) this.assignmentManager.stop();
      if (this.fileSystemManager != null) this.fileSystemManager.stop();
      HConnectionManager.deleteConnection(this.conf, true);
      this.zooKeeper.close();
    }
    LOG.info("HMaster main thread exiting");
  }

  private void loop() {
    // Check if we should stop every second.
    Sleeper sleeper = new Sleeper(1000, this);
    while (!this.stopped) {
      sleeper.sleep();
    }
  }

  /**
   * Finish initialization of HMaster after becoming the primary master.
   *
   * <ol>
   * <li>Initialize master components - file system manager, server manager,
   *     assignment manager, region server tracker, catalog tracker, etc</li>
   * <li>Start necessary service threads - rpc server, info server,
   *     executor services, etc</li>
   * <li>Set cluster as UP in ZooKeeper</li>
   * <li>Wait for RegionServers to check-in</li>
   * <li>Split logs and perform data recovery, if necessary</li>
   * <li>Ensure assignment of root and meta regions<li>
   * <li>Handle either fresh cluster start or master failover</li>
   * </ol>
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  private void finishInitialization()
  throws IOException, InterruptedException, KeeperException {

    isActiveMaster = true;

    /*
     * We are active master now... go initialize components we need to run.
     * Note, there may be dross in zk from previous runs; it'll get addressed
     * below after we determine if cluster startup or failover.
     */

    // TODO: Do this using Dependency Injection, using PicoContainer, Guice or Spring.
    this.fileSystemManager = new MasterFileSystem(this, metrics);
    // publish cluster ID
    ClusterId.setClusterId(this.zooKeeper,
        fileSystemManager.getClusterId());

    this.connection = HConnectionManager.getConnection(conf);
    this.executorService = new ExecutorService(getServerName());

    this.serverManager = new ServerManager(this, this, metrics);

    this.catalogTracker = new CatalogTracker(this.zooKeeper, this.connection,
      this, conf.getInt("hbase.master.catalog.timeout", Integer.MAX_VALUE));
    this.catalogTracker.start();

    this.assignmentManager = new AssignmentManager(this, serverManager,
      this.catalogTracker, this.executorService);
    this.balancer = new LoadBalancer(conf);
    zooKeeper.registerListenerFirst(assignmentManager);

    this.regionServerTracker = new RegionServerTracker(zooKeeper, this,
      this.serverManager);
    this.regionServerTracker.start();

    // Set the cluster as up.  If new RSs, they'll be waiting on this before
    // going ahead with their startup.
    this.clusterStatusTracker = new ClusterStatusTracker(getZooKeeper(), this);
    this.clusterStatusTracker.start();
    boolean wasUp = this.clusterStatusTracker.isClusterUp();
    if (!wasUp) this.clusterStatusTracker.setClusterUp();

    LOG.info("Server active/primary master; " + this.address +
      ", sessionid=0x" +
      Long.toHexString(this.zooKeeper.getZooKeeper().getSessionId()) +
      ", cluster-up flag was=" + wasUp);

    // initialize master side coprocessors before we start handling requests
    this.cpHost = new MasterCoprocessorHost(this, this.conf);

    // start up all service threads.
    startServiceThreads();

    // Wait for region servers to report in.  Returns count of regions.
    int regionCount = this.serverManager.waitForRegionServers();

    // TODO: Should do this in background rather than block master startup
    this.fileSystemManager.
      splitLogAfterStartup(this.serverManager.getOnlineServers());

    // Make sure root and meta assigned before proceeding.
    assignRootAndMeta();

    // Is this fresh start with no regions assigned or are we a master joining
    // an already-running cluster?  If regionsCount == 0, then for sure a
    // fresh start.  TOOD: Be fancier.  If regionsCount == 2, perhaps the
    // 2 are .META. and -ROOT- and we should fall into the fresh startup
    // branch below.  For now, do processFailover.
    if (regionCount == 0) {
      LOG.info("Master startup proceeding: cluster startup");
      this.assignmentManager.cleanoutUnassigned();
      this.assignmentManager.assignAllUserRegions();
    } else {
      LOG.info("Master startup proceeding: master failover");
      this.assignmentManager.processFailover();
    }

    // Start balancer and meta catalog janitor after meta and regions have
    // been assigned.
    this.balancerChore = getAndStartBalancerChore(this);
    this.catalogJanitorChore =
      Threads.setDaemonThreadRunning(new CatalogJanitor(this, this));

    LOG.info("Master has completed initialization");
    initialized = true;
  }

  /**
   * Check <code>-ROOT-</code> and <code>.META.</code> are assigned.  If not,
   * assign them.
   * @throws InterruptedException
   * @throws IOException
   * @throws KeeperException
   * @return Count of regions we assigned.
   */
  int assignRootAndMeta()
  throws InterruptedException, IOException, KeeperException {
    int assigned = 0;
    long timeout = this.conf.getLong("hbase.catalog.verification.timeout", 1000);

    // Work on ROOT region.  Is it in zk in transition?
    boolean rit = this.assignmentManager.
      processRegionInTransitionAndBlockUntilAssigned(HRegionInfo.ROOT_REGIONINFO);
    if (!catalogTracker.verifyRootRegionLocation(timeout)) {
      this.assignmentManager.assignRoot();
      this.catalogTracker.waitForRoot();
      assigned++;
    } else {
      // Region already assigned.  We didnt' assign it.  Add to in-memory state.
      this.assignmentManager.regionOnline(HRegionInfo.ROOT_REGIONINFO,
        this.serverManager.getHServerInfo(this.catalogTracker.getRootLocation()));
    }
    LOG.info("-ROOT- assigned=" + assigned + ", rit=" + rit +
      ", location=" + catalogTracker.getRootLocation());

    // Work on meta region
    rit = this.assignmentManager.
      processRegionInTransitionAndBlockUntilAssigned(HRegionInfo.FIRST_META_REGIONINFO);
    if (!this.catalogTracker.verifyMetaRegionLocation(timeout)) {
      this.assignmentManager.assignMeta();
      this.catalogTracker.waitForMeta();
      // Above check waits for general meta availability but this does not
      // guarantee that the transition has completed
      this.assignmentManager.waitForAssignment(HRegionInfo.FIRST_META_REGIONINFO);
      assigned++;
    } else {
      // Region already assigned.  We didnt' assign it.  Add to in-memory state.
      this.assignmentManager.regionOnline(HRegionInfo.FIRST_META_REGIONINFO,
        this.serverManager.getHServerInfo(this.catalogTracker.getMetaLocation()));
    }
    LOG.info(".META. assigned=" + assigned + ", rit=" + rit +
      ", location=" + catalogTracker.getMetaLocation());
    return assigned;
  }

  /*
   * @return This masters' address.
   * @throws UnknownHostException
   */
  private static String getMyAddress(final Configuration c)
  throws UnknownHostException {
    // Find out our address up in DNS.
    String s = DNS.getDefaultHost(c.get("hbase.master.dns.interface","default"),
      c.get("hbase.master.dns.nameserver","default"));
    s += ":" + c.get(HConstants.MASTER_PORT,
        Integer.toString(HConstants.DEFAULT_MASTER_PORT));
    return s;
  }

  /** @return HServerAddress of the master server */
  public HServerAddress getMasterAddress() {
    return this.address;
  }

  public long getProtocolVersion(String protocol, long clientVersion) {
    if (HMasterInterface.class.getName().equals(protocol)) {
      return HMasterInterface.VERSION;
    } else if (HMasterRegionInterface.class.getName().equals(protocol)) {
      return HMasterRegionInterface.VERSION;
    }
    // unknown protocol
    LOG.warn("Version requested for unimplemented protocol: "+protocol);
    return -1;
  }

  /** @return InfoServer object. Maybe null.*/
  public InfoServer getInfoServer() {
    return this.infoServer;
  }

  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }

  @Override
  public ServerManager getServerManager() {
    return this.serverManager;
  }

  @Override
  public ExecutorService getExecutorService() {
    return this.executorService;
  }

  @Override
  public MasterFileSystem getMasterFileSystem() {
    return this.fileSystemManager;
  }

  /**
   * Get the ZK wrapper object - needed by master_jsp.java
   * @return the zookeeper wrapper
   */
  public ZooKeeperWatcher getZooKeeperWatcher() {
    return this.zooKeeper;
  }

  /*
   * Start up all services. If any of these threads gets an unhandled exception
   * then they just die with a logged message.  This should be fine because
   * in general, we do not expect the master to get such unhandled exceptions
   *  as OOMEs; it should be lightly loaded. See what HRegionServer does if
   *  need to install an unexpected exception handler.
   */
  private void startServiceThreads() throws IOException{
 
   // Start the executor service pools
   this.executorService.startExecutorService(ExecutorType.MASTER_OPEN_REGION,
      conf.getInt("hbase.master.executor.openregion.threads", 5));
   this.executorService.startExecutorService(ExecutorType.MASTER_CLOSE_REGION,
      conf.getInt("hbase.master.executor.closeregion.threads", 5));
   this.executorService.startExecutorService(ExecutorType.MASTER_SERVER_OPERATIONS,
      conf.getInt("hbase.master.executor.serverops.threads", 3));
   this.executorService.startExecutorService(ExecutorType.MASTER_META_SERVER_OPERATIONS,
      conf.getInt("hbase.master.executor.serverops.threads", 5));
   
   // We depend on there being only one instance of this executor running
   // at a time.  To do concurrency, would need fencing of enable/disable of
   // tables.
   this.executorService.startExecutorService(ExecutorType.MASTER_TABLE_OPERATIONS, 1);

   // Start log cleaner thread
   String n = Thread.currentThread().getName();
   this.logCleaner =
      new LogCleaner(conf.getInt("hbase.master.cleaner.interval", 60 * 1000),
         this, conf, getMasterFileSystem().getFileSystem(),
         getMasterFileSystem().getOldLogDir());
         Threads.setDaemonThreadRunning(logCleaner, n + ".oldLogCleaner");

   // Put up info server.
   int port = this.conf.getInt("hbase.master.info.port", 60010);
   if (port >= 0) {
     String a = this.conf.get("hbase.master.info.bindAddress", "0.0.0.0");
     this.infoServer = new InfoServer(MASTER, a, port, false);
     this.infoServer.setAttribute(MASTER, this);
     this.infoServer.start();
    }
   
    // Start allowing requests to happen.
    this.rpcServer.openServer();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Started service threads");
    }

  }

  private void stopServiceThreads() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping service threads");
    }
    if (this.rpcServer != null) this.rpcServer.stop();
    // Clean up and close up shop
    if (this.logCleaner!= null) this.logCleaner.interrupt();
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    if (this.executorService != null) this.executorService.shutdown();
  }

  private static Thread getAndStartBalancerChore(final HMaster master) {
    String name = master.getServerName() + "-BalancerChore";
    int balancerPeriod =
      master.getConfiguration().getInt("hbase.balancer.period", 300000);
    // Start up the load balancer chore
    Chore chore = new Chore(name, balancerPeriod, master) {
      @Override
      protected void chore() {
        master.balance();
      }
    };
    return Threads.setDaemonThreadRunning(chore);
  }

  private void stopChores() {
    if (this.balancerChore != null) {
      this.balancerChore.interrupt();
    }
    if (this.catalogJanitorChore != null) {
      this.catalogJanitorChore.interrupt();
    }
  }

  @Override
  public MapWritable regionServerStartup(final HServerInfo serverInfo,
    final long serverCurrentTime)
  throws IOException {
    // Set the ip into the passed in serverInfo.  Its ip is more than likely
    // not the ip that the master sees here.  See at end of this method where
    // we pass it back to the regionserver by setting "hbase.regionserver.address"
    // Everafter, the HSI combination 'server name' is what uniquely identifies
    // the incoming RegionServer.
    InetSocketAddress address = new InetSocketAddress(
        HBaseServer.getRemoteIp().getHostName(),
        serverInfo.getServerAddress().getPort());
    serverInfo.setServerAddress(new HServerAddress(address));

    // Register with server manager
    this.serverManager.regionServerStartup(serverInfo, serverCurrentTime);
    // Send back some config info
    MapWritable mw = createConfigurationSubset();
     mw.put(new Text("hbase.regionserver.address"),
         serverInfo.getServerAddress());
    return mw;
  }

  /**
   * @return Subset of configuration to pass initializing regionservers: e.g.
   * the filesystem to use and root directory to use.
   */
  protected MapWritable createConfigurationSubset() {
    MapWritable mw = addConfig(new MapWritable(), HConstants.HBASE_DIR);
    return addConfig(mw, "fs.default.name");
  }

  private MapWritable addConfig(final MapWritable mw, final String key) {
    mw.put(new Text(key), new Text(this.conf.get(key)));
    return mw;
  }

  @Override
  public HMsg [] regionServerReport(HServerInfo serverInfo, HMsg msgs[],
    HRegionInfo[] mostLoadedRegions)
  throws IOException {
    return adornRegionServerAnswer(serverInfo,
      this.serverManager.regionServerReport(serverInfo, msgs, mostLoadedRegions));
  }

  /**
   * Override if you'd add messages to return to regionserver <code>hsi</code>
   * or to send an exception.
   * @param msgs Messages to add to
   * @return Messages to return to
   * @throws IOException exceptions that were injected for the region servers
   */
  protected HMsg [] adornRegionServerAnswer(final HServerInfo hsi,
      final HMsg [] msgs) throws IOException {
    return msgs;
  }

  public boolean isMasterRunning() {
    return !isStopped();
  }

  /**
   * @return Maximum time we should run balancer for
   */
  private int getBalancerCutoffTime() {
    int balancerCutoffTime =
      getConfiguration().getInt("hbase.balancer.max.balancing", -1);
    if (balancerCutoffTime == -1) {
      // No time period set so create one -- do half of balancer period.
      int balancerPeriod =
        getConfiguration().getInt("hbase.balancer.period", 300000);
      balancerCutoffTime = balancerPeriod / 2;
      // If nonsense period, set it to balancerPeriod
      if (balancerCutoffTime <= 0) balancerCutoffTime = balancerPeriod;
    }
    return balancerCutoffTime;
  }

  @Override
  public boolean balance() {
    // If balance not true, don't run balancer.
    if (!this.balanceSwitch) return false;
    // Do this call outside of synchronized block.
    int maximumBalanceTime = getBalancerCutoffTime();
    long cutoffTime = System.currentTimeMillis() + maximumBalanceTime;
    boolean balancerRan;
    synchronized (this.balancer) {
      // Only allow one balance run at at time.
      if (this.assignmentManager.isRegionsInTransition()) {
        LOG.debug("Not running balancer because " +
          this.assignmentManager.getRegionsInTransition().size() +
          " region(s) in transition: " +
          org.apache.commons.lang.StringUtils.
            abbreviate(this.assignmentManager.getRegionsInTransition().toString(), 256));
        return false;
      }
      if (this.serverManager.areDeadServersInProgress()) {
        LOG.debug("Not running balancer because processing dead regionserver(s): " +
          this.serverManager.getDeadServers());
        return false;
      }

      if (this.cpHost != null) {
        try {
          if (this.cpHost.preBalance()) {
            LOG.debug("Coprocessor bypassing balancer request");
            return false;
          }
        } catch (IOException ioe) {
          LOG.error("Error invoking master coprocessor preBalance()", ioe);
          return false;
        }
      }

      Map<HServerInfo, List<HRegionInfo>> assignments =
        this.assignmentManager.getAssignments();
      // Returned Map from AM does not include mention of servers w/o assignments.
      for (Map.Entry<String, HServerInfo> e:
          this.serverManager.getOnlineServers().entrySet()) {
        HServerInfo hsi = e.getValue();
        if (!assignments.containsKey(hsi)) {
          assignments.put(hsi, new ArrayList<HRegionInfo>());
        }
      }
      List<RegionPlan> plans = this.balancer.balanceCluster(assignments);
      int rpCount = 0;	// number of RegionPlans balanced so far
      long totalRegPlanExecTime = 0;
      balancerRan = plans != null;
      if (plans != null && !plans.isEmpty()) {
        for (RegionPlan plan: plans) {
          LOG.info("balance " + plan);
          long balStartTime = System.currentTimeMillis();
          this.assignmentManager.balance(plan);
          totalRegPlanExecTime += System.currentTimeMillis()-balStartTime;
          rpCount++;
          if (rpCount < plans.size() &&
              // if performing next balance exceeds cutoff time, exit the loop
              (System.currentTimeMillis() + (totalRegPlanExecTime / rpCount)) > cutoffTime) {
            LOG.debug("No more balancing till next balance run; maximumBalanceTime=" +
              maximumBalanceTime);
            break;
          }
        }
      }
      if (this.cpHost != null) {
        try {
          this.cpHost.postBalance();
        } catch (IOException ioe) {
          // balancing already succeeded so don't change the result
          LOG.error("Error invoking master coprocessor postBalance()", ioe);
        }
      }
    }
    return balancerRan;
  }

  @Override
  public boolean balanceSwitch(final boolean b) {
    boolean oldValue = this.balanceSwitch;
    boolean newValue = b;
    try {
      if (this.cpHost != null) {
        newValue = this.cpHost.preBalanceSwitch(newValue);
      }
      this.balanceSwitch = newValue;
      LOG.info("Balance=" + newValue);
      if (this.cpHost != null) {
        this.cpHost.postBalanceSwitch(oldValue, newValue);
      }
    } catch (IOException ioe) {
      LOG.warn("Error flipping balance switch", ioe);
    }
    return oldValue;
  }

  /**
   * Switch for the background {@link CatalogJanitor} thread.
   * Used for testing.  The thread will continue to run.  It will just be a noop
   * if disabled.
   * @param b If false, the catalog janitor won't do anything.
   */
  public void setCatalogJanitorEnabled(final boolean b) {
    ((CatalogJanitor)this.catalogJanitorChore).setEnabled(b);
  }

  @Override
  public void move(final byte[] encodedRegionName, final byte[] destServerName)
  throws UnknownRegionException {
    Pair<HRegionInfo, HServerInfo> p =
      this.assignmentManager.getAssignment(encodedRegionName);
    if (p == null)
      throw new UnknownRegionException(Bytes.toString(encodedRegionName));
    HRegionInfo hri = p.getFirst();
    HServerInfo dest = null;
    if (destServerName == null || destServerName.length == 0) {
      LOG.info("Passed destination servername is null/empty so " +
        "choosing a server at random");
      this.assignmentManager.clearRegionPlan(hri);
      // Unassign will reassign it elsewhere choosing random server.
      this.assignmentManager.unassign(hri);
    } else {
      dest = this.serverManager.getServerInfo(new String(destServerName));

      if (this.cpHost != null) {
        this.cpHost.preMove(p.getFirst(), p.getSecond(), dest);
      }
      RegionPlan rp = new RegionPlan(p.getFirst(), p.getSecond(), dest);
      this.assignmentManager.balance(rp);
      if (this.cpHost != null) {
        this.cpHost.postMove(p.getFirst(), p.getSecond(), dest);
      }
    }
  }

  public void createTable(HTableDescriptor desc, byte [][] splitKeys)
  throws IOException {
    createTable(desc, splitKeys, false);
  }

  public void createTable(HTableDescriptor desc, byte [][] splitKeys,
      boolean sync)
  throws IOException {
    if (!isMasterRunning()) {
      throw new MasterNotRunningException();
    }
    if (cpHost != null) {
      cpHost.preCreateTable(desc, splitKeys);
    }
    HRegionInfo [] newRegions = null;
    if(splitKeys == null || splitKeys.length == 0) {
      newRegions = new HRegionInfo [] { new HRegionInfo(desc, null, null) };
    } else {
      int numRegions = splitKeys.length + 1;
      newRegions = new HRegionInfo[numRegions];
      byte [] startKey = null;
      byte [] endKey = null;
      for(int i=0;i<numRegions;i++) {
        endKey = (i == splitKeys.length) ? null : splitKeys[i];
        newRegions[i] = new HRegionInfo(desc, startKey, endKey);
        startKey = endKey;
      }
    }
    int timeout = conf.getInt("hbase.client.catalog.timeout", 10000);
    // Need META availability to create a table
    try {
      if(catalogTracker.waitForMeta(timeout) == null) {
        throw new NotAllMetaRegionsOnlineException();
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted waiting for meta availability", e);
      throw new IOException(e);
    }
    createTable(newRegions, sync);
  }

  private synchronized void createTable(final HRegionInfo [] newRegions,
      final boolean sync)
  throws IOException {
    String tableName = newRegions[0].getTableDesc().getNameAsString();
    if(MetaReader.tableExists(catalogTracker, tableName)) {
      throw new TableExistsException(tableName);
    }
    for (HRegionInfo newRegion : newRegions) {
      // 1. Set table enabling flag up in zk.
      try {
        assignmentManager.getZKTable().setEnabledTable(tableName);
      } catch (KeeperException e) {
        throw new IOException("Unable to ensure that the table will be" +
          " enabled because of a ZooKeeper issue", e);
      }

      // 2. Create HRegion
      HRegion region = HRegion.createHRegion(newRegion,
        fileSystemManager.getRootDir(), conf);

      // 3. Insert into META
      MetaEditor.addRegionToMeta(catalogTracker, region.getRegionInfo());

      // 4. Close the new region to flush to disk.  Close log file too.
      region.close();
      region.getLog().closeAndDelete();
    }

    // 5. Trigger immediate assignment of the regions in round-robin fashion
    List<HServerInfo> servers = serverManager.getOnlineServersList();
    this.assignmentManager.bulkAssignUserRegions(newRegions, servers, sync);

    // 6. If sync, wait for assignment of regions
    if (sync) {
      LOG.debug("Waiting for " + newRegions.length + " region(s) to be assigned");
      for (HRegionInfo regionInfo : newRegions) {
        try {
          this.assignmentManager.waitForAssignment(regionInfo);
        } catch (InterruptedException e) {
          LOG.info("Interrupted waiting for region to be assigned during " +
              "create table call", e);
          Thread.currentThread().interrupt();
          return;
        }
      }
    }

    if (cpHost != null) {
      cpHost.postCreateTable(newRegions, sync);
    }
  }

  private static boolean isCatalogTable(final byte [] tableName) {
    return Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME) ||
           Bytes.equals(tableName, HConstants.META_TABLE_NAME);
  }

  public void deleteTable(final byte [] tableName) throws IOException {
    if (cpHost != null) {
      cpHost.preDeleteTable(tableName);
    }
    this.executorService.submit(new DeleteTableHandler(tableName, this, this));
    if (cpHost != null) {
      cpHost.postDeleteTable(tableName);
    }
  }

  public void addColumn(byte [] tableName, HColumnDescriptor column)
  throws IOException {
    if (cpHost != null) {
      cpHost.preAddColumn(tableName, column);
    }
    new TableAddFamilyHandler(tableName, column, this, this).process();
    if (cpHost != null) {
      cpHost.postAddColumn(tableName, column);
    }
  }

  public void modifyColumn(byte [] tableName, HColumnDescriptor descriptor)
  throws IOException {
    if (cpHost != null) {
      cpHost.preModifyColumn(tableName, descriptor);
    }
    new TableModifyFamilyHandler(tableName, descriptor, this, this).process();
    if (cpHost != null) {
      cpHost.postModifyColumn(tableName, descriptor);
    }
  }

  public void deleteColumn(final byte [] tableName, final byte [] c)
  throws IOException {
    if (cpHost != null) {
      cpHost.preDeleteColumn(tableName, c);
    }
    new TableDeleteFamilyHandler(tableName, c, this, this).process();
    if (cpHost != null) {
      cpHost.postDeleteColumn(tableName, c);
    }
  }

  public void enableTable(final byte [] tableName) throws IOException {
    if (cpHost != null) {
      cpHost.preEnableTable(tableName);
    }
    this.executorService.submit(new EnableTableHandler(this, tableName,
      catalogTracker, assignmentManager));
    if (cpHost != null) {
      cpHost.postEnableTable(tableName);
    }
  }

  public void disableTable(final byte [] tableName) throws IOException {
    if (cpHost != null) {
      cpHost.preDisableTable(tableName);
    }
    this.executorService.submit(new DisableTableHandler(this, tableName,
      catalogTracker, assignmentManager));
    if (cpHost != null) {
      cpHost.postDisableTable(tableName);
    }
  }

  /**
   * Return the region and current deployment for the region containing
   * the given row. If the region cannot be found, returns null. If it
   * is found, but not currently deployed, the second element of the pair
   * may be null.
   */
  Pair<HRegionInfo,HServerAddress> getTableRegionForRow(
      final byte [] tableName, final byte [] rowKey)
  throws IOException {
    final AtomicReference<Pair<HRegionInfo, HServerAddress>> result =
      new AtomicReference<Pair<HRegionInfo, HServerAddress>>(null);

    MetaScannerVisitor visitor =
      new MetaScannerVisitor() {
        @Override
        public boolean processRow(Result data) throws IOException {
          if (data == null || data.size() <= 0) {
            return true;
          }
          Pair<HRegionInfo, HServerAddress> pair =
            MetaReader.metaRowToRegionPair(data);
          if (pair == null) {
            return false;
          }
          if (!Bytes.equals(pair.getFirst().getTableDesc().getName(),
                tableName)) {
            return false;
          }
          result.set(pair);
          return true;
        }
    };

    MetaScanner.metaScan(conf, visitor, tableName, rowKey, 1);
    return result.get();
  }

  @Override
  public void modifyTable(final byte[] tableName, HTableDescriptor htd)
  throws IOException {
    if (cpHost != null) {
      cpHost.preModifyTable(tableName, htd);
    }
    this.executorService.submit(new ModifyTableHandler(tableName, htd, this, this));
    if (cpHost != null) {
      cpHost.postModifyTable(tableName, htd);
    }
  }

  @Override
  public void checkTableModifiable(final byte [] tableName)
  throws IOException {
    String tableNameStr = Bytes.toString(tableName);
    if (isCatalogTable(tableName)) {
      throw new IOException("Can't modify catalog tables");
    }
    if (!MetaReader.tableExists(getCatalogTracker(), tableNameStr)) {
      throw new TableNotFoundException(tableNameStr);
    }
    if (!getAssignmentManager().getZKTable().
        isDisabledTable(Bytes.toString(tableName))) {
      throw new TableNotDisabledException(tableName);
    }
  }

  public void clearFromTransition(HRegionInfo hri) {
    if (this.assignmentManager.isRegionInTransition(hri) != null) {
      this.assignmentManager.clearRegionFromTransition(hri);
    }
  }
  /**
   * @return cluster status
   */
  public ClusterStatus getClusterStatus() {
    ClusterStatus status = new ClusterStatus();
    status.setHBaseVersion(VersionInfo.getVersion());
    status.setServerInfo(serverManager.getOnlineServers().values());
    status.setDeadServers(serverManager.getDeadServers());
    status.setRegionsInTransition(assignmentManager.getRegionsInTransition());
    status.setClusterId(fileSystemManager.getClusterId());
    return status;
  }

  public String getClusterId() {
    return fileSystemManager.getClusterId();
  }

  @Override
  public void abort(final String msg, final Throwable t) {
    if (t != null) LOG.fatal(msg, t);
    else LOG.fatal(msg);
    this.abort = true;
    stop("Aborting");
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return zooKeeper;
  }

  public MasterCoprocessorHost getCoprocessorHost() {
    return cpHost;
  }

  @Override
  public String getServerName() {
    return address.toString();
  }

  @Override
  public CatalogTracker getCatalogTracker() {
    return catalogTracker;
  }

  @Override
  public AssignmentManager getAssignmentManager() {
    return this.assignmentManager;
  }

  @Override
  public void shutdown() {
    if (cpHost != null) {
      try {
        cpHost.preShutdown();
      } catch (IOException ioe) {
        LOG.error("Error call master coprocessor preShutdown()", ioe);
      }
    }
    this.serverManager.shutdownCluster();
    try {
      this.clusterStatusTracker.setClusterDown();
    } catch (KeeperException e) {
      LOG.error("ZooKeeper exception trying to set cluster as down in ZK", e);
    }
  }

  @Override
  public void stopMaster() {
    if (cpHost != null) {
      try {
        cpHost.preStopMaster();
      } catch (IOException ioe) {
        LOG.error("Error call master coprocessor preStopMaster()", ioe);
      }
    }
    stop("Stopped by " + Thread.currentThread().getName());
  }

  @Override
  public void stop(final String why) {
    LOG.info(why);
    this.stopped = true;
    // If we are a backup master, we need to interrupt wait
    synchronized (this.activeMasterManager.clusterHasActiveMaster) {
      this.activeMasterManager.clusterHasActiveMaster.notifyAll();
    }
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  /**
   * Report whether this master is currently the active master or not.
   * If not active master, we are parked on ZK waiting to become active.
   *
   * This method is used for testing.
   *
   * @return true if active master, false if not.
   */
  public boolean isActiveMaster() {
    return isActiveMaster;
  }

  /**
   * Report whether this master has completed with its initialization and is
   * ready.  If ready, the master is also the active master.  A standby master
   * is never ready.
   *
   * This method is used for testing.
   *
   * @return true if master is ready to go, false if not.
   */
  public boolean isInitialized() {
    return initialized;
  }

  @Override
  public void assign(final byte [] regionName, final boolean force)
  throws IOException {
    if (cpHost != null) {
      if (cpHost.preAssign(regionName, force)) {
        return;
      }
    }
    Pair<HRegionInfo, HServerAddress> pair =
      MetaReader.getRegion(this.catalogTracker, regionName);
    if (pair == null) throw new UnknownRegionException(Bytes.toString(regionName));
    assignRegion(pair.getFirst());
    if (cpHost != null) {
      cpHost.postAssign(pair.getFirst());
    }
  }

  public void assignRegion(HRegionInfo hri) {
    assignmentManager.assign(hri, true);
  }

  @Override
  public void unassign(final byte [] regionName, final boolean force)
  throws IOException {
    if (cpHost != null) {
      if (cpHost.preUnassign(regionName, force)) {
        return;
      }
    }
    Pair<HRegionInfo, HServerAddress> pair =
      MetaReader.getRegion(this.catalogTracker, regionName);
    if (pair == null) throw new UnknownRegionException(Bytes.toString(regionName));
    HRegionInfo hri = pair.getFirst();
    if (force) this.assignmentManager.clearRegionFromTransition(hri);
    this.assignmentManager.unassign(hri, force);
    if (cpHost != null) {
      cpHost.postUnassign(hri, force);
    }
  }

  /**
   * Utility for constructing an instance of the passed HMaster class.
   * @param masterClass
   * @param conf
   * @return HMaster instance.
   */
  public static HMaster constructMaster(Class<? extends HMaster> masterClass,
      final Configuration conf)  {
    try {
      Constructor<? extends HMaster> c =
        masterClass.getConstructor(Configuration.class);
      return c.newInstance(conf);
    } catch (InvocationTargetException ite) {
      Throwable target = ite.getTargetException() != null?
        ite.getTargetException(): ite;
      if (target.getCause() != null) target = target.getCause();
      throw new RuntimeException("Failed construction of Master: " +
        masterClass.toString(), target);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of Master: " +
        masterClass.toString() + ((e.getCause() != null)?
          e.getCause().getMessage(): ""), e);
    }
  }


  /**
   * @see org.apache.hadoop.hbase.master.HMasterCommandLine
   */
  public static void main(String [] args) throws Exception {
    new HMasterCommandLine(HMaster.class).doMain(args);
  }
}
