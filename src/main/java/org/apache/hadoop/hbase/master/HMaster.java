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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorService.ExecutorType;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HMasterRegionInterface;
import org.apache.hadoop.hbase.master.LoadBalancer.RegionPlan;
import org.apache.hadoop.hbase.master.handler.DeleteTableHandler;
import org.apache.hadoop.hbase.master.handler.DisableTableHandler;
import org.apache.hadoop.hbase.master.handler.EnableTableHandler;
import org.apache.hadoop.hbase.master.handler.ModifyTableHandler;
import org.apache.hadoop.hbase.master.handler.TableAddFamilyHandler;
import org.apache.hadoop.hbase.master.handler.TableDeleteFamilyHandler;
import org.apache.hadoop.hbase.master.handler.TableModifyFamilyHandler;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.RegionServerTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
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
  private final HBaseServer rpcServer;
  // Address of the HMaster
  private final HServerAddress address;
  // file system manager for the master FS operations
  private final MasterFileSystem fileSystemManager;

  private final HConnection connection;

  // server manager to deal with region server info
  private final ServerManager serverManager;

  // manager of assignment nodes in zookeeper
  final AssignmentManager assignmentManager;
  // manager of catalog regions
  private final CatalogTracker catalogTracker;
  // Cluster status zk tracker and local setter
  private ClusterStatusTracker clusterStatusTracker;

  // True if this is the master that started the cluster.
  boolean clusterStarter;

  // This flag is for stopping this Master instance.
  private boolean stopped = false;
  // Set on abort -- usually failure of our zk session
  private volatile boolean abort = false;

  // Instance of the hbase executor service.
  ExecutorService executorService;

  private LoadBalancer balancer = new LoadBalancer();
  private Chore balancerChore;
  private volatile boolean balance = true;

  /**
   * Initializes the HMaster. The steps are as follows:
   *
   * <ol>
   * <li>Initialize HMaster RPC and address
   * <li>Connect to ZooKeeper and figure out if this is a fresh cluster start or
   *     a failed over master
   * <li>Block until becoming active master
   * <li>Initialize master components - server manager, region manager,
   *     region server queue, file system manager, etc
   * </ol>
   * @throws InterruptedException 
   */
  public HMaster(final Configuration conf)
  throws IOException, KeeperException, InterruptedException {
    this.conf = conf;
    /*
     * 1. Determine address and initialize RPC server (but do not start).
     * The RPC server ports can be ephemeral.
     */
    HServerAddress a = new HServerAddress(getMyAddress(this.conf));
    int numHandlers = conf.getInt("hbase.regionserver.handler.count", 10);
    this.rpcServer = HBaseRPC.getServer(this, a.getBindAddress(), a.getPort(),
      numHandlers, false, conf);
    this.address = new HServerAddress(rpcServer.getListenerAddress());

    // set the thread name now we have an address
    setName(MASTER + "-" + this.address);

    // Hack! Maps DFSClient => Master for logs.  HDFS made this 
    // config param for task trackers, but we can piggyback off of it.
    if (this.conf.get("mapred.task.id") == null) {
      this.conf.set("mapred.task.id", "hb_m_" + this.address.toString() +
        "_" + System.currentTimeMillis());
    }

    /*
     * 2. Determine if this is a fresh cluster startup or failed over master.
     * This is done by checking for the existence of any ephemeral
     * RegionServer nodes in ZooKeeper.  These nodes are created by RSs on
     * their initialization but only after they find the primary master.  As
     * long as this check is done before we write our address into ZK, this
     * will work.  Note that multiple masters could find this to be true on
     * startup (none have become active master yet), which is why there is an
     * additional check if this master does not become primary on its first attempt.
     */
    this.zooKeeper =
      new ZooKeeperWatcher(conf, MASTER + "-" + getMasterAddress(), this);
    this.clusterStarter = 0 ==
      ZKUtil.getNumberOfChildren(zooKeeper, zooKeeper.rsZNode);

    /*
     * 3. Block on becoming the active master.
     * We race with other masters to write our address into ZooKeeper.  If we
     * succeed, we are the primary/active master and finish initialization.
     *
     * If we do not succeed, there is another active master and we should
     * now wait until it dies to try and become the next active master.  If we
     * do not succeed on our first attempt, this is no longer a cluster startup.
     */
    activeMasterManager = new ActiveMasterManager(zooKeeper, address, this);
    zooKeeper.registerListener(activeMasterManager);

    // Wait here until we are the active master
    clusterStarter = activeMasterManager.blockUntilBecomingActiveMaster();

    /**
     * 4. We are active master now... go initialize components we need to run.
     */
    // TODO: Do this using Dependency Injection, using PicoContainer or Spring.
    this.fileSystemManager = new MasterFileSystem(this);
    this.connection = HConnectionManager.getConnection(conf);
    this.executorService = new ExecutorService(getServerName());

    this.serverManager = new ServerManager(this, this);

    this.catalogTracker = new CatalogTracker(this.zooKeeper, this.connection,
      this, conf.getInt("hbase.master.catalog.timeout", -1));
    this.catalogTracker.start();

    this.assignmentManager = new AssignmentManager(this, serverManager,
      this.catalogTracker, this.executorService);
    zooKeeper.registerListener(assignmentManager);

    this.regionServerTracker = new RegionServerTracker(zooKeeper, this,
      this.serverManager);
    regionServerTracker.start();

    // Set the cluster as up.
    this.clusterStatusTracker = new ClusterStatusTracker(getZooKeeper(), this);
    this.clusterStatusTracker.setClusterUp();
    this.clusterStatusTracker.start();

    LOG.info("Server active/primary master; " + this.address +
      "; clusterStarter=" + this.clusterStarter + ", sessionid=0x" +
      Long.toHexString(this.zooKeeper.getZooKeeper().getSessionId()));
  }

  /**
   * Main processing loop for the HMaster.
   * 1. Handle both fresh cluster start as well as failed over initialization of
   *    the HMaster.
   * 2. Start the necessary services
   * 3. Reassign the root region
   * 4. The master is no longer closed - set "closed" to false
   */
  @Override
  public void run() {
    try {
      // start up all service threads.
      startServiceThreads();
      // wait for minimum number of region servers to be up
      this.serverManager.waitForMinServers();
      // start assignment of user regions, startup or failure
      if (this.clusterStarter) {
        clusterStarterInitializations(this.fileSystemManager,
            this.serverManager, this.catalogTracker, this.assignmentManager);
      } else {
        // Process existing unassigned nodes in ZK, read all regions from META,
        // rebuild in-memory state.
        this.assignmentManager.processFailover();
      }
      // Check if we should stop every second.
      Sleeper sleeper = new Sleeper(1000, this);
      while (!this.stopped  && !this.abort) {
        sleeper.sleep();
      }
    } catch (Throwable t) {
      abort("Unhandled exception. Starting shutdown.", t);
    }

    // Wait for all the remaining region servers to report in IFF we were
    // running a cluster shutdown AND we were NOT aborting.
    if (!this.abort && this.serverManager.isClusterShutdown()) {
      this.serverManager.letRegionServersShutdown();
    }

    // Clean up and close up shop
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    this.rpcServer.stop();
    if (this.balancerChore != null) this.balancerChore.interrupt();
    this.activeMasterManager.stop();
    this.executorService.shutdown();
    HConnectionManager.deleteConnection(this.conf, true);
    this.zooKeeper.close();
    LOG.info("HMaster main thread exiting");
  }

  /*
   * Initializations we need to do if we are cluster starter.
   * @param starter
   * @param mfs
   * @throws IOException 
   */
  private static void clusterStarterInitializations(final MasterFileSystem mfs,
    final ServerManager sm, final CatalogTracker ct, final AssignmentManager am)
  throws IOException, InterruptedException, KeeperException {
      // This master is starting the cluster (its not a preexisting cluster
      // that this master is joining).
      // Initialize the filesystem, which does the following:
      //   - Creates the root hbase directory in the FS if DNE
      //   - If fresh start, create first ROOT and META regions (bootstrap)
      //   - Checks the FS to make sure the root directory is readable
      //   - Creates the archive directory for logs
      mfs.initialize();
      // Do any log splitting necessary
      // TODO: Should do this in background rather than block master startup
      // TODO: Do we want to do this before/while/after RSs check in?
      //       It seems that this method looks at active RSs but happens
      //       concurrently with when we expect them to be checking in
      mfs.splitLogAfterStartup(sm.getOnlineServers());
      // Clean out current state of unassigned
      am.cleanoutUnassigned();
      // assign the root region
      am.assignRoot();
      ct.waitForRoot();
      // assign the meta region
      am.assignMeta();
      ct.waitForMeta();
      // above check waits for general meta availability but this does not
      // guarantee that the transition has completed
      am.waitForAssignment(HRegionInfo.FIRST_META_REGIONINFO);
      am.assignAllUserRegions();
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
    return HBaseRPCProtocolVersion.versionID;
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
  private void startServiceThreads() {
    try {
      // Start the executor service pools
      this.executorService.startExecutorService(ExecutorType.MASTER_OPEN_REGION,
        conf.getInt("hbase.master.executor.openregion.threads", 5));
      this.executorService.startExecutorService(ExecutorType.MASTER_CLOSE_REGION,
        conf.getInt("hbase.master.executor.closeregion.threads", 5));
      this.executorService.startExecutorService(ExecutorType.MASTER_SERVER_OPERATIONS,
        conf.getInt("hbase.master.executor.serverops.threads", 5));
      this.executorService.startExecutorService(ExecutorType.MASTER_TABLE_OPERATIONS,
        conf.getInt("hbase.master.executor.tableops.threads", 5));

      // Put up info server.
      int port = this.conf.getInt("hbase.master.info.port", 60010);
      if (port >= 0) {
        String a = this.conf.get("hbase.master.info.bindAddress", "0.0.0.0");
        this.infoServer = new InfoServer(MASTER, a, port, false);
        this.infoServer.setAttribute(MASTER, this);
        this.infoServer.start();
      }
      this.balancerChore = getAndStartBalancerChore(this);

      // Start the server last so everything else is running before we start
      // receiving requests.
      this.rpcServer.start();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Started service threads");
      }
    } catch (IOException e) {
      if (e instanceof RemoteException) {
        try {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        } catch (IOException ex) {
          LOG.warn("thread start", ex);
        }
      }
      // Something happened during startup. Shut things down.
      abort("Failed startup", e);
    }
  }

  private static Chore getAndStartBalancerChore(final HMaster master) {
    String name = master.getServerName() + "-balancerChore";
    int period = master.getConfiguration().
      getInt("hbase.master.balancer.period", 3000000);
    // Start up the load balancer chore
    Chore chore = new Chore(name, period, master) {
      @Override
      protected void chore() {
        master.balance();
      }
    };
    Threads.setDaemonThreadRunning(chore, name);
    return chore;
  }

  public MapWritable regionServerStartup(final HServerInfo serverInfo)
  throws IOException {
    // Set the ip into the passed in serverInfo.  Its ip is more than likely
    // not the ip that the master sees here.  See at end of this method where
    // we pass it back to the regionserver by setting "hbase.regionserver.address"
    // Everafter, the HSI combination 'server name' is what uniquely identifies
    // the incoming RegionServer.  No more DNS meddling of this little messing
    // belose.
    String rsAddress = HBaseServer.getRemoteAddress();
    serverInfo.setServerAddress(new HServerAddress(rsAddress,
      serverInfo.getServerAddress().getPort()));
    // Register with server manager
    this.serverManager.regionServerStartup(serverInfo);
    // Send back some config info
    MapWritable mw = createConfigurationSubset();
     mw.put(new Text("hbase.regionserver.address"), new Text(rsAddress));
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
   * Run the balancer.
   * @return True if balancer ran, false otherwise.
   */
  public boolean balance() {
    // If balance not true, don't run balancer.
    if (!this.balance) return false;
    synchronized (this.balancer) {
      // Only allow one balance run at at time.
      if (this.assignmentManager.isRegionsInTransition()) {
        LOG.debug("Not running balancer because regions in transition: " +
          this.assignmentManager.getRegionsInTransition());
        return false;
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
      if (plans != null && !plans.isEmpty()) {
        for (RegionPlan plan: plans) {
          this.assignmentManager.balance(plan);
        }
      }
    }
    return true;
  }

  @Override
  public boolean balance(final boolean b) {
    boolean oldValue = this.balance;
    this.balance = b;
    LOG.info("Balance=" + b);
    return oldValue;
  }

  @Override
  public void move(final byte[] encodedRegionName, final byte[] destServerName)
  throws UnknownRegionException {
    Pair<HRegionInfo, HServerInfo> p =
      this.assignmentManager.getAssignment(encodedRegionName);
    if (p == null) throw new UnknownRegionException(Bytes.toString(encodedRegionName));
    HServerInfo dest = this.serverManager.getServerInfo(new String(destServerName));
    RegionPlan rp = new RegionPlan(p.getFirst(), p.getSecond(), dest);
    this.assignmentManager.balance(rp);
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
      boolean sync)
  throws IOException {
    String tableName = newRegions[0].getTableDesc().getNameAsString();
    if(MetaReader.tableExists(catalogTracker, tableName)) {
      throw new TableExistsException(tableName);
    }
    for(HRegionInfo newRegion : newRegions) {
      // 1. Create HRegion
      HRegion region = HRegion.createHRegion(newRegion,
          fileSystemManager.getRootDir(), conf);

      // 2. Insert into META
      MetaEditor.addRegionToMeta(catalogTracker, region.getRegionInfo());

      // 3. Close the new region to flush to disk.  Close log file too.
      region.close();
      region.getLog().closeAndDelete();

      // 4. Trigger immediate assignment of this region
      assignmentManager.assign(region.getRegionInfo());
    }

    // 5. If sync, wait for assignment of regions
    if(sync) {
      LOG.debug("Waiting for " + newRegions.length + " region(s) to be " +
          "assigned before returning");
      for(HRegionInfo regionInfo : newRegions) {
        try {
          assignmentManager.waitForAssignment(regionInfo);
        } catch (InterruptedException e) {
          LOG.info("Interrupted waiting for region to be assigned during " +
              "create table call");
          return;
        }
      }
    }
  }

  private static boolean isCatalogTable(final byte [] tableName) {
    return Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME) ||
           Bytes.equals(tableName, HConstants.META_TABLE_NAME);
  }

  public void deleteTable(final byte [] tableName) throws IOException {
    new DeleteTableHandler(tableName, this, this).process();
  }

  public void addColumn(byte [] tableName, HColumnDescriptor column)
  throws IOException {
    new TableAddFamilyHandler(tableName, column, this, this).process();
  }

  public void modifyColumn(byte [] tableName, HColumnDescriptor descriptor)
  throws IOException {
    new TableModifyFamilyHandler(tableName, descriptor, this, this).process();
  }

  public void deleteColumn(final byte [] tableName, final byte [] c)
  throws IOException {
    new TableDeleteFamilyHandler(tableName, c, this, this).process();
  }

  public void enableTable(final byte [] tableName) throws IOException {
    new EnableTableHandler(this, tableName, catalogTracker, assignmentManager)
      .process();
  }

  public void disableTable(final byte [] tableName) throws IOException {
    new DisableTableHandler(this, tableName, catalogTracker, assignmentManager)
      .process();
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
    this.executorService.submit(new ModifyTableHandler(tableName, htd, this, this));
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
    if (!getAssignmentManager().isTableDisabled(Bytes.toString(tableName))) {
      throw new TableNotDisabledException(tableName);
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
    return status;
  }

  private static void printUsageAndExit() {
    System.err.println("Usage: Master [opts] start|stop");
    System.err.println(" start  Start Master. If local mode, start Master and RegionServer in same JVM");
    System.err.println(" stop   Start cluster shutdown; Master signals RegionServer shutdown");
    System.err.println(" where [opts] are:");
    System.err.println("   --minServers=<servers>    Minimum RegionServers needed to host user tables.");
    System.exit(0);
  }

  @Override
  public void abort(final String msg, final Throwable t) {
    if (t != null) LOG.fatal(msg, t);
    else LOG.fatal(msg);
    this.abort = true;
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return zooKeeper;
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
    this.serverManager.shutdownCluster();
    try {
      this.clusterStatusTracker.setClusterDown();
    } catch (KeeperException e) {
      LOG.error("ZooKeeper exception trying to set cluster as down in ZK", e);
    }
  }

  @Override
  public void stopMaster() {
    stop("Stopped by " + Thread.currentThread().getName());
  }

  @Override
  public void stop(final String why) {
    LOG.info(why);
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  public void assignRegion(HRegionInfo hri) {
    assignmentManager.assign(hri);
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

  /*
   * Version of master that will shutdown the passed zk cluster on its way out.
   */
  static class LocalHMaster extends HMaster {
    private MiniZooKeeperCluster zkcluster = null;

    public LocalHMaster(Configuration conf)
    throws IOException, KeeperException, InterruptedException {
      super(conf);
    }

    @Override
    public void run() {
      super.run();
      if (this.zkcluster != null) {
        try {
          this.zkcluster.shutdown();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    void setZKCluster(final MiniZooKeeperCluster zkcluster) {
      this.zkcluster = zkcluster;
    }
  }

  protected static void doMain(String [] args,
      Class<? extends HMaster> masterClass) {
    Configuration conf = HBaseConfiguration.create();

    Options opt = new Options();
    opt.addOption("minServers", true, "Minimum RegionServers needed to host user tables");
    opt.addOption("D", true, "Override HBase Configuration Settings");
    opt.addOption("backup", false, "Do not try to become HMaster until the primary fails");
    try {
      CommandLine cmd = new GnuParser().parse(opt, args);

      if (cmd.hasOption("minServers")) {
        String val = cmd.getOptionValue("minServers");
        conf.setInt("hbase.regions.server.count.min",
            Integer.valueOf(val));
        LOG.debug("minServers set to " + val);
      }

      if (cmd.hasOption("D")) {
        for (String confOpt : cmd.getOptionValues("D")) {
          String[] kv = confOpt.split("=", 2);
          if (kv.length == 2) {
            conf.set(kv[0], kv[1]);
            LOG.debug("-D configuration override: " + kv[0] + "=" + kv[1]);
          } else {
            throw new ParseException("-D option format invalid: " + confOpt);
          }
        }
      }
      
      // check if we are the backup master - override the conf if so
      if (cmd.hasOption("backup")) {
        conf.setBoolean(HConstants.MASTER_TYPE_BACKUP, true);
      }

      if (cmd.getArgList().contains("start")) {
        try {
          // Print out vm stats before starting up.
          RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
          if (runtime != null) {
            LOG.info("vmName=" + runtime.getVmName() + ", vmVendor=" +
              runtime.getVmVendor() + ", vmVersion=" + runtime.getVmVersion());
            LOG.info("vmInputArguments=" + runtime.getInputArguments());
          }
          // If 'local', defer to LocalHBaseCluster instance.  Starts master
          // and regionserver both in the one JVM.
          if (LocalHBaseCluster.isLocal(conf)) {
            final MiniZooKeeperCluster zooKeeperCluster =
              new MiniZooKeeperCluster();
            File zkDataPath = new File(conf.get("hbase.zookeeper.property.dataDir"));
            int zkClientPort = conf.getInt("hbase.zookeeper.property.clientPort", 0);
            if (zkClientPort == 0) {
              throw new IOException("No config value for hbase.zookeeper.property.clientPort");
            }
            zooKeeperCluster.setTickTime(conf.getInt("hbase.zookeeper.property.tickTime", 3000));
            zooKeeperCluster.setClientPort(zkClientPort);
            int clientPort = zooKeeperCluster.startup(zkDataPath);
            if (clientPort != zkClientPort) {
              String errorMsg = "Couldnt start ZK at requested address of " +
                  zkClientPort + ", instead got: " + clientPort + ". Aborting. Why? " +
                  "Because clients (eg shell) wont be able to find this ZK quorum";
              System.err.println(errorMsg);
              throw new IOException(errorMsg);
            }
            conf.set("hbase.zookeeper.property.clientPort",
              Integer.toString(clientPort));
            // Need to have the zk cluster shutdown when master is shutdown.
            // Run a subclass that does the zk cluster shutdown on its way out.
            LocalHBaseCluster cluster = new LocalHBaseCluster(conf, 1,
              LocalHMaster.class, HRegionServer.class);
            ((LocalHMaster)cluster.getMaster()).setZKCluster(zooKeeperCluster);
            cluster.startup();
          } else {
            HMaster master = constructMaster(masterClass, conf);
            if (master.isStopped()) {
              LOG.info("Won't bring the Master up as a shutdown is requested");
              return;
            }
            master.start();
          }
        } catch (Throwable t) {
          LOG.error("Failed to start master", t);
          System.exit(-1);
        }
      } else if (cmd.getArgList().contains("stop")) {
        HBaseAdmin adm = null;
        try {
          adm = new HBaseAdmin(conf);
        } catch (MasterNotRunningException e) {
          LOG.error("Master not running");
          System.exit(0);
        } catch (ZooKeeperConnectionException e) {
          LOG.error("ZooKeeper not available");
          System.exit(0);
        }
        try {
          adm.shutdown();
        } catch (Throwable t) {
          LOG.error("Failed to stop master", t);
          System.exit(-1);
        }
      } else {
        throw new ParseException("Unknown argument(s): " +
            org.apache.commons.lang.StringUtils.join(cmd.getArgs(), " "));
      }
    } catch (ParseException e) {
      LOG.error("Could not parse: ", e);
      printUsageAndExit();
    }
  }

  /**
   * Main program
   * @param args
   * @throws IOException 
   */
  public static void main(String [] args) throws IOException {
    doMain(args, HMaster.class);
  }
}
