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
package org.apache.hadoop.hdfs.server.federation;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICE_ID;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_ADMIN_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_CACHE_TIME_TO_LIVE_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_DEFAULT_NAMESERVICE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_MONITOR_NAMENODE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.addDirectory;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.waitNamenodeRegistered;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.NameNodeInfo;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.MiniDFSNNTopology.NNConf;
import org.apache.hadoop.hdfs.MiniDFSNNTopology.NSConf;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.NamenodeStatusReport;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterClient;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service.STATE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test utility to mimic a federated HDFS cluster with multiple routers.
 */
public class RouterDFSCluster {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterDFSCluster.class);

  public static final String TEST_STRING = "teststring";
  public static final String TEST_DIR = "testdir";
  public static final String TEST_FILE = "testfile";


  /** Nameservices in the federated cluster. */
  private List<String> nameservices;
  /** Namenodes in the federated cluster. */
  private List<NamenodeContext> namenodes;
  /** Routers in the federated cluster. */
  private List<RouterContext> routers;
  /** If the Namenodes are in high availability.*/
  private boolean highAvailability;
  /** Number of datanodes per nameservice. */
  private int numDatanodesPerNameservice = 2;

  /** Mini cluster. */
  private MiniDFSCluster cluster;

  protected static final long DEFAULT_HEARTBEAT_INTERVAL_MS =
      TimeUnit.SECONDS.toMillis(5);
  protected static final long DEFAULT_CACHE_INTERVAL_MS =
      TimeUnit.SECONDS.toMillis(5);
  /** Heartbeat interval in milliseconds. */
  private long heartbeatInterval;
  /** Cache flush interval in milliseconds. */
  private long cacheFlushInterval;

  /** Router configuration overrides. */
  private Configuration routerOverrides;
  /** Namenode configuration overrides. */
  private Configuration namenodeOverrides;


  /**
   * Router context.
   */
  public class RouterContext {
    private Router router;
    private FileContext fileContext;
    private String nameserviceId;
    private String namenodeId;
    private int rpcPort;
    private int httpPort;
    private DFSClient client;
    private Configuration conf;
    private RouterClient adminClient;
    private URI fileSystemUri;

    public RouterContext(Configuration conf, String nsId, String nnId) {
      this.conf = conf;
      this.nameserviceId = nsId;
      this.namenodeId = nnId;

      this.router = new Router();
      this.router.init(conf);
    }

    public Router getRouter() {
      return this.router;
    }

    public String getNameserviceId() {
      return this.nameserviceId;
    }

    public String getNamenodeId() {
      return this.namenodeId;
    }

    public int getRpcPort() {
      return this.rpcPort;
    }

    public int getHttpPort() {
      return this.httpPort;
    }

    public FileContext getFileContext() {
      return this.fileContext;
    }

    public void initRouter() throws URISyntaxException {
      // Store the bound points for the router interfaces
      InetSocketAddress rpcAddress = router.getRpcServerAddress();
      if (rpcAddress != null) {
        this.rpcPort = rpcAddress.getPort();
        this.fileSystemUri =
            URI.create("hdfs://" + NetUtils.getHostPortString(rpcAddress));
        // Override the default FS to point to the router RPC
        DistributedFileSystem.setDefaultUri(conf, fileSystemUri);
        try {
          this.fileContext = FileContext.getFileContext(conf);
        } catch (UnsupportedFileSystemException e) {
          this.fileContext = null;
        }
      }
      InetSocketAddress httpAddress = router.getHttpServerAddress();
      if (httpAddress != null) {
        this.httpPort = httpAddress.getPort();
      }
    }

    public FileSystem getFileSystem() throws IOException {
      return DistributedFileSystem.get(conf);
    }

    public DFSClient getClient(UserGroupInformation user)
        throws IOException, URISyntaxException, InterruptedException {

      LOG.info("Connecting to router at {}", fileSystemUri);
      return user.doAs(new PrivilegedExceptionAction<DFSClient>() {
        @Override
        public DFSClient run() throws IOException {
          return new DFSClient(fileSystemUri, conf);
        }
      });
    }

    public RouterClient getAdminClient() throws IOException {
      if (adminClient == null) {
        InetSocketAddress routerSocket = router.getAdminServerAddress();
        LOG.info("Connecting to router admin at {}", routerSocket);
        adminClient = new RouterClient(routerSocket, conf);
      }
      return adminClient;
    }

    public DFSClient getClient() throws IOException, URISyntaxException {
      if (client == null) {
        LOG.info("Connecting to router at {}", fileSystemUri);
        client = new DFSClient(fileSystemUri, conf);
      }
      return client;
    }
  }

  /**
   * Namenode context in the federated cluster.
   */
  public class NamenodeContext {
    private Configuration conf;
    private NameNode namenode;
    private String nameserviceId;
    private String namenodeId;
    private FileContext fileContext;
    private int rpcPort;
    private int servicePort;
    private int lifelinePort;
    private int httpPort;
    private URI fileSystemUri;
    private int index;
    private DFSClient client;

    public NamenodeContext(
        Configuration conf, String nsId, String nnId, int index) {
      this.conf = conf;
      this.nameserviceId = nsId;
      this.namenodeId = nnId;
      this.index = index;
    }

    public NameNode getNamenode() {
      return this.namenode;
    }

    public String getNameserviceId() {
      return this.nameserviceId;
    }

    public String getNamenodeId() {
      return this.namenodeId;
    }

    public FileContext getFileContext() {
      return this.fileContext;
    }

    public void setNamenode(NameNode nn) throws URISyntaxException {
      this.namenode = nn;

      // Store the bound ports and override the default FS with the local NN RPC
      this.rpcPort = nn.getNameNodeAddress().getPort();
      this.servicePort = nn.getServiceRpcAddress().getPort();
      this.lifelinePort = nn.getServiceRpcAddress().getPort();
      this.httpPort = nn.getHttpAddress().getPort();
      this.fileSystemUri = new URI("hdfs://" + namenode.getHostAndPort());
      DistributedFileSystem.setDefaultUri(this.conf, this.fileSystemUri);

      try {
        this.fileContext = FileContext.getFileContext(this.conf);
      } catch (UnsupportedFileSystemException e) {
        this.fileContext = null;
      }
    }

    public String getRpcAddress() {
      return namenode.getNameNodeAddress().getHostName() + ":" + rpcPort;
    }

    public String getServiceAddress() {
      return namenode.getServiceRpcAddress().getHostName() + ":" + servicePort;
    }

    public String getLifelineAddress() {
      return namenode.getServiceRpcAddress().getHostName() + ":" + lifelinePort;
    }

    public String getHttpAddress() {
      return namenode.getHttpAddress().getHostName() + ":" + httpPort;
    }

    public FileSystem getFileSystem() throws IOException {
      return DistributedFileSystem.get(conf);
    }

    public void resetClient() {
      client = null;
    }

    public DFSClient getClient(UserGroupInformation user)
        throws IOException, URISyntaxException, InterruptedException {

      LOG.info("Connecting to namenode at {}", fileSystemUri);
      return user.doAs(new PrivilegedExceptionAction<DFSClient>() {
        @Override
        public DFSClient run() throws IOException {
          return new DFSClient(fileSystemUri, conf);
        }
      });
    }

    public DFSClient getClient() throws IOException, URISyntaxException {
      if (client == null) {
        LOG.info("Connecting to namenode at {}", fileSystemUri);
        client = new DFSClient(fileSystemUri, conf);
      }
      return client;
    }

    public String getConfSuffix() {
      String suffix = nameserviceId;
      if (highAvailability) {
        suffix += "." + namenodeId;
      }
      return suffix;
    }
  }

  public RouterDFSCluster(boolean ha, int numNameservices, int numNamenodes,
      long heartbeatInterval, long cacheFlushInterval) {
    this.highAvailability = ha;
    this.heartbeatInterval = heartbeatInterval;
    this.cacheFlushInterval = cacheFlushInterval;
    configureNameservices(numNameservices, numNamenodes);
  }

  public RouterDFSCluster(boolean ha, int numNameservices) {
    this(ha, numNameservices, 2,
        DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_CACHE_INTERVAL_MS);
  }

  public RouterDFSCluster(boolean ha, int numNameservices, int numNamenodes) {
    this(ha, numNameservices, numNamenodes,
        DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_CACHE_INTERVAL_MS);
  }

  /**
   * Add configuration settings to override default Router settings.
   *
   * @param conf Router configuration overrides.
   */
  public void addRouterOverrides(Configuration conf) {
    if (this.routerOverrides == null) {
      this.routerOverrides = conf;
    } else {
      this.routerOverrides.addResource(conf);
    }
  }

  /**
   * Add configuration settings to override default Namenode settings.
   *
   * @param conf Namenode configuration overrides.
   */
  public void addNamenodeOverrides(Configuration conf) {
    if (this.namenodeOverrides == null) {
      this.namenodeOverrides = conf;
    } else {
      this.namenodeOverrides.addResource(conf);
    }
  }

  /**
   * Generate the configuration for a client.
   *
   * @param nsId Nameservice identifier.
   * @return New namenode configuration.
   */
  public Configuration generateNamenodeConfiguration(String nsId) {
    Configuration conf = new HdfsConfiguration();

    conf.set(DFS_NAMESERVICES, getNameservicesKey());
    conf.set(FS_DEFAULT_NAME_KEY, "hdfs://" + nsId);

    for (String ns : nameservices) {
      if (highAvailability) {
        conf.set(
            DFS_HA_NAMENODES_KEY_PREFIX + "." + ns,
            NAMENODES[0] + "," + NAMENODES[1]);
      }

      for (NamenodeContext context : getNamenodes(ns)) {
        String suffix = context.getConfSuffix();

        conf.set(DFS_NAMENODE_RPC_ADDRESS_KEY + "." + suffix,
            "127.0.0.1:" + context.rpcPort);
        conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY + "." + suffix,
            "127.0.0.1:" + context.httpPort);
        conf.set(DFS_NAMENODE_RPC_BIND_HOST_KEY + "." + suffix,
            "0.0.0.0");

        // If the service port is enabled by default, we need to set them up
        boolean servicePortEnabled = false;
        if (servicePortEnabled) {
          conf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + "." + suffix,
              "127.0.0.1:" + context.servicePort);
          conf.set(DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY + "." + suffix,
              "0.0.0.0");
        }
      }
    }

    if (this.namenodeOverrides != null) {
      conf.addResource(this.namenodeOverrides);
    }
    return conf;
  }

  /**
   * Generate the configuration for a client.
   *
   * @return New configuration for a client.
   */
  public Configuration generateClientConfiguration() {
    Configuration conf = new HdfsConfiguration(false);
    String ns0 = getNameservices().get(0);
    conf.addResource(generateNamenodeConfiguration(ns0));
    return conf;
  }

  /**
   * Generate the configuration for a Router.
   *
   * @param nsId Nameservice identifier.
   * @param nnId Namenode identifier.
   * @return New configuration for a Router.
   */
  public Configuration generateRouterConfiguration(String nsId, String nnId) {

    Configuration conf = new HdfsConfiguration(false);
    conf.addResource(generateNamenodeConfiguration(nsId));

    conf.setInt(DFS_ROUTER_HANDLER_COUNT_KEY, 10);
    conf.set(DFS_ROUTER_RPC_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFS_ROUTER_RPC_BIND_HOST_KEY, "0.0.0.0");

    conf.set(DFS_ROUTER_ADMIN_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFS_ROUTER_ADMIN_BIND_HOST_KEY, "0.0.0.0");

    conf.set(DFS_ROUTER_HTTP_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFS_ROUTER_HTTPS_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFS_ROUTER_HTTP_BIND_HOST_KEY, "0.0.0.0");

    conf.set(DFS_ROUTER_DEFAULT_NAMESERVICE, nameservices.get(0));
    conf.setLong(DFS_ROUTER_HEARTBEAT_INTERVAL_MS, heartbeatInterval);
    conf.setLong(DFS_ROUTER_CACHE_TIME_TO_LIVE_MS, cacheFlushInterval);

    // Use mock resolver classes
    conf.setClass(FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS,
        MockResolver.class, ActiveNamenodeResolver.class);
    conf.setClass(FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
        MockResolver.class, FileSubclusterResolver.class);

    // Set the nameservice ID for the default NN monitor
    conf.set(DFS_NAMESERVICE_ID, nsId);
    if (nnId != null) {
      conf.set(DFS_HA_NAMENODE_ID_KEY, nnId);
    }

    // Namenodes to monitor
    StringBuilder sb = new StringBuilder();
    for (String ns : this.nameservices) {
      for (NamenodeContext context : getNamenodes(ns)) {
        String suffix = context.getConfSuffix();
        if (sb.length() != 0) {
          sb.append(",");
        }
        sb.append(suffix);
      }
    }
    conf.set(DFS_ROUTER_MONITOR_NAMENODE, sb.toString());

    // Add custom overrides if available
    if (this.routerOverrides != null) {
      for (Entry<String, String> entry : this.routerOverrides) {
        String confKey = entry.getKey();
        String confValue = entry.getValue();
        conf.set(confKey, confValue);
      }
    }
    return conf;
  }

  public void configureNameservices(int numNameservices, int numNamenodes) {
    this.nameservices = new ArrayList<>();
    this.namenodes = new ArrayList<>();

    NamenodeContext context = null;
    int nnIndex = 0;
    for (int i=0; i<numNameservices; i++) {
      String ns = "ns" + i;
      this.nameservices.add("ns" + i);

      Configuration nnConf = generateNamenodeConfiguration(ns);
      if (!highAvailability) {
        context = new NamenodeContext(nnConf, ns, null, nnIndex++);
        this.namenodes.add(context);
      } else {
        for (int j=0; j<numNamenodes; j++) {
          context = new NamenodeContext(nnConf, ns, NAMENODES[j], nnIndex++);
          this.namenodes.add(context);
        }
      }
    }
  }

  public void setNumDatanodesPerNameservice(int num) {
    this.numDatanodesPerNameservice = num;
  }

  public String getNameservicesKey() {
    StringBuilder sb = new StringBuilder();
    for (String nsId : this.nameservices) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(nsId);
    }
    return sb.toString();
  }

  public String getRandomNameservice() {
    Random r = new Random();
    int randIndex = r.nextInt(nameservices.size());
    return nameservices.get(randIndex);
  }

  public List<String> getNameservices() {
    return nameservices;
  }

  public List<NamenodeContext> getNamenodes(String nameservice) {
    List<NamenodeContext> nns = new ArrayList<>();
    for (NamenodeContext c : namenodes) {
      if (c.nameserviceId.equals(nameservice)) {
        nns.add(c);
      }
    }
    return nns;
  }

  public NamenodeContext getRandomNamenode() {
    Random rand = new Random();
    int i = rand.nextInt(this.namenodes.size());
    return this.namenodes.get(i);
  }

  public List<NamenodeContext> getNamenodes() {
    return this.namenodes;
  }

  public boolean isHighAvailability() {
    return highAvailability;
  }

  public NamenodeContext getNamenode(String nameservice, String namenode) {
    for (NamenodeContext c : this.namenodes) {
      if (c.nameserviceId.equals(nameservice)) {
        if (namenode == null || namenode.isEmpty() ||
            c.namenodeId == null || c.namenodeId.isEmpty()) {
          return c;
        } else if (c.namenodeId.equals(namenode)) {
          return c;
        }
      }
    }
    return null;
  }

  public List<RouterContext> getRouters(String nameservice) {
    List<RouterContext> nns = new ArrayList<>();
    for (RouterContext c : routers) {
      if (c.nameserviceId.equals(nameservice)) {
        nns.add(c);
      }
    }
    return nns;
  }

  public RouterContext getRouterContext(String nsId, String nnId) {
    for (RouterContext c : routers) {
      if (nnId == null) {
        return c;
      }
      if (c.namenodeId.equals(nnId) &&
          c.nameserviceId.equals(nsId)) {
        return c;
      }
    }
    return null;
  }

  public RouterContext getRandomRouter() {
    Random rand = new Random();
    return routers.get(rand.nextInt(routers.size()));
  }

  public List<RouterContext> getRouters() {
    return routers;
  }

  public RouterContext buildRouter(String nsId, String nnId)
      throws URISyntaxException, IOException {
    Configuration config = generateRouterConfiguration(nsId, nnId);
    RouterContext rc = new RouterContext(config, nsId, nnId);
    return rc;
  }

  public void startCluster() {
    startCluster(null);
  }

  public void startCluster(Configuration overrideConf) {
    try {
      MiniDFSNNTopology topology = new MiniDFSNNTopology();
      for (String ns : nameservices) {
        NSConf conf = new MiniDFSNNTopology.NSConf(ns);
        if (highAvailability) {
          for (int i=0; i<namenodes.size()/nameservices.size(); i++) {
            NNConf nnConf = new MiniDFSNNTopology.NNConf("nn" + i);
            conf.addNN(nnConf);
          }
        } else {
          NNConf nnConf = new MiniDFSNNTopology.NNConf(null);
          conf.addNN(nnConf);
        }
        topology.addNameservice(conf);
      }
      topology.setFederation(true);

      // Start mini DFS cluster
      String ns0 = nameservices.get(0);
      Configuration nnConf = generateNamenodeConfiguration(ns0);
      if (overrideConf != null) {
        nnConf.addResource(overrideConf);
      }
      cluster = new MiniDFSCluster.Builder(nnConf)
          .numDataNodes(nameservices.size() * numDatanodesPerNameservice)
          .nnTopology(topology)
          .build();
      cluster.waitActive();

      // Store NN pointers
      for (int i = 0; i < namenodes.size(); i++) {
        NameNode nn = cluster.getNameNode(i);
        namenodes.get(i).setNamenode(nn);
      }

    } catch (Exception e) {
      LOG.error("Cannot start Router DFS cluster: {}", e.getMessage(), e);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  public void startRouters()
      throws InterruptedException, URISyntaxException, IOException {

    // Create one router per nameservice
    this.routers = new ArrayList<>();
    for (String ns : this.nameservices) {
      for (NamenodeContext context : getNamenodes(ns)) {
        RouterContext router = buildRouter(ns, context.namenodeId);
        this.routers.add(router);
      }
    }

    // Start all routers
    for (RouterContext router : this.routers) {
      router.router.start();
    }

    // Wait until all routers are active and record their ports
    for (RouterContext router : this.routers) {
      waitActive(router);
      router.initRouter();
    }
  }

  public void waitActive(NamenodeContext nn) throws IOException {
    cluster.waitActive(nn.index);
  }

  public void waitActive(RouterContext router)
      throws InterruptedException {
    for (int loopCount = 0; loopCount < 20; loopCount++) {
      // Validate connection of routers to NNs
      if (router.router.getServiceState() == STATE.STARTED) {
        return;
      }
      Thread.sleep(1000);
    }
    fail("Timeout waiting for " + router.router + " to activate");
  }

  public void registerNamenodes() throws IOException {
    for (RouterContext r : this.routers) {
      ActiveNamenodeResolver resolver = r.router.getNamenodeResolver();
      for (NamenodeContext nn : this.namenodes) {
        // Generate a report
        NamenodeStatusReport report = new NamenodeStatusReport(
            nn.nameserviceId, nn.namenodeId,
            nn.getRpcAddress(), nn.getServiceAddress(),
            nn.getLifelineAddress(), nn.getHttpAddress());
        FSImage fsImage = nn.namenode.getNamesystem().getFSImage();
        NamespaceInfo nsInfo = fsImage.getStorage().getNamespaceInfo();
        report.setNamespaceInfo(nsInfo);

        // Determine HA state from nn public state string
        String nnState = nn.namenode.getState();
        HAServiceState haState = HAServiceState.ACTIVE;
        for (HAServiceState state : HAServiceState.values()) {
          if (nnState.equalsIgnoreCase(state.name())) {
            haState = state;
            break;
          }
        }
        report.setHAServiceState(haState);

        // Register with the resolver
        resolver.registerNamenode(report);
      }
    }
  }

  public void waitNamenodeRegistration() throws Exception {
    for (RouterContext r : this.routers) {
      Router router = r.router;
      for (NamenodeContext nn : this.namenodes) {
        ActiveNamenodeResolver nnResolver = router.getNamenodeResolver();
        waitNamenodeRegistered(
            nnResolver, nn.nameserviceId, nn.namenodeId, null);
      }
    }
  }

  public void waitRouterRegistrationQuorum(RouterContext router,
      FederationNamenodeServiceState state, String nsId, String nnId)
          throws Exception {
    LOG.info("Waiting for NN {} {} to transition to {}", nsId, nnId, state);
    ActiveNamenodeResolver nnResolver = router.router.getNamenodeResolver();
    waitNamenodeRegistered(nnResolver, nsId, nnId, state);
  }

  /**
   * Get the federated path for a nameservice.
   * @param nsId Nameservice identifier.
   * @return Path in the Router.
   */
  public String getFederatedPathForNS(String nsId) {
    return "/" + nsId;
  }

  /**
   * Get the namenode path for a nameservice.
   * @param nsId Nameservice identifier.
   * @return Path in the Namenode.
   */
  public String getNamenodePathForNS(String nsId) {
    return "/target-" + nsId;
  }

  /**
   * Get the federated test directory for a nameservice.
   * @param nsId Nameservice identifier.
   * @return Example:
   *         <ul>
   *         <li>/ns0/testdir which maps to ns0->/target-ns0/testdir
   *         </ul>
   */
  public String getFederatedTestDirectoryForNS(String nsId) {
    return getFederatedPathForNS(nsId) + "/" + TEST_DIR;
  }

  /**
   * Get the namenode test directory for a nameservice.
   * @param nsId Nameservice identifier.
   * @return example:
   *         <ul>
   *         <li>/target-ns0/testdir
   *         </ul>
   */
  public String getNamenodeTestDirectoryForNS(String nsId) {
    return getNamenodePathForNS(nsId) + "/" + TEST_DIR;
  }

  /**
   * Get the federated test file for a nameservice.
   * @param nsId Nameservice identifier.
   * @return example:
   *         <ul>
   *         <li>/ns0/testfile which maps to ns0->/target-ns0/testfile
   *         </ul>
   */
  public String getFederatedTestFileForNS(String nsId) {
    return getFederatedPathForNS(nsId) + "/" + TEST_FILE;
  }

  /**
   * Get the namenode test file for a nameservice.
   * @param nsId Nameservice identifier.
   * @return example:
   *         <ul>
   *         <li>/target-ns0/testfile
   *         </ul>
   */
  public String getNamenodeTestFileForNS(String nsId) {
    return getNamenodePathForNS(nsId) + "/" + TEST_FILE;
  }

  /**
   * Switch a namenode in a nameservice to be the active.
   * @param nsId Nameservice identifier.
   * @param nnId Namenode identifier.
   */
  public void switchToActive(String nsId, String nnId) {
    try {
      int total = cluster.getNumNameNodes();
      NameNodeInfo[] nns = cluster.getNameNodeInfos();
      for (int i = 0; i < total; i++) {
        NameNodeInfo nn = nns[i];
        if (nn.getNameserviceId().equals(nsId) &&
            nn.getNamenodeId().equals(nnId)) {
          cluster.transitionToActive(i);
        }
      }
    } catch (Throwable e) {
      LOG.error("Cannot transition to active", e);
    }
  }

  /**
   * Switch a namenode in a nameservice to be in standby.
   * @param nsId Nameservice identifier.
   * @param nnId Namenode identifier.
   */
  public void switchToStandby(String nsId, String nnId) {
    try {
      int total = cluster.getNumNameNodes();
      NameNodeInfo[] nns = cluster.getNameNodeInfos();
      for (int i = 0; i < total; i++) {
        NameNodeInfo nn = nns[i];
        if (nn.getNameserviceId().equals(nsId) &&
            nn.getNamenodeId().equals(nnId)) {
          cluster.transitionToStandby(i);
        }
      }
    } catch (Throwable e) {
      LOG.error("Cannot transition to standby", e);
    }
  }

  /**
   * Stop the federated HDFS cluster.
   */
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    if (routers != null) {
      for (RouterContext context : routers) {
        stopRouter(context);
      }
    }
  }

  /**
   * Stop a router.
   * @param router Router context.
   */
  public void stopRouter(RouterContext router) {
    try {
      router.router.shutDown();

      int loopCount = 0;
      while (router.router.getServiceState() != STATE.STOPPED) {
        loopCount++;
        Thread.sleep(1000);
        if (loopCount > 20) {
          LOG.error("Cannot shutdown router {}", router.rpcPort);
          break;
        }
      }
    } catch (InterruptedException e) {
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Namespace Test Fixtures
  /////////////////////////////////////////////////////////////////////////////

  /**
   * Creates test directories via the namenode.
   * 1) /target-ns0/testfile
   * 2) /target-ns1/testfile
   * @throws IOException
   */
  public void createTestDirectoriesNamenode() throws IOException {
    // Add a test dir to each NS and verify
    for (String ns : getNameservices()) {
      NamenodeContext context = getNamenode(ns, null);
      if (!createTestDirectoriesNamenode(context)) {
        throw new IOException("Cannot create test directory for ns " + ns);
      }
    }
  }

  public boolean createTestDirectoriesNamenode(NamenodeContext nn)
      throws IOException {
    FileSystem fs = nn.getFileSystem();
    String testDir = getNamenodeTestDirectoryForNS(nn.nameserviceId);
    return addDirectory(fs, testDir);
  }

  public void deleteAllFiles() throws IOException {
    // Delete all files via the NNs and verify
    for (NamenodeContext context : getNamenodes()) {
      FileSystem fs = context.getFileSystem();
      FileStatus[] status = fs.listStatus(new Path("/"));
      for (int i = 0; i <status.length; i++) {
        Path p = status[i].getPath();
        fs.delete(p, true);
      }
      status = fs.listStatus(new Path("/"));
      assertEquals(status.length, 0);
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // MockRouterResolver Test Fixtures
  /////////////////////////////////////////////////////////////////////////////

  /**
   * <ul>
   * <li>/ -> [ns0->/].
   * <li>/nso -> ns0->/target-ns0.
   * <li>/ns1 -> ns1->/target-ns1.
   * </ul>
   */
  public void installMockLocations() {
    for (RouterContext r : routers) {
      MockResolver resolver =
          (MockResolver) r.router.getSubclusterResolver();
      // create table entries
      for (String nsId : nameservices) {
        // Direct path
        String routerPath = getFederatedPathForNS(nsId);
        String nnPath = getNamenodePathForNS(nsId);
        resolver.addLocation(routerPath, nsId, nnPath);
      }

      // Root path points to both first nameservice
      String ns0 = nameservices.get(0);
      resolver.addLocation("/", ns0, "/");
    }
  }

  public MiniDFSCluster getCluster() {
    return cluster;
  }

  /**
   * Wait until the federated cluster is up and ready.
   * @throws IOException If we cannot wait for the cluster to be up.
   */
  public void waitClusterUp() throws IOException {
    cluster.waitClusterUp();
    registerNamenodes();
    try {
      waitNamenodeRegistration();
    } catch (Exception e) {
      throw new IOException("Cannot wait for the namenodes", e);
    }
  }
}
