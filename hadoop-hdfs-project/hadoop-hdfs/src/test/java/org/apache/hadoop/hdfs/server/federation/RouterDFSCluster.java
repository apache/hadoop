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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.MiniDFSNNTopology.NNConf;
import org.apache.hadoop.hdfs.MiniDFSNNTopology.NSConf;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.resolver.NamenodeStatusReport;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service.STATE;

/**
 * Test utility to mimic a federated HDFS cluster with a router.
 */
public class RouterDFSCluster {
  /**
   * Router context.
   */
  public class RouterContext {
    private Router router;
    private FileContext fileContext;
    private String nameserviceId;
    private String namenodeId;
    private int rpcPort;
    private DFSClient client;
    private Configuration conf;
    private URI fileSystemUri;

    public RouterContext(Configuration conf, String ns, String nn)
        throws URISyntaxException {
      this.namenodeId = nn;
      this.nameserviceId = ns;
      this.conf = conf;
      router = new Router();
      router.init(conf);
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

    public FileContext getFileContext() {
      return this.fileContext;
    }

    public void initRouter() throws URISyntaxException {
    }

    public DistributedFileSystem getFileSystem() throws IOException {
      DistributedFileSystem fs =
          (DistributedFileSystem) DistributedFileSystem.get(conf);
      return fs;
    }

    public DFSClient getClient(UserGroupInformation user)
        throws IOException, URISyntaxException, InterruptedException {

      LOG.info("Connecting to router at " + fileSystemUri);
      return user.doAs(new PrivilegedExceptionAction<DFSClient>() {
        @Override
        public DFSClient run() throws IOException {
          return new DFSClient(fileSystemUri, conf);
        }
      });
    }

    public DFSClient getClient() throws IOException, URISyntaxException {

      if (client == null) {
        LOG.info("Connecting to router at " + fileSystemUri);
        client = new DFSClient(fileSystemUri, conf);
      }
      return client;
    }
  }

  /**
   * Namenode context.
   */
  public class NamenodeContext {
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
    private Configuration conf;
    private DFSClient client;

    public NamenodeContext(Configuration conf, String ns, String nn,
        int index) {
      this.conf = conf;
      this.namenodeId = nn;
      this.nameserviceId = ns;
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

    public void setNamenode(NameNode n) throws URISyntaxException {
      namenode = n;

      // Store the bound ports and override the default FS with the local NN's
      // RPC
      rpcPort = n.getNameNodeAddress().getPort();
      servicePort = n.getServiceRpcAddress().getPort();
      lifelinePort = n.getServiceRpcAddress().getPort();
      httpPort = n.getHttpAddress().getPort();
      fileSystemUri = new URI("hdfs://" + namenode.getHostAndPort());
      DistributedFileSystem.setDefaultUri(conf, fileSystemUri);

      try {
        this.fileContext = FileContext.getFileContext(conf);
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

    public DistributedFileSystem getFileSystem() throws IOException {
      DistributedFileSystem fs =
          (DistributedFileSystem) DistributedFileSystem.get(conf);
      return fs;
    }

    public void resetClient() {
      client = null;
    }

    public DFSClient getClient(UserGroupInformation user)
        throws IOException, URISyntaxException, InterruptedException {

      LOG.info("Connecting to namenode at " + fileSystemUri);
      return user.doAs(new PrivilegedExceptionAction<DFSClient>() {
        @Override
        public DFSClient run() throws IOException {
          return new DFSClient(fileSystemUri, conf);
        }
      });
    }

    public DFSClient getClient() throws IOException, URISyntaxException {
      if (client == null) {
        LOG.info("Connecting to namenode at " + fileSystemUri);
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

  public static final String NAMENODE1 = "nn0";
  public static final String NAMENODE2 = "nn1";
  public static final String NAMENODE3 = "nn2";
  public static final String TEST_STRING = "teststring";
  public static final String TEST_DIR = "testdir";
  public static final String TEST_FILE = "testfile";

  private List<String> nameservices;
  private List<RouterContext> routers;
  private List<NamenodeContext> namenodes;
  private static final Log LOG = LogFactory.getLog(RouterDFSCluster.class);
  private MiniDFSCluster cluster;
  private boolean highAvailability;

  protected static final int DEFAULT_HEARTBEAT_INTERVAL = 5;
  protected static final int DEFAULT_CACHE_INTERVAL_SEC = 5;
  private Configuration routerOverrides;
  private Configuration namenodeOverrides;

  private static final String NAMENODES = NAMENODE1 + "," + NAMENODE2;

  public RouterDFSCluster(boolean ha, int numNameservices) {
    this(ha, numNameservices, 2);
  }

  public RouterDFSCluster(boolean ha, int numNameservices, int numNamenodes) {
    this.highAvailability = ha;
    configureNameservices(numNameservices, numNamenodes);
  }

  public void addRouterOverrides(Configuration conf) {
    if (this.routerOverrides == null) {
      this.routerOverrides = conf;
    } else {
      this.routerOverrides.addResource(conf);
    }
  }

  public void addNamenodeOverrides(Configuration conf) {
    if (this.namenodeOverrides == null) {
      this.namenodeOverrides = conf;
    } else {
      this.namenodeOverrides.addResource(conf);
    }
  }

  public Configuration generateNamenodeConfiguration(
      String defaultNameserviceId) {
    Configuration c = new HdfsConfiguration();

    c.set(DFSConfigKeys.DFS_NAMESERVICES, getNameservicesKey());
    c.set("fs.defaultFS", "hdfs://" + defaultNameserviceId);

    for (String ns : nameservices) {
      if (highAvailability) {
        c.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + ns, NAMENODES);
      }

      for (NamenodeContext context : getNamenodes(ns)) {
        String suffix = context.getConfSuffix();

        c.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + suffix,
            "127.0.0.1:" + context.rpcPort);
        c.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY + "." + suffix,
            "127.0.0.1:" + context.httpPort);
        c.set(DFSConfigKeys.DFS_NAMENODE_RPC_BIND_HOST_KEY + "." + suffix,
            "0.0.0.0");
      }
    }

    if (namenodeOverrides != null) {
      c.addResource(namenodeOverrides);
    }
    return c;
  }

  public Configuration generateClientConfiguration() {
    Configuration conf = new HdfsConfiguration();
    conf.addResource(generateNamenodeConfiguration(getNameservices().get(0)));
    return conf;
  }

  public Configuration generateRouterConfiguration(String localNameserviceId,
      String localNamenodeId) throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.addResource(generateNamenodeConfiguration(localNameserviceId));

    // Use mock resolver classes
    conf.set(DFSConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS,
        MockResolver.class.getCanonicalName());
    conf.set(DFSConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
        MockResolver.class.getCanonicalName());

    // Set the nameservice ID for the default NN monitor
    conf.set(DFSConfigKeys.DFS_NAMESERVICE_ID, localNameserviceId);

    if (localNamenodeId != null) {
      conf.set(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY, localNamenodeId);
    }

    StringBuilder routerBuilder = new StringBuilder();
    for (String ns : nameservices) {
      for (NamenodeContext context : getNamenodes(ns)) {
        String suffix = context.getConfSuffix();

        if (routerBuilder.length() != 0) {
          routerBuilder.append(",");
        }
        routerBuilder.append(suffix);
      }
    }

    return conf;
  }

  public void configureNameservices(int numNameservices, int numNamenodes) {
    nameservices = new ArrayList<String>();
    for (int i = 0; i < numNameservices; i++) {
      nameservices.add("ns" + i);
    }
    namenodes = new ArrayList<NamenodeContext>();
    int index = 0;
    for (String ns : nameservices) {
      Configuration nnConf = generateNamenodeConfiguration(ns);
      if (highAvailability) {
        NamenodeContext context =
            new NamenodeContext(nnConf, ns, NAMENODE1, index);
        namenodes.add(context);
        index++;

        if (numNamenodes > 1) {
          context = new NamenodeContext(nnConf, ns, NAMENODE2, index + 1);
          namenodes.add(context);
          index++;
        }

        if (numNamenodes > 2) {
          context = new NamenodeContext(nnConf, ns, NAMENODE3, index + 1);
          namenodes.add(context);
          index++;
        }

      } else {
        NamenodeContext context = new NamenodeContext(nnConf, ns, null, index);
        namenodes.add(context);
        index++;
      }
    }
  }

  public String getNameservicesKey() {
    StringBuilder ns = new StringBuilder();
    for (int i = 0; i < nameservices.size(); i++) {
      if (i > 0) {
        ns.append(",");
      }
      ns.append(nameservices.get(i));
    }
    return ns.toString();
  }

  public String getRandomNameservice() {
    Random r = new Random();
    return nameservices.get(r.nextInt(nameservices.size()));
  }

  public List<String> getNameservices() {
    return nameservices;
  }

  public List<NamenodeContext> getNamenodes(String nameservice) {
    ArrayList<NamenodeContext> nns = new ArrayList<NamenodeContext>();
    for (NamenodeContext c : namenodes) {
      if (c.nameserviceId.equals(nameservice)) {
        nns.add(c);
      }
    }
    return nns;
  }

  public NamenodeContext getRandomNamenode() {
    Random rand = new Random();
    return namenodes.get(rand.nextInt(namenodes.size()));
  }

  public List<NamenodeContext> getNamenodes() {
    return namenodes;
  }

  public boolean isHighAvailability() {
    return highAvailability;
  }

  public NamenodeContext getNamenode(String nameservice,
      String namenode) {
    for (NamenodeContext c : namenodes) {
      if (c.nameserviceId.equals(nameservice)) {
        if (namenode == null || c.namenodeId == null || namenode.isEmpty()
            || c.namenodeId.isEmpty()) {
          return c;
        } else if (c.namenodeId.equals(namenode)) {
          return c;
        }
      }
    }
    return null;
  }

  public List<RouterContext> getRouters(String nameservice) {
    ArrayList<RouterContext> nns = new ArrayList<RouterContext>();
    for (RouterContext c : routers) {
      if (c.nameserviceId.equals(nameservice)) {
        nns.add(c);
      }
    }
    return nns;
  }

  public RouterContext getRouterContext(String nameservice,
      String namenode) {
    for (RouterContext c : routers) {
      if (namenode == null) {
        return c;
      }
      if (c.namenodeId.equals(namenode)
          && c.nameserviceId.equals(nameservice)) {
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

  public RouterContext buildRouter(String nameservice, String namenode)
      throws URISyntaxException, IOException {
    Configuration config = generateRouterConfiguration(nameservice, namenode);
    RouterContext rc = new RouterContext(config, nameservice, namenode);
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
          for(int i = 0; i < namenodes.size()/nameservices.size(); i++) {
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
      Configuration nnConf = generateNamenodeConfiguration(nameservices.get(0));
      if (overrideConf != null) {
        nnConf.addResource(overrideConf);
      }
      cluster = new MiniDFSCluster.Builder(nnConf).nnTopology(topology).build();
      cluster.waitActive();

      // Store NN pointers
      for (int i = 0; i < namenodes.size(); i++) {
        NameNode nn = cluster.getNameNode(i);
        namenodes.get(i).setNamenode(nn);
      }

    } catch (Exception e) {
      LOG.error("Cannot start Router DFS cluster: " + e.getMessage(), e);
      cluster.shutdown();
    }
  }

  public void startRouters()
      throws InterruptedException, URISyntaxException, IOException {
    // Create routers
    routers = new ArrayList<RouterContext>();
    for (String ns : nameservices) {

      for (NamenodeContext context : getNamenodes(ns)) {
        routers.add(buildRouter(ns, context.namenodeId));
      }
    }

    // Start all routers
    for (RouterContext router : routers) {
      router.router.start();
    }
    // Wait until all routers are active and record their ports
    for (RouterContext router : routers) {
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
    assertFalse(
        "Timeout waiting for " + router.router.toString() + " to activate.",
        true);
  }


  public void registerNamenodes() throws IOException {
    for (RouterContext r : routers) {
      ActiveNamenodeResolver resolver = r.router.getNamenodeResolver();
      for (NamenodeContext nn : namenodes) {
        // Generate a report
        NamenodeStatusReport report = new NamenodeStatusReport(nn.nameserviceId,
            nn.namenodeId, nn.getRpcAddress(), nn.getServiceAddress(),
            nn.getLifelineAddress(), nn.getHttpAddress());
        report.setNamespaceInfo(nn.namenode.getNamesystem().getFSImage()
            .getStorage().getNamespaceInfo());

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

  public void waitNamenodeRegistration()
      throws InterruptedException, IllegalStateException, IOException {
    for (RouterContext r : routers) {
      for (NamenodeContext nn : namenodes) {
        FederationTestUtils.waitNamenodeRegistered(
            r.router.getNamenodeResolver(), nn.nameserviceId, nn.namenodeId,
            null);
      }
    }
  }

  public void waitRouterRegistrationQuorum(RouterContext router,
      FederationNamenodeServiceState state, String nameservice, String namenode)
          throws InterruptedException, IOException {
    LOG.info("Waiting for NN - " + nameservice + ":" + namenode
        + " to transition to state - " + state);
    FederationTestUtils.waitNamenodeRegistered(
        router.router.getNamenodeResolver(), nameservice, namenode, state);
  }

  public String getFederatedPathForNameservice(String ns) {
    return "/" + ns;
  }

  public String getNamenodePathForNameservice(String ns) {
    return "/target-" + ns;
  }

  /**
   * @return example:
   *         <ul>
   *         <li>/ns0/testdir which maps to ns0->/target-ns0/testdir
   *         </ul>
   */
  public String getFederatedTestDirectoryForNameservice(String ns) {
    return getFederatedPathForNameservice(ns) + "/" + TEST_DIR;
  }

  /**
   * @return example:
   *         <ul>
   *         <li>/target-ns0/testdir
   *         </ul>
   */
  public String getNamenodeTestDirectoryForNameservice(String ns) {
    return getNamenodePathForNameservice(ns) + "/" + TEST_DIR;
  }

  /**
   * @return example:
   *         <ul>
   *         <li>/ns0/testfile which maps to ns0->/target-ns0/testfile
   *         </ul>
   */
  public String getFederatedTestFileForNameservice(String ns) {
    return getFederatedPathForNameservice(ns) + "/" + TEST_FILE;
  }

  /**
   * @return example:
   *         <ul>
   *         <li>/target-ns0/testfile
   *         </ul>
   */
  public String getNamenodeTestFileForNameservice(String ns) {
    return getNamenodePathForNameservice(ns) + "/" + TEST_FILE;
  }

  public void shutdown() {
    cluster.shutdown();
    if (routers != null) {
      for (RouterContext context : routers) {
        stopRouter(context);
      }
    }
  }

  public void stopRouter(RouterContext router) {
    try {

      router.router.shutDown();

      int loopCount = 0;
      while (router.router.getServiceState() != STATE.STOPPED) {
        loopCount++;
        Thread.sleep(1000);
        if (loopCount > 20) {
          LOG.error("Unable to shutdown router - " + router.rpcPort);
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
        throw new IOException("Unable to create test directory for ns - " + ns);
      }
    }
  }

  public boolean createTestDirectoriesNamenode(NamenodeContext nn)
      throws IOException {
    return FederationTestUtils.addDirectory(nn.getFileSystem(),
        getNamenodeTestDirectoryForNameservice(nn.nameserviceId));
  }

  public void deleteAllFiles() throws IOException {
    // Delete all files via the NNs and verify
    for (NamenodeContext context : getNamenodes()) {
      FileStatus[] status = context.getFileSystem().listStatus(new Path("/"));
      for(int i = 0; i <status.length; i++) {
        Path p = status[i].getPath();
        context.getFileSystem().delete(p, true);
      }
      status = context.getFileSystem().listStatus(new Path("/"));
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
      for (String ns : nameservices) {
        // Direct path
        resolver.addLocation(getFederatedPathForNameservice(ns), ns,
            getNamenodePathForNameservice(ns));
      }

      // Root path goes to both NS1
      resolver.addLocation("/", nameservices.get(0), "/");
    }
  }
}
