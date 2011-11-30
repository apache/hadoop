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
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.MiniDFSNNTopology.NNConf;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocolR23Compatible.ClientDatanodeWireProtocol;
import org.apache.hadoop.hdfs.protocolR23Compatible.ClientNamenodeWireProtocol;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * This class creates a single-process DFS cluster for junit testing.
 * The data directories for non-simulated DFS are under the testing directory.
 * For simulated data nodes, no underlying fs storage is used.
 */
@InterfaceAudience.LimitedPrivate({"HBase", "HDFS", "Hive", "MapReduce", "Pig"})
@InterfaceStability.Unstable
public class MiniDFSCluster {

  private static final String NAMESERVICE_ID_PREFIX = "nameserviceId";
  private static final Log LOG = LogFactory.getLog(MiniDFSCluster.class);
  /** System property to set the data dir: {@value} */
  public static final String PROP_TEST_BUILD_DATA = "test.build.data";
  /** Configuration option to set the data dir: {@value} */
  public static final String HDFS_MINIDFS_BASEDIR = "hdfs.minidfs.basedir";

  static { DefaultMetricsSystem.setMiniClusterMode(true); }

  /**
   * Class to construct instances of MiniDFSClusters with specific options.
   */
  public static class Builder {
    private int nameNodePort = 0;
    private int nameNodeHttpPort = 0;
    private final Configuration conf;
    private int numDataNodes = 1;
    private boolean format = true;
    private boolean manageNameDfsDirs = true;
    private boolean manageDataDfsDirs = true;
    private StartupOption option = null;
    private String[] racks = null; 
    private String [] hosts = null;
    private long [] simulatedCapacities = null;
    private String clusterId = null;
    private boolean waitSafeMode = true;
    private boolean setupHostsFile = false;
    private MiniDFSNNTopology nnTopology = null;
    
    public Builder(Configuration conf) {
      this.conf = conf;
    }
    
    /**
     * Default: 0
     */
    public Builder nameNodePort(int val) {
      this.nameNodePort = val;
      return this;
    }
    
    /**
     * Default: 0
     */
    public Builder nameNodeHttpPort(int val) {
      this.nameNodeHttpPort = val;
      return this;
    }

    /**
     * Default: 1
     */
    public Builder numDataNodes(int val) {
      this.numDataNodes = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder format(boolean val) {
      this.format = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder manageNameDfsDirs(boolean val) {
      this.manageNameDfsDirs = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder manageDataDfsDirs(boolean val) {
      this.manageDataDfsDirs = val;
      return this;
    }

    /**
     * Default: null
     */
    public Builder startupOption(StartupOption val) {
      this.option = val;
      return this;
    }

    /**
     * Default: null
     */
    public Builder racks(String[] val) {
      this.racks = val;
      return this;
    }

    /**
     * Default: null
     */
    public Builder hosts(String[] val) {
      this.hosts = val;
      return this;
    }

    /**
     * Default: null
     */
    public Builder simulatedCapacities(long[] val) {
      this.simulatedCapacities = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder waitSafeMode(boolean val) {
      this.waitSafeMode = val;
      return this;
    }
    
    /**
     * Default: null
     */
    public Builder clusterId(String cid) {
      this.clusterId = cid;
      return this;
    }

    /**
     * Default: false
     * When true the hosts file/include file for the cluster is setup
     */
    public Builder setupHostsFile(boolean val) {
      this.setupHostsFile = val;
      return this;
    }
    
    /**
     * Default: a single namenode.
     * See {@link MiniDFSNNTopology#simpleFederatedTopology(int)} to set up
     * federated nameservices
     */
    public Builder nnTopology(MiniDFSNNTopology topology) {
      this.nnTopology = topology;
      return this;
    }
    
    /**
     * Construct the actual MiniDFSCluster
     */
    public MiniDFSCluster build() throws IOException {
      return new MiniDFSCluster(this);
    }
  }
  
  /**
   * Used by builder to create and return an instance of MiniDFSCluster
   */
  private MiniDFSCluster(Builder builder) throws IOException {
    if (builder.nnTopology == null) {
      // If no topology is specified, build a single NN. 
      builder.nnTopology = MiniDFSNNTopology.simpleSingleNN(
          builder.nameNodePort, builder.nameNodeHttpPort);
    }
    
    LOG.info("starting cluster with " + 
        builder.nnTopology.countNameNodes() + " namenodes.");
    nameNodes = new NameNodeInfo[builder.nnTopology.countNameNodes()];
      
    initMiniDFSCluster(builder.conf,
                       builder.numDataNodes,
                       builder.format,
                       builder.manageNameDfsDirs,
                       builder.manageDataDfsDirs,
                       builder.option,
                       builder.racks,
                       builder.hosts,
                       builder.simulatedCapacities,
                       builder.clusterId,
                       builder.waitSafeMode,
                       builder.setupHostsFile,
                       builder.nnTopology);
  }
  
  public class DataNodeProperties {
    DataNode datanode;
    Configuration conf;
    String[] dnArgs;

    DataNodeProperties(DataNode node, Configuration conf, String[] args) {
      this.datanode = node;
      this.conf = conf;
      this.dnArgs = args;
    }
  }

  private Configuration conf;
  private NameNodeInfo[] nameNodes;
  private int numDataNodes;
  private ArrayList<DataNodeProperties> dataNodes = 
                         new ArrayList<DataNodeProperties>();
  private File base_dir;
  private File data_dir;
  private boolean waitSafeMode = true;
  private boolean federation;
  
  /**
   * Stores the information related to a namenode in the cluster
   */
  static class NameNodeInfo {
    final NameNode nameNode;
    final Configuration conf;
    NameNodeInfo(NameNode nn, Configuration conf) {
      this.nameNode = nn;
      this.conf = conf;
    }
  }
  
  /**
   * This null constructor is used only when wishing to start a data node cluster
   * without a name node (ie when the name node is started elsewhere).
   */
  public MiniDFSCluster() {
    nameNodes = new NameNodeInfo[0]; // No namenode in the cluster
  }
  
  /**
   * Modify the config and start up the servers with the given operation.
   * Servers will be started on free ports.
   * <p>
   * The caller must manage the creation of NameNode and DataNode directories
   * and have already set {@link #DFS_NAMENODE_NAME_DIR_KEY} and 
   * {@link #DFS_DATANODE_DATA_DIR_KEY} in the given conf.
   * 
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param nameNodeOperation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   */
  @Deprecated // in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        StartupOption nameNodeOperation) throws IOException {
    this(0, conf, numDataNodes, false, false, false,  nameNodeOperation, 
          null, null, null);
  }
  
  /**
   * Modify the config and start up the servers.  The rpc and info ports for
   * servers are guaranteed to use free ports.
   * <p>
   * NameNode and DataNode directory creation and configuration will be
   * managed by this class.
   *
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param racks array of strings indicating the rack that each DataNode is on
   */
  @Deprecated // in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        boolean format,
                        String[] racks) throws IOException {
    this(0, conf, numDataNodes, format, true, true,  null, racks, null, null);
  }
  
  /**
   * Modify the config and start up the servers.  The rpc and info ports for
   * servers are guaranteed to use free ports.
   * <p>
   * NameNode and DataNode directory creation and configuration will be
   * managed by this class.
   *
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostname for each DataNode
   */
  @Deprecated // in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        boolean format,
                        String[] racks, String[] hosts) throws IOException {
    this(0, conf, numDataNodes, format, true, true, null, racks, hosts, null);
  }
  
  /**
   * NOTE: if possible, the other constructors that don't have nameNode port 
   * parameter should be used as they will ensure that the servers use free 
   * ports.
   * <p>
   * Modify the config and start up the servers.  
   * 
   * @param nameNodePort suggestion for which rpc port to use.  caller should
   *          use getNameNodePort() to get the actual port used.
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting 
   *          up
   * @param manageDfsDirs if true, the data directories for servers will be
   *          created and {@link #DFS_NAMENODE_NAME_DIR_KEY} and 
   *          {@link #DFS_DATANODE_DATA_DIR_KEY} will be set in 
   *          the conf
   * @param operation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   */
  @Deprecated // in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
  public MiniDFSCluster(int nameNodePort, 
                        Configuration conf,
                        int numDataNodes,
                        boolean format,
                        boolean manageDfsDirs,
                        StartupOption operation,
                        String[] racks) throws IOException {
    this(nameNodePort, conf, numDataNodes, format, manageDfsDirs, manageDfsDirs,
         operation, racks, null, null);
  }

  /**
   * NOTE: if possible, the other constructors that don't have nameNode port 
   * parameter should be used as they will ensure that the servers use free ports.
   * <p>
   * Modify the config and start up the servers.  
   * 
   * @param nameNodePort suggestion for which rpc port to use.  caller should
   *          use getNameNodePort() to get the actual port used.
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param manageDfsDirs if true, the data directories for servers will be
   *          created and {@link #DFS_NAMENODE_NAME_DIR_KEY} and 
   *          {@link #DFS_DATANODE_DATA_DIR_KEY} will be set in 
   *          the conf
   * @param operation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param simulatedCapacities array of capacities of the simulated data nodes
   */
  @Deprecated // in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
  public MiniDFSCluster(int nameNodePort, 
                        Configuration conf,
                        int numDataNodes,
                        boolean format,
                        boolean manageDfsDirs,
                        StartupOption operation,
                        String[] racks,
                        long[] simulatedCapacities) throws IOException {
    this(nameNodePort, conf, numDataNodes, format, manageDfsDirs, manageDfsDirs,
          operation, racks, null, simulatedCapacities);
  }
  
  /**
   * NOTE: if possible, the other constructors that don't have nameNode port 
   * parameter should be used as they will ensure that the servers use free ports.
   * <p>
   * Modify the config and start up the servers.  
   * 
   * @param nameNodePort suggestion for which rpc port to use.  caller should
   *          use getNameNodePort() to get the actual port used.
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param manageNameDfsDirs if true, the data directories for servers will be
   *          created and {@link #DFS_NAMENODE_NAME_DIR_KEY} and 
   *          {@link #DFS_DATANODE_DATA_DIR_KEY} will be set in 
   *          the conf
   * @param manageDataDfsDirs if true, the data directories for datanodes will
   *          be created and {@link #DFS_DATANODE_DATA_DIR_KEY} 
   *          set to same in the conf
   * @param operation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostnames of each DataNode
   * @param simulatedCapacities array of capacities of the simulated data nodes
   */
  @Deprecated // in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
  public MiniDFSCluster(int nameNodePort, 
                        Configuration conf,
                        int numDataNodes,
                        boolean format,
                        boolean manageNameDfsDirs,
                        boolean manageDataDfsDirs,
                        StartupOption operation,
                        String[] racks, String hosts[],
                        long[] simulatedCapacities) throws IOException {
    this.nameNodes = new NameNodeInfo[1]; // Single namenode in the cluster
    initMiniDFSCluster(conf, numDataNodes, format,
        manageNameDfsDirs, manageDataDfsDirs, operation, racks, hosts,
        simulatedCapacities, null, true, false,
        MiniDFSNNTopology.simpleSingleNN(nameNodePort, 0));
  }

  private void initMiniDFSCluster(
      Configuration conf,
      int numDataNodes, boolean format, boolean manageNameDfsDirs,
      boolean manageDataDfsDirs, StartupOption operation, String[] racks,
      String[] hosts, long[] simulatedCapacities, String clusterId,
      boolean waitSafeMode, boolean setupHostsFile,
      MiniDFSNNTopology nnTopology)
  throws IOException {
    this.conf = conf;
    base_dir = new File(determineDfsBaseDir());
    data_dir = new File(base_dir, "data");
    this.waitSafeMode = waitSafeMode;
    
    // use alternate RPC engine if spec'd
    String rpcEngineName = System.getProperty("hdfs.rpc.engine");
    if (rpcEngineName != null && !"".equals(rpcEngineName)) {
      
      LOG.info("HDFS using RPCEngine: " + rpcEngineName);
      try {
        Class<?> rpcEngine = conf.getClassByName(rpcEngineName);
        setRpcEngine(conf, NamenodeProtocols.class, rpcEngine);
        setRpcEngine(conf, ClientNamenodeWireProtocol.class, rpcEngine);
        setRpcEngine(conf, ClientDatanodeWireProtocol.class, rpcEngine);
        setRpcEngine(conf, NamenodeProtocol.class, rpcEngine);
        setRpcEngine(conf, ClientProtocol.class, rpcEngine);
        setRpcEngine(conf, DatanodeProtocol.class, rpcEngine);
        setRpcEngine(conf, RefreshAuthorizationPolicyProtocol.class, rpcEngine);
        setRpcEngine(conf, RefreshUserMappingsProtocol.class, rpcEngine);
        setRpcEngine(conf, GetUserMappingsProtocol.class, rpcEngine);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }

      // disable service authorization, as it does not work with tunnelled RPC
      conf.setBoolean(HADOOP_SECURITY_AUTHORIZATION,
                      false);
    }
    
    int replication = conf.getInt(DFS_REPLICATION_KEY, 3);
    conf.setInt(DFS_REPLICATION_KEY, Math.min(replication, numDataNodes));
    conf.setInt(DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, 0);
    conf.setInt(DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY, 3); // 3 second
    conf.setClass(NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY, 
                   StaticMapping.class, DNSToSwitchMapping.class);
    
    federation = nnTopology.isFederated();
    createNameNodesAndSetConf(
        nnTopology, manageNameDfsDirs, format, operation, clusterId, conf);
    
    if (format) {
      if (data_dir.exists() && !FileUtil.fullyDelete(data_dir)) {
        throw new IOException("Cannot remove data directory: " + data_dir);
      }
    }
    
    // Start the DataNodes
    startDataNodes(conf, numDataNodes, manageDataDfsDirs, operation, racks,
        hosts, simulatedCapacities, setupHostsFile);
    waitClusterUp();
    //make sure ProxyUsers uses the latest conf
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
  }
  
  private void createNameNodesAndSetConf(MiniDFSNNTopology nnTopology,
      boolean manageNameDfsDirs, boolean format, StartupOption operation,
      String clusterId,
      Configuration conf) throws IOException {
    Preconditions.checkArgument(nnTopology.countNameNodes() > 0,
        "empty NN topology: no namenodes specified!");

    if (!federation && nnTopology.countNameNodes() == 1) {
      NNConf onlyNN = nnTopology.getOnlyNameNode();
      // we only had one NN, set DEFAULT_NAME for it
      conf.set(FS_DEFAULT_NAME_KEY, "127.0.0.1:" + onlyNN.getIpcPort());
    }
    
    int nnCounter = 0;
    List<String> nsIds = Lists.newArrayList();
    for (MiniDFSNNTopology.NSConf nameservice : nnTopology.getNameservices()) {
      String nsId = nameservice.getId();
      nsIds.add(nameservice.getId());
      
      Preconditions.checkArgument(
          !federation || nsId != null,
          "if there is more than one NS, they must have names");

      // First set up the configuration which all of the NNs
      // need to have - have to do this a priori before starting
      // *any* of the NNs, so they know to come up in standby.
      List<String> nnIds = Lists.newArrayList();
      // Iterate over the NNs in this nameservice
      for (NNConf nn : nameservice.getNNs()) {
        nnIds.add(nn.getNnId());

        initNameNodeAddress(conf, nameservice.getId(), nn);
      }

      // If HA is enabled on this nameservice, enumerate all the namenodes
      // in the configuration. Also need to set a shared edits dir
      if (nnIds.size() > 1) {
        conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY, nameservice.getId()),
            Joiner.on(",").join(nnIds));
        if (manageNameDfsDirs) {
          URI sharedEditsUri = fileAsURI(new File(base_dir, "shared-edits-" +
              nnCounter + "-through-" + (nnCounter+nnIds.size()-1)));
          // TODO in HDFS-1971: conf.set(DFS_NAMENODE_SHARED_EDITS_DIR_KEY, sharedEditsUri.toString());
        }
      }

      // Now start all the NNs in this nameservice.
      int i = 0;
      for (NNConf nn : nameservice.getNNs()) {
        initNameNodeConf(conf, nsId, nn.getNnId(), manageNameDfsDirs, nnCounter);
        
        boolean formatThisOne = format;
        if (format && i++ > 0) {
          // Don't format the second NN in an HA setup - that
          // would result in it having a different clusterID,
          // block pool ID, etc. Instead, copy the name dirs
          // from the first one.
          formatThisOne = false;
          copyNameDirs(getConfiguration(nnCounter - 1), conf);
        }
        
        createNameNode(nnCounter++, conf, numDataNodes, formatThisOne,
            operation, clusterId, nsId, nn.getNnId());
      }
      
    }
    if (federation) {
      // If we have more than one nameservice, need to enumerate them in the
      // config.
      conf.set(DFS_FEDERATION_NAMESERVICES, Joiner.on(",").join(nsIds));
    }
    
  }
  
  private void initNameNodeConf(Configuration conf,
      String nameserviceId, String nnId,
      boolean manageNameDfsDirs, int nnIndex)
      throws IOException {
    if (nameserviceId != null) {
      conf.set(DFS_FEDERATION_NAMESERVICE_ID, nameserviceId);
    }
    if (nnId != null) {
      conf.set(DFS_HA_NAMENODE_ID_KEY, nnId);
    }
    
    if (manageNameDfsDirs) {
      conf.set(DFS_NAMENODE_NAME_DIR_KEY,
          fileAsURI(new File(base_dir, "name" + (2*nnIndex + 1)))+","+
          fileAsURI(new File(base_dir, "name" + (2*nnIndex + 2))));
      conf.set(DFS_NAMENODE_CHECKPOINT_DIR_KEY,
          fileAsURI(new File(base_dir, "namesecondary" + (2*nnIndex + 1)))+","+
          fileAsURI(new File(base_dir, "namesecondary" + (2*nnIndex + 2))));
    }
  }

  private void copyNameDirs(Configuration srcConf, Configuration dstConf)
      throws IOException {
    Collection<URI> srcDirs = FSNamesystem.getNamespaceDirs(srcConf);
    Collection<URI> dstDirs = FSNamesystem.getNamespaceDirs(dstConf);
    URI srcDir = Lists.newArrayList(srcDirs).get(0);
    FileSystem dstFS = FileSystem.getLocal(dstConf).getRaw();
    for (URI dstDir : dstDirs) {
      Preconditions.checkArgument(!dstDir.equals(srcDir));
      Files.deleteRecursively(new File(dstDir));
      LOG.info("Copying namedir from primary node dir "
          + srcDir + " to " + dstDir);
      FileUtil.copy(
          new File(srcDir),
          dstFS, new Path(dstDir), false, dstConf);
    }
  }

  /**
   * Initialize the address and port for this NameNode. In the
   * non-federated case, the nameservice and namenode ID may be
   * null.
   */
  private static void initNameNodeAddress(Configuration conf,
      String nameserviceId, NNConf nnConf) {
    // Set NN-specific specific key
    String key = DFSUtil.addKeySuffixes(
        DFS_NAMENODE_HTTP_ADDRESS_KEY, nameserviceId,
        nnConf.getNnId());
    conf.set(key, "127.0.0.1:" + nnConf.getHttpPort());

    key = DFSUtil.addKeySuffixes(
        DFS_NAMENODE_RPC_ADDRESS_KEY, nameserviceId,
        nnConf.getNnId());
    conf.set(key, "127.0.0.1:" + nnConf.getIpcPort());
  }
  
  private void createNameNode(int nnIndex, Configuration conf,
      int numDataNodes, boolean format, StartupOption operation,
      String clusterId, String nameserviceId,
      String nnId)
      throws IOException {
    // Format and clean out DataNode directories
    if (format) {
      DFSTestUtil.formatNameNode(conf);
    }
    if (operation == StartupOption.UPGRADE){
      operation.setClusterId(clusterId);
    }
    
    // Start the NameNode
    String[] args = (operation == null ||
                     operation == StartupOption.FORMAT ||
                     operation == StartupOption.REGULAR) ?
      new String[] {} : new String[] {operation.getName()};
    NameNode nn =  NameNode.createNameNode(args, conf);
    
    // After the NN has started, set back the bound ports into
    // the conf
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_RPC_ADDRESS_KEY, nameserviceId, nnId), NameNode
        .getHostPortString(nn.getNameNodeAddress()));
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_HTTP_ADDRESS_KEY, nameserviceId, nnId), NameNode
        .getHostPortString(nn.getHttpAddress()));
    DFSUtil.setGenericConf(conf, nameserviceId, nnId,
        DFS_NAMENODE_HTTP_ADDRESS_KEY);
    nameNodes[nnIndex] = new NameNodeInfo(nn, new Configuration(conf));
  }

  private void setRpcEngine(Configuration conf, Class<?> protocol, Class<?> engine) {
    conf.setClass("rpc.engine."+protocol.getName(), engine, Object.class);
  }

  /**
   * @return URI of the namenode from a single namenode MiniDFSCluster
   */
  public URI getURI() {
    checkSingleNameNode();
    return getURI(0);
  }
  
  /**
   * @return URI of the given namenode in MiniDFSCluster
   */
  public URI getURI(int nnIndex) {
    InetSocketAddress addr = nameNodes[nnIndex].nameNode.getNameNodeAddress();
    String hostPort = NameNode.getHostPortString(addr);
    URI uri = null;
    try {
      uri = new URI("hdfs://" + hostPort);
    } catch (URISyntaxException e) {
      NameNode.LOG.warn("unexpected URISyntaxException: " + e );
    }
    return uri;
  }

  /**
   * @return Configuration of for the given namenode
   */
  public Configuration getConfiguration(int nnIndex) {
    return nameNodes[nnIndex].conf;
  }

  /**
   * wait for the given namenode to get out of safemode.
   */
  public void waitNameNodeUp(int nnIndex) {
    while (!isNameNodeUp(nnIndex)) {
      try {
        LOG.warn("Waiting for namenode at " + nnIndex + " to start...");
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
  }
  
  /**
   * wait for the cluster to get out of safemode.
   */
  public void waitClusterUp() {
    if (numDataNodes > 0) {
      while (!isClusterUp()) {
        try {
          LOG.warn("Waiting for the Mini HDFS Cluster to start...");
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }
    }
  }

  /**
   * Modify the config and start up additional DataNodes.  The info port for
   * DataNodes is guaranteed to use a free port.
   *  
   *  Data nodes can run with the name node in the mini cluster or
   *  a real name node. For example, running with a real name node is useful
   *  when running simulated data nodes with a real name node.
   *  If minicluster's name node is null assume that the conf has been
   *  set with the right address:port of the name node.
   *
   * @param conf the base configuration to use in starting the DataNodes.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param manageDfsDirs if true, the data directories for DataNodes will be
   *          created and {@link #DFS_DATANODE_DATA_DIR_KEY} will be set 
   *          in the conf
   * @param operation the operation with which to start the DataNodes.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostnames for each DataNode
   * @param simulatedCapacities array of capacities of the simulated data nodes
   *
   * @throws IllegalStateException if NameNode has been shutdown
   */
  public synchronized void startDataNodes(Configuration conf, int numDataNodes, 
                             boolean manageDfsDirs, StartupOption operation, 
                             String[] racks, String[] hosts,
                             long[] simulatedCapacities) throws IOException {
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks,
                   hosts, simulatedCapacities, false);
  }

  /**
   * Modify the config and start up additional DataNodes.  The info port for
   * DataNodes is guaranteed to use a free port.
   *  
   *  Data nodes can run with the name node in the mini cluster or
   *  a real name node. For example, running with a real name node is useful
   *  when running simulated data nodes with a real name node.
   *  If minicluster's name node is null assume that the conf has been
   *  set with the right address:port of the name node.
   *
   * @param conf the base configuration to use in starting the DataNodes.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param manageDfsDirs if true, the data directories for DataNodes will be
   *          created and {@link #DFS_DATANODE_DATA_DIR_KEY} will be 
   *          set in the conf
   * @param operation the operation with which to start the DataNodes.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostnames for each DataNode
   * @param simulatedCapacities array of capacities of the simulated data nodes
   * @param setupHostsFile add new nodes to dfs hosts files
   *
   * @throws IllegalStateException if NameNode has been shutdown
   */
  public synchronized void startDataNodes(Configuration conf, int numDataNodes, 
                             boolean manageDfsDirs, StartupOption operation, 
                             String[] racks, String[] hosts,
                             long[] simulatedCapacities,
                             boolean setupHostsFile) throws IOException {
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, hosts,
                   simulatedCapacities, setupHostsFile, false);
  }

  /**
   * Modify the config and start up additional DataNodes.  The info port for
   * DataNodes is guaranteed to use a free port.
   *  
   *  Data nodes can run with the name node in the mini cluster or
   *  a real name node. For example, running with a real name node is useful
   *  when running simulated data nodes with a real name node.
   *  If minicluster's name node is null assume that the conf has been
   *  set with the right address:port of the name node.
   *
   * @param conf the base configuration to use in starting the DataNodes.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param manageDfsDirs if true, the data directories for DataNodes will be
   *          created and {@link #DFS_DATANODE_DATA_DIR_KEY} will be 
   *          set in the conf
   * @param operation the operation with which to start the DataNodes.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostnames for each DataNode
   * @param simulatedCapacities array of capacities of the simulated data nodes
   * @param setupHostsFile add new nodes to dfs hosts files
   * @param checkDataNodeAddrConfig if true, only set DataNode port addresses if not already set in config
   *
   * @throws IllegalStateException if NameNode has been shutdown
   */
  public synchronized void startDataNodes(Configuration conf, int numDataNodes,
                             boolean manageDfsDirs, StartupOption operation, 
                             String[] racks, String[] hosts,
                             long[] simulatedCapacities,
                             boolean setupHostsFile,
                             boolean checkDataNodeAddrConfig) throws IOException {
    conf.set(DFS_DATANODE_HOST_NAME_KEY, "127.0.0.1");

    int curDatanodesNum = dataNodes.size();
    // for mincluster's the default initialDelay for BRs is 0
    if (conf.get(DFS_BLOCKREPORT_INITIAL_DELAY_KEY) == null) {
      conf.setLong(DFS_BLOCKREPORT_INITIAL_DELAY_KEY, 0);
    }
    // If minicluster's name node is null assume that the conf has been
    // set with the right address:port of the name node.
    //
    if (racks != null && numDataNodes > racks.length ) {
      throw new IllegalArgumentException( "The length of racks [" + racks.length
          + "] is less than the number of datanodes [" + numDataNodes + "].");
    }
    if (hosts != null && numDataNodes > hosts.length ) {
      throw new IllegalArgumentException( "The length of hosts [" + hosts.length
          + "] is less than the number of datanodes [" + numDataNodes + "].");
    }
    //Generate some hostnames if required
    if (racks != null && hosts == null) {
      hosts = new String[numDataNodes];
      for (int i = curDatanodesNum; i < curDatanodesNum + numDataNodes; i++) {
        hosts[i - curDatanodesNum] = "host" + i + ".foo.com";
      }
    }

    if (simulatedCapacities != null 
        && numDataNodes > simulatedCapacities.length) {
      throw new IllegalArgumentException( "The length of simulatedCapacities [" 
          + simulatedCapacities.length
          + "] is less than the number of datanodes [" + numDataNodes + "].");
    }

    String [] dnArgs = (operation == null ||
                        operation != StartupOption.ROLLBACK) ?
        null : new String[] {operation.getName()};
    
    
    for (int i = curDatanodesNum; i < curDatanodesNum+numDataNodes; i++) {
      Configuration dnConf = new HdfsConfiguration(conf);
      // Set up datanode address
      setupDatanodeAddress(dnConf, setupHostsFile, checkDataNodeAddrConfig);
      if (manageDfsDirs) {
        File dir1 = getInstanceStorageDir(i, 0);
        File dir2 = getInstanceStorageDir(i, 1);
        dir1.mkdirs();
        dir2.mkdirs();
        if (!dir1.isDirectory() || !dir2.isDirectory()) { 
          throw new IOException("Mkdirs failed to create directory for DataNode "
                                + i + ": " + dir1 + " or " + dir2);
        }
        String dirs = fileAsURI(dir1) + "," + fileAsURI(dir2);
        dnConf.set(DFS_DATANODE_DATA_DIR_KEY, dirs);
        conf.set(DFS_DATANODE_DATA_DIR_KEY, dirs);
      }
      if (simulatedCapacities != null) {
        dnConf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
        dnConf.setLong(SimulatedFSDataset.CONFIG_PROPERTY_CAPACITY,
            simulatedCapacities[i-curDatanodesNum]);
      }
      LOG.info("Starting DataNode " + i + " with "
                         + DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY + ": "
                         + dnConf.get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY));
      if (hosts != null) {
        dnConf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, hosts[i - curDatanodesNum]);
        LOG.info("Starting DataNode " + i + " with hostname set to: "
                           + dnConf.get(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY));
      }
      if (racks != null) {
        String name = hosts[i - curDatanodesNum];
        LOG.info("Adding node with hostname : " + name + " to rack " +
                            racks[i-curDatanodesNum]);
        StaticMapping.addNodeToRack(name,
                                    racks[i-curDatanodesNum]);
      }
      Configuration newconf = new HdfsConfiguration(dnConf); // save config
      if (hosts != null) {
        NetUtils.addStaticResolution(hosts[i - curDatanodesNum], "localhost");
      }
      DataNode dn = DataNode.instantiateDataNode(dnArgs, dnConf);
      if(dn == null)
        throw new IOException("Cannot start DataNode in "
            + dnConf.get(DFS_DATANODE_DATA_DIR_KEY));
      //since the HDFS does things based on IP:port, we need to add the mapping
      //for IP:port to rackId
      String ipAddr = dn.getSelfAddr().getAddress().getHostAddress();
      if (racks != null) {
        int port = dn.getSelfAddr().getPort();
        LOG.info("Adding node with IP:port : " + ipAddr + ":" + port +
                            " to rack " + racks[i-curDatanodesNum]);
        StaticMapping.addNodeToRack(ipAddr + ":" + port,
                                  racks[i-curDatanodesNum]);
      }
      dn.runDatanodeDaemon();
      dataNodes.add(new DataNodeProperties(dn, newconf, dnArgs));
    }
    curDatanodesNum += numDataNodes;
    this.numDataNodes += numDataNodes;
    waitActive();
  }
  
  
  
  /**
   * Modify the config and start up the DataNodes.  The info port for
   * DataNodes is guaranteed to use a free port.
   *
   * @param conf the base configuration to use in starting the DataNodes.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param manageDfsDirs if true, the data directories for DataNodes will be
   *          created and {@link DFSConfigKeys#DFS_DATANODE_DATA_DIR_KEY} will be 
   *          set in the conf
   * @param operation the operation with which to start the DataNodes.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   *
   * @throws IllegalStateException if NameNode has been shutdown
   */
  
  public void startDataNodes(Configuration conf, int numDataNodes, 
      boolean manageDfsDirs, StartupOption operation, 
      String[] racks
      ) throws IOException {
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, null,
        null, false);
  }
  
  /**
   * Modify the config and start up additional DataNodes.  The info port for
   * DataNodes is guaranteed to use a free port.
   *  
   *  Data nodes can run with the name node in the mini cluster or
   *  a real name node. For example, running with a real name node is useful
   *  when running simulated data nodes with a real name node.
   *  If minicluster's name node is null assume that the conf has been
   *  set with the right address:port of the name node.
   *
   * @param conf the base configuration to use in starting the DataNodes.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param manageDfsDirs if true, the data directories for DataNodes will be
   *          created and {@link DFSConfigKeys#DFS_DATANODE_DATA_DIR_KEY} will 
   *          be set in the conf
   * @param operation the operation with which to start the DataNodes.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param simulatedCapacities array of capacities of the simulated data nodes
   *
   * @throws IllegalStateException if NameNode has been shutdown
   */
  public void startDataNodes(Configuration conf, int numDataNodes, 
                             boolean manageDfsDirs, StartupOption operation, 
                             String[] racks,
                             long[] simulatedCapacities) throws IOException {
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, null,
                   simulatedCapacities, false);
    
  }

  /**
   * Finalize the namenode. Block pools corresponding to the namenode are
   * finalized on the datanode.
   */
  private void finalizeNamenode(NameNode nn, Configuration conf) throws Exception {
    if (nn == null) {
      throw new IllegalStateException("Attempting to finalize "
                                      + "Namenode but it is not running");
    }
    ToolRunner.run(new DFSAdmin(conf), new String[] {"-finalizeUpgrade"});
  }
  
  /**
   * Finalize cluster for the namenode at the given index 
   * @see MiniDFSCluster#finalizeCluster(Configuration)
   * @param nnIndex
   * @param conf
   * @throws Exception
   */
  public void finalizeCluster(int nnIndex, Configuration conf) throws Exception {
    finalizeNamenode(nameNodes[nnIndex].nameNode, nameNodes[nnIndex].conf);
  }

  /**
   * If the NameNode is running, attempt to finalize a previous upgrade.
   * When this method return, the NameNode should be finalized, but
   * DataNodes may not be since that occurs asynchronously.
   *
   * @throws IllegalStateException if the Namenode is not running.
   */
  public void finalizeCluster(Configuration conf) throws Exception {
    for (NameNodeInfo nnInfo : nameNodes) {
      if (nnInfo == null) {
        throw new IllegalStateException("Attempting to finalize "
            + "Namenode but it is not running");
      }
      finalizeNamenode(nnInfo.nameNode, nnInfo.conf);
    }
  }
  
  public int getNumNameNodes() {
    return nameNodes.length;
  }
  
  /**
   * Gets the started NameNode.  May be null.
   */
  public NameNode getNameNode() {
    checkSingleNameNode();
    return getNameNode(0);
  }
  
  /**
   * Get an instance of the NameNode's RPC handler.
   */
  public NamenodeProtocols getNameNodeRpc() {
    checkSingleNameNode();
    return getNameNode(0).getRpcServer();
  }
  
  /**
   * Gets the NameNode for the index.  May be null.
   */
  public NameNode getNameNode(int nnIndex) {
    return nameNodes[nnIndex].nameNode;
  }
  
  /**
   * Return the {@link FSNamesystem} object.
   * @return {@link FSNamesystem} object.
   */
  public FSNamesystem getNamesystem() {
    checkSingleNameNode();
    return NameNodeAdapter.getNamesystem(nameNodes[0].nameNode);
  }
  
  public FSNamesystem getNamesystem(int nnIndex) {
    return NameNodeAdapter.getNamesystem(nameNodes[nnIndex].nameNode);
  }

  /**
   * Gets a list of the started DataNodes.  May be empty.
   */
  public ArrayList<DataNode> getDataNodes() {
    ArrayList<DataNode> list = new ArrayList<DataNode>();
    for (int i = 0; i < dataNodes.size(); i++) {
      DataNode node = dataNodes.get(i).datanode;
      list.add(node);
    }
    return list;
  }
  
  /** @return the datanode having the ipc server listen port */
  public DataNode getDataNode(int ipcPort) {
    for(DataNode dn : getDataNodes()) {
      if (dn.ipcServer.getListenerAddress().getPort() == ipcPort) {
        return dn;
      }
    }
    return null;
  }

  /**
   * Gets the rpc port used by the NameNode, because the caller 
   * supplied port is not necessarily the actual port used.
   * Assumption: cluster has a single namenode
   */     
  public int getNameNodePort() {
    checkSingleNameNode();
    return getNameNodePort(0);
  }
    
  /**
   * Gets the rpc port used by the NameNode at the given index, because the
   * caller supplied port is not necessarily the actual port used.
   */     
  public int getNameNodePort(int nnIndex) {
    return nameNodes[nnIndex].nameNode.getNameNodeAddress().getPort();
  }
    
  /**
   * Shutdown all the nodes in the cluster.
   */
  public void shutdown() {
    LOG.info("Shutting down the Mini HDFS Cluster");
    shutdownDataNodes();
    for (NameNodeInfo nnInfo : nameNodes) {
      if (nnInfo == null) continue;
      NameNode nameNode = nnInfo.nameNode;
      if (nameNode != null) {
        nameNode.stop();
        nameNode.join();
        nameNode = null;
      }
    }
  }
  
  /**
   * Shutdown all DataNodes started by this class.  The NameNode
   * is left running so that new DataNodes may be started.
   */
  public void shutdownDataNodes() {
    for (int i = dataNodes.size()-1; i >= 0; i--) {
      LOG.info("Shutting down DataNode " + i);
      DataNode dn = dataNodes.remove(i).datanode;
      dn.shutdown();
      numDataNodes--;
    }
  }

  /**
   * Shutdown all the namenodes.
   */
  public synchronized void shutdownNameNodes() {
    for (int i = 0; i < nameNodes.length; i++) {
      shutdownNameNode(i);
    }
  }
  
  /**
   * Shutdown the namenode at a given index.
   */
  public synchronized void shutdownNameNode(int nnIndex) {
    NameNode nn = nameNodes[nnIndex].nameNode;
    if (nn != null) {
      LOG.info("Shutting down the namenode");
      nn.stop();
      nn.join();
      Configuration conf = nameNodes[nnIndex].conf;
      nameNodes[nnIndex] = new NameNodeInfo(null, conf);
    }
  }
  
  /**
   * Restart the namenode.
   */
  public synchronized void restartNameNode() throws IOException {
    checkSingleNameNode();
    restartNameNode(true);
  }
  
  /**
   * Restart the namenode. Optionally wait for the cluster to become active.
   */
  public synchronized void restartNameNode(boolean waitActive)
      throws IOException {
    checkSingleNameNode();
    restartNameNode(0, waitActive);
  }
  
  /**
   * Restart the namenode at a given index.
   */
  public synchronized void restartNameNode(int nnIndex) throws IOException {
    restartNameNode(nnIndex, true);
  }

  /**
   * Restart the namenode at a given index. Optionally wait for the cluster
   * to become active.
   */
  public synchronized void restartNameNode(int nnIndex, boolean waitActive)
      throws IOException {
    Configuration conf = nameNodes[nnIndex].conf;
    shutdownNameNode(nnIndex);
    NameNode nn = NameNode.createNameNode(new String[] {}, conf);
    nameNodes[nnIndex] = new NameNodeInfo(nn, conf);
    if (waitActive) {
      waitClusterUp();
      LOG.info("Restarted the namenode");
      waitActive();
      LOG.info("Cluster is active");
    }
  }

  /**
   * Return the contents of the given block on the given datanode.
   *
   * @param block block to be corrupted
   * @throws IOException on error accessing the file for the given block
   */
  public int corruptBlockOnDataNodes(ExtendedBlock block) throws IOException{
    int blocksCorrupted = 0;
    File[] blockFiles = getAllBlockFiles(block);
    for (File f : blockFiles) {
      if (corruptBlock(f)) {
        blocksCorrupted++;
      }
    }
    return blocksCorrupted;
  }

  public String readBlockOnDataNode(int i, ExtendedBlock block)
      throws IOException {
    assert (i >= 0 && i < dataNodes.size()) : "Invalid datanode "+i;
    File blockFile = getBlockFile(i, block);
    if (blockFile != null && blockFile.exists()) {
      return DFSTestUtil.readFile(blockFile);
    }
    return null;
  }

  /**
   * Corrupt a block on a particular datanode.
   *
   * @param i index of the datanode
   * @param blk name of the block
   * @throws IOException on error accessing the given block or if
   * the contents of the block (on the same datanode) differ.
   * @return true if a replica was corrupted, false otherwise
   * Types: delete, write bad data, truncate
   */
  public static boolean corruptReplica(int i, ExtendedBlock blk)
      throws IOException {
    File blockFile = getBlockFile(i, blk);
    return corruptBlock(blockFile);
  }

  /*
   * Corrupt a block on a particular datanode
   */
  public static boolean corruptBlock(File blockFile) throws IOException {
    if (blockFile == null || !blockFile.exists()) {
      return false;
    }
    // Corrupt replica by writing random bytes into replica
    Random random = new Random();
    RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
    FileChannel channel = raFile.getChannel();
    String badString = "BADBAD";
    int rand = random.nextInt((int)channel.size()/2);
    raFile.seek(rand);
    raFile.write(badString.getBytes());
    raFile.close();
    LOG.warn("Corrupting the block " + blockFile);
    return true;
  }

  /*
   * Shutdown a particular datanode
   */
  public synchronized DataNodeProperties stopDataNode(int i) {
    if (i < 0 || i >= dataNodes.size()) {
      return null;
    }
    DataNodeProperties dnprop = dataNodes.remove(i);
    DataNode dn = dnprop.datanode;
    LOG.info("MiniDFSCluster Stopping DataNode " +
                       dn.getMachineName() +
                       " from a total of " + (dataNodes.size() + 1) + 
                       " datanodes.");
    dn.shutdown();
    numDataNodes--;
    return dnprop;
  }

  /*
   * Shutdown a datanode by name.
   */
  public synchronized DataNodeProperties stopDataNode(String name) {
    int i;
    for (i = 0; i < dataNodes.size(); i++) {
      DataNode dn = dataNodes.get(i).datanode;
      // get BP registration
      DatanodeRegistration dnR = 
        DataNodeTestUtils.getDNRegistrationByMachineName(dn, name);
      LOG.info("for name=" + name + " found bp=" + dnR + 
          "; with dnMn=" + dn.getMachineName());
      if(dnR != null) {
        break;
      }
    }
    return stopDataNode(i);
  }

  /**
   * Restart a datanode
   * @param dnprop datanode's property
   * @return true if restarting is successful
   * @throws IOException
   */
  public boolean restartDataNode(DataNodeProperties dnprop) throws IOException {
    return restartDataNode(dnprop, false);
  }

  /**
   * Restart a datanode, on the same port if requested
   * @param dnprop the datanode to restart
   * @param keepPort whether to use the same port 
   * @return true if restarting is successful
   * @throws IOException
   */
  public synchronized boolean restartDataNode(DataNodeProperties dnprop,
      boolean keepPort) throws IOException {
    Configuration conf = dnprop.conf;
    String[] args = dnprop.dnArgs;
    Configuration newconf = new HdfsConfiguration(conf); // save cloned config
    if (keepPort) {
      InetSocketAddress addr = dnprop.datanode.getSelfAddr();
      conf.set(DFS_DATANODE_ADDRESS_KEY, addr.getAddress().getHostAddress() + ":"
          + addr.getPort());
    }
    dataNodes.add(new DataNodeProperties(DataNode.createDataNode(args, conf),
        newconf, args));
    numDataNodes++;
    return true;
  }

  /*
   * Restart a particular datanode, use newly assigned port
   */
  public boolean restartDataNode(int i) throws IOException {
    return restartDataNode(i, false);
  }

  /*
   * Restart a particular datanode, on the same port if keepPort is true
   */
  public synchronized boolean restartDataNode(int i, boolean keepPort)
      throws IOException {
    DataNodeProperties dnprop = stopDataNode(i);
    if (dnprop == null) {
      return false;
    } else {
      return restartDataNode(dnprop, keepPort);
    }
  }

  /*
   * Restart all datanodes, on the same ports if keepPort is true
   */
  public synchronized boolean restartDataNodes(boolean keepPort)
      throws IOException {
    for (int i = dataNodes.size() - 1; i >= 0; i--) {
      if (!restartDataNode(i, keepPort))
        return false;
      LOG.info("Restarted DataNode " + i);
    }
    return true;
  }

  /*
   * Restart all datanodes, use newly assigned ports
   */
  public boolean restartDataNodes() throws IOException {
    return restartDataNodes(false);
  }
  
  /**
   * Returns true if the NameNode is running and is out of Safe Mode
   * or if waiting for safe mode is disabled.
   */
  public boolean isNameNodeUp(int nnIndex) {
    NameNode nameNode = nameNodes[nnIndex].nameNode;
    if (nameNode == null) {
      return false;
    }
    long[] sizes;
    sizes = NameNodeAdapter.getStats(nameNode.getNamesystem());
    boolean isUp = false;
    synchronized (this) {
      isUp = ((!nameNode.isInSafeMode() || !waitSafeMode) && sizes[0] != 0);
    }
    return isUp;
  }

  /**
   * Returns true if all the NameNodes are running and is out of Safe Mode.
   */
  public boolean isClusterUp() {
    for (int index = 0; index < nameNodes.length; index++) {
      if (!isNameNodeUp(index)) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Returns true if there is at least one DataNode running.
   */
  public boolean isDataNodeUp() {
    if (dataNodes == null || dataNodes.size() == 0) {
      return false;
    }
    for (DataNodeProperties dn : dataNodes) {
      if (dn.datanode.isDatanodeUp()) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Get a client handle to the DFS cluster with a single namenode.
   */
  public FileSystem getFileSystem() throws IOException {
    checkSingleNameNode();
    return getFileSystem(0);
  }
  
  /**
   * Get a client handle to the DFS cluster for the namenode at given index.
   */
  public FileSystem getFileSystem(int nnIndex) throws IOException {
    return FileSystem.get(getURI(nnIndex), nameNodes[nnIndex].conf);
  }

  /**
   * Get another FileSystem instance that is different from FileSystem.get(conf).
   * This simulating different threads working on different FileSystem instances.
   */
  public FileSystem getNewFileSystemInstance(int nnIndex) throws IOException {
    return FileSystem.newInstance(getURI(nnIndex), nameNodes[nnIndex].conf);
  }
  
  /**
   * @return a http URL
   */
  public String getHttpUri(int nnIndex) {
    return "http://"
        + nameNodes[nnIndex].conf
            .get(DFS_NAMENODE_HTTP_ADDRESS_KEY);
  }
  
  /**
   * @return a {@link HftpFileSystem} object.
   */
  public HftpFileSystem getHftpFileSystem(int nnIndex) throws IOException {
    String uri = "hftp://"
        + nameNodes[nnIndex].conf
            .get(DFS_NAMENODE_HTTP_ADDRESS_KEY);
    try {
      return (HftpFileSystem)FileSystem.get(new URI(uri), conf);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  /**
   *  @return a {@link HftpFileSystem} object as specified user. 
   */
  public HftpFileSystem getHftpFileSystemAs(final String username,
      final Configuration conf, final int nnIndex, final String... groups)
      throws IOException, InterruptedException {
    final UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        username, groups);
    return ugi.doAs(new PrivilegedExceptionAction<HftpFileSystem>() {
      @Override
      public HftpFileSystem run() throws Exception {
        return getHftpFileSystem(nnIndex);
      }
    });
  }

  /**
   * Get the directories where the namenode stores its image.
   */
  public Collection<URI> getNameDirs(int nnIndex) {
    return FSNamesystem.getNamespaceDirs(nameNodes[nnIndex].conf);
  }

  /**
   * Get the directories where the namenode stores its edits.
   */
  public Collection<URI> getNameEditsDirs(int nnIndex) {
    return FSNamesystem.getNamespaceEditsDirs(nameNodes[nnIndex].conf);
  }
  
  private HAServiceProtocol getHaServiceClient(int nnIndex) throws IOException {
    InetSocketAddress addr = nameNodes[nnIndex].nameNode.getServiceRpcAddress();
    return RPC.getProxy(HAServiceProtocol.class,
        HAServiceProtocol.versionID, addr, conf);
  }
  
  public void transitionToActive(int nnIndex) throws IOException,
      ServiceFailedException {
    getHaServiceClient(nnIndex).transitionToActive();
  }
  
  public void transitionToStandby(int nnIndex) throws IOException,
      ServiceFailedException {
    getHaServiceClient(nnIndex).transitionToActive();
  }

  /** Wait until the given namenode gets registration from all the datanodes */
  public void waitActive(int nnIndex) throws IOException {
    if (nameNodes.length == 0 || nameNodes[nnIndex] == null) {
      return;
    }
    InetSocketAddress addr = nameNodes[nnIndex].nameNode.getServiceRpcAddress();
    assert addr.getPort() != 0;
    DFSClient client = new DFSClient(addr, conf);

    // ensure all datanodes have registered and sent heartbeat to the namenode
    while (shouldWait(client.datanodeReport(DatanodeReportType.LIVE), addr)) {
      try {
        LOG.info("Waiting for cluster to become active");
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
    }

    client.close();
  }
  
  /**
   * Wait until the cluster is active and running.
   */
  public void waitActive() throws IOException {
    for (int index = 0; index < nameNodes.length; index++) {
      int failedCount = 0;
      while (true) {
        try {
          waitActive(index);
          break;
        } catch (IOException e) {
          failedCount++;
          // Cached RPC connection to namenode, if any, is expected to fail once
          if (failedCount > 1) {
            LOG.warn("Tried waitActive() " + failedCount
                + " time(s) and failed, giving up.  "
                + StringUtils.stringifyException(e));
            throw e;
          }
        }
      }
    }
  }
  
  private synchronized boolean shouldWait(DatanodeInfo[] dnInfo,
      InetSocketAddress addr) {
    // If a datanode failed to start, then do not wait
    for (DataNodeProperties dn : dataNodes) {
      // the datanode thread communicating with the namenode should be alive
      if (!dn.datanode.isBPServiceAlive(addr)) {
        LOG.warn("BPOfferService failed to start in datanode " + dn.datanode
            + " for namenode at " + addr);
        return false;
      }
    }
    
    // Wait for expected number of datanodes to start
    if (dnInfo.length != numDataNodes) {
      return true;
    }
    
    // if one of the data nodes is not fully started, continue to wait
    for (DataNodeProperties dn : dataNodes) {
      if (!dn.datanode.isDatanodeFullyStarted()) {
        return true;
      }
    }
    
    // make sure all datanodes have sent first heartbeat to namenode,
    // using (capacity == 0) as proxy.
    for (DatanodeInfo dn : dnInfo) {
      if (dn.getCapacity() == 0) {
        return true;
      }
    }
    
    // If datanode dataset is not initialized then wait
    for (DataNodeProperties dn : dataNodes) {
      if (dn.datanode.data == null) {
        return true;
      }
    }
    return false;
  }

  public void formatDataNodeDirs() throws IOException {
    base_dir = new File(determineDfsBaseDir());
    data_dir = new File(base_dir, "data");
    if (data_dir.exists() && !FileUtil.fullyDelete(data_dir)) {
      throw new IOException("Cannot remove data directory: " + data_dir);
    }
  }
  
  /**
   * 
   * @param dataNodeIndex - data node whose block report is desired - the index is same as for getDataNodes()
   * @return the block report for the specified data node
   */
  public Iterable<Block> getBlockReport(String bpid, int dataNodeIndex) {
    if (dataNodeIndex < 0 || dataNodeIndex > dataNodes.size()) {
      throw new IndexOutOfBoundsException();
    }
    return dataNodes.get(dataNodeIndex).datanode.getFSDataset().getBlockReport(
        bpid);
  }
  
  
  /**
   * 
   * @return block reports from all data nodes
   *    BlockListAsLongs is indexed in the same order as the list of datanodes returned by getDataNodes()
   */
  public Iterable<Block>[] getAllBlockReports(String bpid) {
    int numDataNodes = dataNodes.size();
    Iterable<Block>[] result = new BlockListAsLongs[numDataNodes];
    for (int i = 0; i < numDataNodes; ++i) {
     result[i] = getBlockReport(bpid, i);
    }
    return result;
  }
  
  
  /**
   * This method is valid only if the data nodes have simulated data
   * @param dataNodeIndex - data node i which to inject - the index is same as for getDataNodes()
   * @param blocksToInject - the blocks
   * @throws IOException
   *              if not simulatedFSDataset
   *             if any of blocks already exist in the data node
   *   
   */
  public void injectBlocks(int dataNodeIndex, Iterable<Block> blocksToInject) throws IOException {
    if (dataNodeIndex < 0 || dataNodeIndex > dataNodes.size()) {
      throw new IndexOutOfBoundsException();
    }
    FSDatasetInterface dataSet = dataNodes.get(dataNodeIndex).datanode.getFSDataset();
    if (!(dataSet instanceof SimulatedFSDataset)) {
      throw new IOException("injectBlocks is valid only for SimilatedFSDataset");
    }
    String bpid = getNamesystem().getBlockPoolId();
    SimulatedFSDataset sdataset = (SimulatedFSDataset) dataSet;
    sdataset.injectBlocks(bpid, blocksToInject);
    dataNodes.get(dataNodeIndex).datanode.scheduleAllBlockReport(0);
  }

  /**
   * Multiple-NameNode version of {@link #injectBlocks(Iterable[])}.
   */
  public void injectBlocks(int nameNodeIndex, int dataNodeIndex,
      Iterable<Block> blocksToInject) throws IOException {
    if (dataNodeIndex < 0 || dataNodeIndex > dataNodes.size()) {
      throw new IndexOutOfBoundsException();
    }
    FSDatasetInterface dataSet = dataNodes.get(dataNodeIndex).datanode.getFSDataset();
    if (!(dataSet instanceof SimulatedFSDataset)) {
      throw new IOException("injectBlocks is valid only for SimilatedFSDataset");
    }
    String bpid = getNamesystem(nameNodeIndex).getBlockPoolId();
    SimulatedFSDataset sdataset = (SimulatedFSDataset) dataSet;
    sdataset.injectBlocks(bpid, blocksToInject);
    dataNodes.get(dataNodeIndex).datanode.scheduleAllBlockReport(0);
  }

  /**
   * This method is valid only if the data nodes have simulated data
   * @param blocksToInject - blocksToInject[] is indexed in the same order as the list 
   *             of datanodes returned by getDataNodes()
   * @throws IOException
   *             if not simulatedFSDataset
   *             if any of blocks already exist in the data nodes
   *             Note the rest of the blocks are not injected.
   */
  public void injectBlocks(Iterable<Block>[] blocksToInject)
      throws IOException {
    if (blocksToInject.length >  dataNodes.size()) {
      throw new IndexOutOfBoundsException();
    }
    for (int i = 0; i < blocksToInject.length; ++i) {
     injectBlocks(i, blocksToInject[i]);
    }
  }

  /**
   * Set the softLimit and hardLimit of client lease periods
   */
  public void setLeasePeriod(long soft, long hard) {
    NameNodeAdapter.setLeasePeriod(getNamesystem(), soft, hard);
  }

  /**
   * Returns the current set of datanodes
   */
  DataNode[] listDataNodes() {
    DataNode[] list = new DataNode[dataNodes.size()];
    for (int i = 0; i < dataNodes.size(); i++) {
      list[i] = dataNodes.get(i).datanode;
    }
    return list;
  }

  /**
   * Access to the data directory used for Datanodes
   */
  public String getDataDirectory() {
    return data_dir.getAbsolutePath();
  }

  /**
   * Get the base directory for this MiniDFS instance.
   * <p/>
   * Within the MiniDFCluster class and any subclasses, this method should be
   * used instead of {@link #getBaseDirectory()} which doesn't support
   * configuration-specific base directories.
   * <p/>
   * First the Configuration property {@link #HDFS_MINIDFS_BASEDIR} is fetched.
   * If non-null, this is returned.
   * If this is null, then {@link #getBaseDirectory()} is called.
   * @return the base directory for this instance.
   */
  protected String determineDfsBaseDir() {
    String dfsdir = conf.get(HDFS_MINIDFS_BASEDIR, null);
    if (dfsdir == null) {
      dfsdir = getBaseDirectory();
    }
    return dfsdir;
  }

  /**
   * Get the base directory for any DFS cluster whose configuration does
   * not explicitly set it. This is done by retrieving the system property
   * {@link #PROP_TEST_BUILD_DATA} (defaulting to "build/test/data" ),
   * and returning that directory with a subdir of /dfs.
   * @return a directory for use as a miniDFS filesystem.
   */
  public static String getBaseDirectory() {
    return System.getProperty(PROP_TEST_BUILD_DATA, "build/test/data") + "/dfs/";
  }

  /**
   * Get a storage directory for a datanode in this specific instance of
   * a MiniCluster.
   *
   * @param dnIndex datanode index (starts from 0)
   * @param dirIndex directory index (0 or 1). Index 0 provides access to the
   *          first storage directory. Index 1 provides access to the second
   *          storage directory.
   * @return Storage directory
   */
  public File getInstanceStorageDir(int dnIndex, int dirIndex) {
    return new File(base_dir, getStorageDirPath(dnIndex, dirIndex));
  }

  /**
   * Get a storage directory for a datanode. There are two storage directories
   * per datanode:
   * <ol>
   * <li><base directory>/data/data<2*dnIndex + 1></li>
   * <li><base directory>/data/data<2*dnIndex + 2></li>
   * </ol>
   * 
   * @param dnIndex datanode index (starts from 0)
   * @param dirIndex directory index (0 or 1). Index 0 provides access to the
   *          first storage directory. Index 1 provides access to the second
   *          storage directory.
   * @return Storage directory
   */
  public static File getStorageDir(int dnIndex, int dirIndex) {
    return new File(getBaseDirectory(), getStorageDirPath(dnIndex, dirIndex));
  }

  /**
   * Calculate the DN instance-specific path for appending to the base dir
   * to determine the location of the storage of a DN instance in the mini cluster
   * @param dnIndex datanode index
   * @param dirIndex directory index (0 or 1).
   * @return
   */
  private static String getStorageDirPath(int dnIndex, int dirIndex) {
    return "data/data" + (2 * dnIndex + 1 + dirIndex);
  }

  /**
   * Get current directory corresponding to the datanode as defined in
   * (@link Storage#STORAGE_DIR_CURRENT}
   * @param storageDir the storage directory of a datanode.
   * @return the datanode current directory
   */
  public static String getDNCurrentDir(File storageDir) {
    return storageDir + "/" + Storage.STORAGE_DIR_CURRENT + "/";
  }
  
  /**
   * Get directory corresponding to block pool directory in the datanode
   * @param storageDir the storage directory of a datanode.
   * @return the block pool directory
   */
  public static String getBPDir(File storageDir, String bpid) {
    return getDNCurrentDir(storageDir) + bpid + "/";
  }
  /**
   * Get directory relative to block pool directory in the datanode
   * @param storageDir
   * @return current directory
   */
  public static String getBPDir(File storageDir, String bpid, String dirName) {
    return getBPDir(storageDir, bpid) + dirName + "/";
  }
  
  /**
   * Get finalized directory for a block pool
   * @param storageDir storage directory
   * @param bpid Block pool Id
   * @return finalized directory for a block pool
   */
  public static File getRbwDir(File storageDir, String bpid) {
    return new File(getBPDir(storageDir, bpid, Storage.STORAGE_DIR_CURRENT)
        + DataStorage.STORAGE_DIR_RBW );
  }
  
  /**
   * Get finalized directory for a block pool
   * @param storageDir storage directory
   * @param bpid Block pool Id
   * @return finalized directory for a block pool
   */
  public static File getFinalizedDir(File storageDir, String bpid) {
    return new File(getBPDir(storageDir, bpid, Storage.STORAGE_DIR_CURRENT)
        + DataStorage.STORAGE_DIR_FINALIZED );
  }
  
  /**
   * Get file correpsonding to a block
   * @param storageDir storage directory
   * @param blk block to be corrupted
   * @return file corresponding to the block
   */
  public static File getBlockFile(File storageDir, ExtendedBlock blk) {
    return new File(getFinalizedDir(storageDir, blk.getBlockPoolId()), 
        blk.getBlockName());
  }

  /**
   * Shut down a cluster if it is not null
   * @param cluster cluster reference or null
   */
  public static void shutdownCluster(MiniDFSCluster cluster) {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  /**
   * Get all files related to a block from all the datanodes
   * @param block block for which corresponding files are needed
   */
  public File[] getAllBlockFiles(ExtendedBlock block) {
    if (dataNodes.size() == 0) return new File[0];
    ArrayList<File> list = new ArrayList<File>();
    for (int i=0; i < dataNodes.size(); i++) {
      File blockFile = getBlockFile(i, block);
      if (blockFile != null) {
        list.add(blockFile);
      }
    }
    return list.toArray(new File[list.size()]);
  }
  
  /**
   * Get files related to a block for a given datanode
   * @param dnIndex Index of the datanode to get block files for
   * @param block block for which corresponding files are needed
   */
  public static File getBlockFile(int dnIndex, ExtendedBlock block) {
    // Check for block file in the two storage directories of the datanode
    for (int i = 0; i <=1 ; i++) {
      File storageDir = MiniDFSCluster.getStorageDir(dnIndex, i);
      File blockFile = getBlockFile(storageDir, block);
      if (blockFile.exists()) {
        return blockFile;
      }
    }
    return null;
  }
  
  /**
   * Throw an exception if the MiniDFSCluster is not started with a single
   * namenode
   */
  private void checkSingleNameNode() {
    if (nameNodes.length != 1) {
      throw new IllegalArgumentException("Namenode index is needed");
    }
  }

  /**
   * Add a namenode to a federated cluster and start it. Configuration of
   * datanodes in the cluster is refreshed to register with the new namenode.
   * 
   * @return newly started namenode
   */
  public NameNode addNameNode(Configuration conf, int namenodePort)
      throws IOException {
    if(!federation)
      throw new IOException("cannot add namenode to non-federated cluster");

    int nnIndex = nameNodes.length;
    int numNameNodes = nameNodes.length + 1;
    NameNodeInfo[] newlist = new NameNodeInfo[numNameNodes];
    System.arraycopy(nameNodes, 0, newlist, 0, nameNodes.length);
    nameNodes = newlist;
    String nameserviceId = NAMESERVICE_ID_PREFIX + (nnIndex + 1);
    
    String nameserviceIds = conf.get(DFS_FEDERATION_NAMESERVICES);
    nameserviceIds += "," + nameserviceId;
    conf.set(DFS_FEDERATION_NAMESERVICES, nameserviceIds);
  
    String nnId = null;
    initNameNodeAddress(conf, nameserviceId,
        new NNConf(nnId).setIpcPort(namenodePort));
    initNameNodeConf(conf, nameserviceId, nnId, true, nnIndex);
    createNameNode(nnIndex, conf, numDataNodes, true, null, null,
        nameserviceId, nnId);

    // Refresh datanodes with the newly started namenode
    for (DataNodeProperties dn : dataNodes) {
      DataNode datanode = dn.datanode;
      datanode.refreshNamenodes(conf);
    }

    // Wait for new namenode to get registrations from all the datanodes
    waitActive(nnIndex);
    return nameNodes[nnIndex].nameNode;
  }
  
  private int getFreeSocketPort() {
    int port = 0;
    try {
      ServerSocket s = new ServerSocket(0);
      port = s.getLocalPort();
      s.close();
      return port;
    } catch (IOException e) {
      // Could not get a free port. Return default port 0.
    }
    return port;
  }
  
  private void setupDatanodeAddress(Configuration conf, boolean setupHostsFile,
                           boolean checkDataNodeAddrConfig) throws IOException {
    if (setupHostsFile) {
      String hostsFile = conf.get(DFS_HOSTS, "").trim();
      if (hostsFile.length() == 0) {
        throw new IOException("Parameter dfs.hosts is not setup in conf");
      }
      // Setup datanode in the include file, if it is defined in the conf
      String address = "127.0.0.1:" + getFreeSocketPort();
      if (checkDataNodeAddrConfig) {
        conf.setIfUnset(DFS_DATANODE_ADDRESS_KEY, address);
      } else {
        conf.set(DFS_DATANODE_ADDRESS_KEY, address);
      }
      addToFile(hostsFile, address);
      LOG.info("Adding datanode " + address + " to hosts file " + hostsFile);
    } else {
      if (checkDataNodeAddrConfig) {
        conf.setIfUnset(DFS_DATANODE_ADDRESS_KEY, "127.0.0.1:0");
        conf.setIfUnset(DFS_DATANODE_HTTP_ADDRESS_KEY, "127.0.0.1:0");
        conf.setIfUnset(DFS_DATANODE_IPC_ADDRESS_KEY, "127.0.0.1:0");
      } else {
        conf.set(DFS_DATANODE_ADDRESS_KEY, "127.0.0.1:0");
        conf.set(DFS_DATANODE_HTTP_ADDRESS_KEY, "127.0.0.1:0");
        conf.set(DFS_DATANODE_IPC_ADDRESS_KEY, "127.0.0.1:0");
      }
    }
  }
  
  private void addToFile(String p, String address) throws IOException {
    File f = new File(p);
    f.createNewFile();
    PrintWriter writer = new PrintWriter(new FileWriter(f, true));
    try {
      writer.println(address);
    } finally {
      writer.close();
    }
  }
}
