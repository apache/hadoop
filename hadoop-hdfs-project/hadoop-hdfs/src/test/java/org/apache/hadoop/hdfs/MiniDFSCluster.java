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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HOSTS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICE_ID;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Supplier;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.MiniDFSNNTopology.NNConf;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.FsDatasetTestUtils;
import org.apache.hadoop.hdfs.server.datanode.FsDatasetTestUtils.MaterializedReplica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * This class creates a single-process DFS cluster for junit testing.
 * The data directories for non-simulated DFS are under the testing directory.
 * For simulated data nodes, no underlying fs storage is used.
 */
@InterfaceAudience.LimitedPrivate({"HBase", "HDFS", "Hive", "MapReduce", "Pig"})
@InterfaceStability.Unstable
public class MiniDFSCluster implements AutoCloseable {

  private static final String NAMESERVICE_ID_PREFIX = "nameserviceId";
  private static final Logger LOG =
      LoggerFactory.getLogger(MiniDFSCluster.class);
  /** System property to set the data dir: {@value} */
  public static final String PROP_TEST_BUILD_DATA =
      GenericTestUtils.SYSPROP_TEST_DATA_DIR;
  /** Configuration option to set the data dir: {@value} */
  public static final String HDFS_MINIDFS_BASEDIR = "hdfs.minidfs.basedir";
  /** Configuration option to set the provided data dir: {@value} */
  public static final String HDFS_MINIDFS_BASEDIR_PROVIDED =
      "hdfs.minidfs.basedir.provided";
  public static final String  DFS_NAMENODE_SAFEMODE_EXTENSION_TESTING_KEY
      = DFS_NAMENODE_SAFEMODE_EXTENSION_KEY + ".testing";
  public static final String  DFS_NAMENODE_DECOMMISSION_INTERVAL_TESTING_KEY
      = DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY + ".testing";

  // Changing this default may break some tests that assume it is 2.
  private static final int DEFAULT_STORAGES_PER_DATANODE = 2;

  static { DefaultMetricsSystem.setMiniClusterMode(true); }

  public int getStoragesPerDatanode() {
    return storagesPerDatanode;
  }

  /**
   * Class to construct instances of MiniDFSClusters with specific options.
   */
  public static class Builder {
    private int nameNodePort = 0;
    private int nameNodeHttpPort = 0;
    private final Configuration conf;
    private int numDataNodes = 1;
    private StorageType[][] storageTypes = null;
    private StorageType[] storageTypes1D = null;
    private int storagesPerDatanode = DEFAULT_STORAGES_PER_DATANODE;
    private boolean format = true;
    private boolean manageNameDfsDirs = true;
    private boolean manageNameDfsSharedDirs = true;
    private boolean enableManagedDfsDirsRedundancy = true;
    private boolean manageDataDfsDirs = true;
    private StartupOption option = null;
    private StartupOption dnOption = null;
    private String[] racks = null; 
    private String [] hosts = null;
    private long [] simulatedCapacities = null;
    private long [][] storageCapacities = null;
    private long [] storageCapacities1D = null;
    private String clusterId = null;
    private boolean waitSafeMode = true;
    private boolean setupHostsFile = false;
    private MiniDFSNNTopology nnTopology = null;
    private boolean checkExitOnShutdown = true;
    private boolean checkDataNodeAddrConfig = false;
    private boolean checkDataNodeHostConfig = false;
    private Configuration[] dnConfOverlays;
    private boolean skipFsyncForTesting = true;
    private boolean useConfiguredTopologyMappingClass = false;

    public Builder(Configuration conf) {
      this.conf = conf;
      this.storagesPerDatanode =
          FsDatasetTestUtils.Factory.getFactory(conf).getDefaultNumOfDataDirs();
      if (null == conf.get(HDFS_MINIDFS_BASEDIR)) {
        conf.set(HDFS_MINIDFS_BASEDIR,
            new File(getBaseDirectory()).getAbsolutePath());
      }
    }

    public Builder(Configuration conf, File basedir) {
      this.conf = conf;
      this.storagesPerDatanode =
          FsDatasetTestUtils.Factory.getFactory(conf).getDefaultNumOfDataDirs();
      if (null == basedir) {
        throw new IllegalArgumentException(
            "MiniDFSCluster base directory cannot be null");
      }
      String cdir = conf.get(HDFS_MINIDFS_BASEDIR);
      if (cdir != null) {
        throw new IllegalArgumentException(
            "MiniDFSCluster base directory already defined (" + cdir + ")");
      }
      conf.set(HDFS_MINIDFS_BASEDIR, basedir.getAbsolutePath());
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
     * Default: DEFAULT_STORAGES_PER_DATANODE
     */
    public Builder storagesPerDatanode(int numStorages) {
      this.storagesPerDatanode = numStorages;
      return this;
    }

    /**
     * Set the same storage type configuration for each datanode.
     * If storageTypes is uninitialized or passed null then
     * StorageType.DEFAULT is used.
     */
    public Builder storageTypes(StorageType[] types) {
      this.storageTypes1D = types;
      return this;
    }

    /**
     * Set custom storage type configuration for each datanode.
     * If storageTypes is uninitialized or passed null then
     * StorageType.DEFAULT is used.
     */
    public Builder storageTypes(StorageType[][] types) {
      this.storageTypes = types;
      return this;
    }

    /**
     * Set the same storage capacity configuration for each datanode.
     * If storageTypes is uninitialized or passed null then
     * StorageType.DEFAULT is used.
     */
    public Builder storageCapacities(long[] capacities) {
      this.storageCapacities1D = capacities;
      return this;
    }

    /**
     * Set custom storage capacity configuration for each datanode.
     * If storageCapacities is uninitialized or passed null then
     * capacity is limited by available disk space.
     */
    public Builder storageCapacities(long[][] capacities) {
      this.storageCapacities = capacities;
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
    public Builder manageNameDfsSharedDirs(boolean val) {
      this.manageNameDfsSharedDirs = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder enableManagedDfsDirsRedundancy(boolean val) {
      this.enableManagedDfsDirsRedundancy = val;
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
    public Builder dnStartupOption(StartupOption val) {
      this.dnOption = val;
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
     * Use SimulatedFSDataset and limit the capacity of each DN per
     * the values passed in val.
     *
     * For limiting the capacity of volumes with real storage, see
     * {@link FsVolumeImpl#setCapacityForTesting}
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
     * Default: true
     */
    public Builder checkExitOnShutdown(boolean val) {
      this.checkExitOnShutdown = val;
      return this;
    }

    /**
     * Default: false
     */
    public Builder checkDataNodeAddrConfig(boolean val) {
      this.checkDataNodeAddrConfig = val;
      return this;
    }

    /**
     * Default: false
     */
    public Builder checkDataNodeHostConfig(boolean val) {
      this.checkDataNodeHostConfig = val;
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
     * Default: null
     * 
     * An array of {@link Configuration} objects that will overlay the
     * global MiniDFSCluster Configuration for the corresponding DataNode.
     * 
     * Useful for setting specific per-DataNode configuration parameters.
     */
    public Builder dataNodeConfOverlays(Configuration[] dnConfOverlays) {
      this.dnConfOverlays = dnConfOverlays;
      return this;
    }

    /**
     * Default: true
     * When true, we skip fsync() calls for speed improvements.
     */
    public Builder skipFsyncForTesting(boolean val) {
      this.skipFsyncForTesting = val;
      return this;
    }

    public Builder useConfiguredTopologyMappingClass(
        boolean useConfiguredTopologyMappingClass) {
      this.useConfiguredTopologyMappingClass =
          useConfiguredTopologyMappingClass;
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
  protected MiniDFSCluster(Builder builder) throws IOException {
    if (builder.nnTopology == null) {
      // If no topology is specified, build a single NN. 
      builder.nnTopology = MiniDFSNNTopology.simpleSingleNN(
          builder.nameNodePort, builder.nameNodeHttpPort);
    }
    assert builder.storageTypes == null ||
           builder.storageTypes.length == builder.numDataNodes;
    final int numNameNodes = builder.nnTopology.countNameNodes();
    LOG.info("starting cluster: numNameNodes=" + numNameNodes
        + ", numDataNodes=" + builder.numDataNodes);

    this.storagesPerDatanode = builder.storagesPerDatanode;

    // Duplicate the storageType setting for each DN.
    if (builder.storageTypes == null && builder.storageTypes1D != null) {
      assert builder.storageTypes1D.length == storagesPerDatanode;
      builder.storageTypes = new StorageType[builder.numDataNodes][storagesPerDatanode];
      
      for (int i = 0; i < builder.numDataNodes; ++i) {
        builder.storageTypes[i] = builder.storageTypes1D;
      }
    }

    // Duplicate the storageCapacity setting for each DN.
    if (builder.storageCapacities == null && builder.storageCapacities1D != null) {
      assert builder.storageCapacities1D.length == storagesPerDatanode;
      builder.storageCapacities = new long[builder.numDataNodes][storagesPerDatanode];

      for (int i = 0; i < builder.numDataNodes; ++i) {
        builder.storageCapacities[i] = builder.storageCapacities1D;
      }
    }

    initMiniDFSCluster(builder.conf,
                       builder.numDataNodes,
                       builder.storageTypes,
                       builder.format,
                       builder.manageNameDfsDirs,
                       builder.manageNameDfsSharedDirs,
                       builder.enableManagedDfsDirsRedundancy,
                       builder.manageDataDfsDirs,
                       builder.option,
                       builder.dnOption,
                       builder.racks,
                       builder.hosts,
                       builder.storageCapacities,
                       builder.simulatedCapacities,
                       builder.clusterId,
                       builder.waitSafeMode,
                       builder.setupHostsFile,
                       builder.nnTopology,
                       builder.checkExitOnShutdown,
                       builder.checkDataNodeAddrConfig,
                       builder.checkDataNodeHostConfig,
                       builder.dnConfOverlays,
                       builder.skipFsyncForTesting,
                       builder.useConfiguredTopologyMappingClass);
  }
  
  public class DataNodeProperties {
    final DataNode datanode;
    final Configuration conf;
    String[] dnArgs;
    final SecureResources secureResources;
    final int ipcPort;

    DataNodeProperties(DataNode node, Configuration conf, String[] args,
                       SecureResources secureResources, int ipcPort) {
      this.datanode = node;
      this.conf = conf;
      this.dnArgs = args;
      this.secureResources = secureResources;
      this.ipcPort = ipcPort;
    }

    public void setDnArgs(String ... args) {
      dnArgs = args;
    }

    public DataNode getDatanode() {
      return datanode;
    }

  }

  private Configuration conf;
  private Multimap<String, NameNodeInfo> namenodes = ArrayListMultimap.create();
  protected int numDataNodes;
  protected final List<DataNodeProperties> dataNodes =
                         new ArrayList<DataNodeProperties>();
  private File base_dir;
  private File data_dir;
  private boolean waitSafeMode = true;
  private boolean federation;
  private boolean checkExitOnShutdown = true;
  protected final int storagesPerDatanode;
  private Set<FileSystem> fileSystems = Sets.newHashSet();

  private List<long[]> storageCap = Lists.newLinkedList();

  /**
   * A unique instance identifier for the cluster. This
   * is used to disambiguate HA filesystems in the case where
   * multiple MiniDFSClusters are used in the same test suite. 
   */
  private int instanceId;
  private static int instanceCount = 0;
  
  /**
   * Stores the information related to a namenode in the cluster
   */
  public static class NameNodeInfo {
    public NameNode nameNode;
    Configuration conf;
    String nameserviceId;
    String nnId;
    StartupOption startOpt;
    NameNodeInfo(NameNode nn, String nameserviceId, String nnId,
        StartupOption startOpt, Configuration conf) {
      this.nameNode = nn;
      this.nameserviceId = nameserviceId;
      this.nnId = nnId;
      this.startOpt = startOpt;
      this.conf = conf;
    }
    
    public void setStartOpt(StartupOption startOpt) {
      this.startOpt = startOpt;
    }

    public String getNameserviceId() {
      return this.nameserviceId;
    }

    public String getNamenodeId() {
      return this.nnId;
    }
  }
  
  /**
   * This null constructor is used only when wishing to start a data node cluster
   * without a name node (ie when the name node is started elsewhere).
   */
  public MiniDFSCluster() {
    storagesPerDatanode = DEFAULT_STORAGES_PER_DATANODE;
    synchronized (MiniDFSCluster.class) {
      instanceId = instanceCount++;
    }
  }
  
  /**
   * Modify the config and start up the servers with the given operation.
   * Servers will be started on free ports.
   * <p>
   * The caller must manage the creation of NameNode and DataNode directories
   * and have already set {@link DFSConfigKeys#DFS_NAMENODE_NAME_DIR_KEY} and
   * {@link DFSConfigKeys#DFS_DATANODE_DATA_DIR_KEY} in the given conf.
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
    this(0, conf, numDataNodes, format, true, true, null,
        racks, null, null);
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
    this(0, conf, numDataNodes, format, true, true, null,
        racks, hosts, null);
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
   *          created and {@link DFSConfigKeys#DFS_NAMENODE_NAME_DIR_KEY} and
   *          {@link DFSConfigKeys#DFS_DATANODE_DATA_DIR_KEY} will be set in
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
    this(nameNodePort, conf, numDataNodes, format, manageDfsDirs,
        manageDfsDirs, operation, racks, null, null);
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
   *          created and {@link DFSConfigKeys#DFS_NAMENODE_NAME_DIR_KEY} and
   *          {@link DFSConfigKeys#DFS_DATANODE_DATA_DIR_KEY} will be set in
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
    this(nameNodePort, conf, numDataNodes, format, manageDfsDirs, 
        manageDfsDirs, operation, racks, null, simulatedCapacities);
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
   *          created and {@link DFSConfigKeys#DFS_NAMENODE_NAME_DIR_KEY} and
   *          {@link DFSConfigKeys#DFS_DATANODE_DATA_DIR_KEY} will be set in
   *          the conf
   * @param manageDataDfsDirs if true, the data directories for datanodes will
   *          be created and {@link DFSConfigKeys#DFS_DATANODE_DATA_DIR_KEY}
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
    this.storagesPerDatanode = DEFAULT_STORAGES_PER_DATANODE;
    initMiniDFSCluster(conf, numDataNodes, null, format,
                       manageNameDfsDirs, true, manageDataDfsDirs, manageDataDfsDirs,
                       operation, null, racks, hosts,
                       null, simulatedCapacities, null, true, false,
                       MiniDFSNNTopology.simpleSingleNN(nameNodePort, 0),
                       true, false, false, null, true, false);
  }

  private void initMiniDFSCluster(
      Configuration conf,
      int numDataNodes, StorageType[][] storageTypes, boolean format,
      boolean manageNameDfsDirs,
      boolean manageNameDfsSharedDirs, boolean enableManagedDfsDirsRedundancy,
      boolean manageDataDfsDirs, StartupOption startOpt,
      StartupOption dnStartOpt, String[] racks,
      String[] hosts,
      long[][] storageCapacities, long[] simulatedCapacities, String clusterId,
      boolean waitSafeMode, boolean setupHostsFile,
      MiniDFSNNTopology nnTopology, boolean checkExitOnShutdown,
      boolean checkDataNodeAddrConfig,
      boolean checkDataNodeHostConfig,
      Configuration[] dnConfOverlays,
      boolean skipFsyncForTesting,
      boolean useConfiguredTopologyMappingClass)
  throws IOException {
    boolean success = false;
    try {
      ExitUtil.disableSystemExit();

      // Re-enable symlinks for tests, see HADOOP-10020 and HADOOP-10052
      FileSystem.enableSymlinks();

      synchronized (MiniDFSCluster.class) {
        instanceId = instanceCount++;
      }

      this.conf = conf;
      base_dir = new File(determineDfsBaseDir());
      data_dir = new File(base_dir, "data");
      this.waitSafeMode = waitSafeMode;
      this.checkExitOnShutdown = checkExitOnShutdown;
    
      int replication = conf.getInt(DFS_REPLICATION_KEY, 3);
      conf.setInt(DFS_REPLICATION_KEY, Math.min(replication, numDataNodes));
      int maintenanceMinReplication = conf.getInt(
          DFSConfigKeys.DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_KEY,
          DFSConfigKeys.DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_DEFAULT);
      if (maintenanceMinReplication ==
          DFSConfigKeys.DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_DEFAULT) {
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_KEY,
            Math.min(maintenanceMinReplication, numDataNodes));
      }
      int safemodeExtension = conf.getInt(
          DFS_NAMENODE_SAFEMODE_EXTENSION_TESTING_KEY, 0);
      conf.setInt(DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, safemodeExtension);
      long decommissionInterval = conf.getTimeDuration(
          DFS_NAMENODE_DECOMMISSION_INTERVAL_TESTING_KEY, 3, TimeUnit.SECONDS);
      conf.setTimeDuration(DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY,
          decommissionInterval, TimeUnit.SECONDS);
      if (!useConfiguredTopologyMappingClass) {
        conf.setClass(NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            StaticMapping.class, DNSToSwitchMapping.class);
      }

      // In an HA cluster, in order for the StandbyNode to perform checkpoints,
      // it needs to know the HTTP port of the Active. So, if ephemeral ports
      // are chosen, disable checkpoints for the test.
      if (!nnTopology.allHttpPortsSpecified() &&
          nnTopology.isHA()) {
        LOG.info("MiniDFSCluster disabling checkpointing in the Standby node " +
            "since no HTTP ports have been specified.");
        conf.setBoolean(DFS_HA_STANDBY_CHECKPOINTS_KEY, false);
      }
      if (!nnTopology.allIpcPortsSpecified() &&
          nnTopology.isHA()) {
        LOG.info("MiniDFSCluster disabling log-roll triggering in the "
            + "Standby node since no IPC ports have been specified.");
        conf.setInt(DFS_HA_LOGROLL_PERIOD_KEY, -1);
      }

      EditLogFileOutputStream.setShouldSkipFsyncForTesting(skipFsyncForTesting);
    
      federation = nnTopology.isFederated();
      try {
        createNameNodesAndSetConf(
            nnTopology, manageNameDfsDirs, manageNameDfsSharedDirs,
            enableManagedDfsDirsRedundancy,
            format, startOpt, clusterId);
      } catch (IOException ioe) {
        LOG.error("IOE creating namenodes. Permissions dump:\n" +
            createPermissionsDiagnosisString(data_dir), ioe);
        throw ioe;
      }
      if (format) {
        if (data_dir.exists() && !FileUtil.fullyDelete(data_dir)) {
          throw new IOException("Cannot remove data directory: " + data_dir +
              createPermissionsDiagnosisString(data_dir));
        }
      }
    
      if (startOpt == StartupOption.RECOVER) {
        return;
      }

      // Start the DataNodes
      startDataNodes(conf, numDataNodes, storageTypes, manageDataDfsDirs,
          dnStartOpt != null ? dnStartOpt : startOpt,
          racks, hosts, storageCapacities, simulatedCapacities, setupHostsFile,
          checkDataNodeAddrConfig, checkDataNodeHostConfig, dnConfOverlays);
      waitClusterUp();
      //make sure ProxyUsers uses the latest conf
      ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
      success = true;
    } finally {
      if (!success) {
        shutdown();
      }
    }
  }
  
  /**
   * @return a debug string which can help diagnose an error of why
   * a given directory might have a permissions error in the context
   * of a test case
   */
  private String createPermissionsDiagnosisString(File path) {
    StringBuilder sb = new StringBuilder();
    while (path != null) { 
      sb.append("path '" + path + "': ").append("\n");
      sb.append("\tabsolute:").append(path.getAbsolutePath()).append("\n");
      sb.append("\tpermissions: ");
      sb.append(path.isDirectory() ? "d": "-");
      sb.append(FileUtil.canRead(path) ? "r" : "-");
      sb.append(FileUtil.canWrite(path) ? "w" : "-");
      sb.append(FileUtil.canExecute(path) ? "x" : "-");
      sb.append("\n");
      path = path.getParentFile();
    }
    return sb.toString();
  }

  private void createNameNodesAndSetConf(MiniDFSNNTopology nnTopology,
      boolean manageNameDfsDirs, boolean manageNameDfsSharedDirs,
      boolean enableManagedDfsDirsRedundancy, boolean format,
      StartupOption operation, String clusterId) throws IOException {
    // do the basic namenode configuration
    configureNameNodes(nnTopology, federation, conf);

    int nnCounter = 0;
    int nsCounter = 0;
    // configure each NS independently
    for (MiniDFSNNTopology.NSConf nameservice : nnTopology.getNameservices()) {
      configureNameService(nameservice, nsCounter++, manageNameDfsSharedDirs,
          manageNameDfsDirs, enableManagedDfsDirsRedundancy,
          format, operation, clusterId, nnCounter);
      nnCounter += nameservice.getNNs().size();
    }

    for (NameNodeInfo nn : namenodes.values()) {
      Configuration nnConf = nn.conf;
      for (NameNodeInfo nnInfo : namenodes.values()) {
        if (nn.equals(nnInfo)) {
          continue;
        }
       copyKeys(conf, nnConf, nnInfo.nameserviceId, nnInfo.nnId);
      }
    }
  }

  private static void copyKeys(Configuration srcConf, Configuration destConf,
      String nameserviceId, String nnId) {
    String key = DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY,
      nameserviceId, nnId);
    destConf.set(key, srcConf.get(key));

    copyKey(srcConf, destConf, nameserviceId, nnId,
        DFS_NAMENODE_HTTP_ADDRESS_KEY);
    copyKey(srcConf, destConf, nameserviceId, nnId,
        DFS_NAMENODE_HTTPS_ADDRESS_KEY);
    copyKey(srcConf, destConf, nameserviceId, nnId,
        DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY);
    copyKey(srcConf, destConf, nameserviceId, nnId,
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY);
  }

  private static void copyKey(Configuration srcConf, Configuration destConf,
      String nameserviceId, String nnId, String baseKey) {
    String key = DFSUtil.addKeySuffixes(baseKey, nameserviceId, nnId);
    String val = srcConf.get(key);
    if (val != null) {
      destConf.set(key, srcConf.get(key));
    }
  }

  /**
   * Do the rest of the NN configuration for things like shared edits,
   * as well as directory formatting, etc. for a single nameservice
   * @param nnCounter the count of the number of namenodes already configured/started. Also,
   *                  acts as the <i>index</i> to the next NN to start (since indicies start at 0).
   * @throws IOException
   */
  private void configureNameService(MiniDFSNNTopology.NSConf nameservice, int nsCounter,
      boolean manageNameDfsSharedDirs, boolean manageNameDfsDirs, boolean
      enableManagedDfsDirsRedundancy, boolean format,
      StartupOption operation, String clusterId,
      final int nnCounter) throws IOException{
    String nsId = nameservice.getId();
    String lastDefaultFileSystem = null;

    // If HA is enabled on this nameservice, enumerate all the namenodes
    // in the configuration. Also need to set a shared edits dir
    int numNNs = nameservice.getNNs().size();
    if (numNNs > 1 && manageNameDfsSharedDirs) {
      URI sharedEditsUri = getSharedEditsDir(nnCounter, nnCounter + numNNs - 1);
      conf.set(DFS_NAMENODE_SHARED_EDITS_DIR_KEY, sharedEditsUri.toString());
      // Clean out the shared edits dir completely, including all subdirectories.
      FileUtil.fullyDelete(new File(sharedEditsUri));
    }

    // Now format first NN and copy the storage directory from that node to the others.
    int nnIndex = nnCounter;
    Collection<URI> prevNNDirs = null;
    for (NNConf nn : nameservice.getNNs()) {
      initNameNodeConf(conf, nsId, nsCounter, nn.getNnId(), manageNameDfsDirs,
          manageNameDfsDirs,  nnIndex);
      Collection<URI> namespaceDirs = FSNamesystem.getNamespaceDirs(conf);
      if (format) {
        // delete the existing namespaces
        for (URI nameDirUri : namespaceDirs) {
          File nameDir = new File(nameDirUri);
          if (nameDir.exists() && !FileUtil.fullyDelete(nameDir)) {
            throw new IOException("Could not fully delete " + nameDir);
          }
        }

        // delete the checkpoint directories, if they exist
        Collection<URI> checkpointDirs = Util.stringCollectionAsURIs(conf
            .getTrimmedStringCollection(DFS_NAMENODE_CHECKPOINT_DIR_KEY));
        for (URI checkpointDirUri : checkpointDirs) {
          File checkpointDir = new File(checkpointDirUri);
          if (checkpointDir.exists() && !FileUtil.fullyDelete(checkpointDir)) {
            throw new IOException("Could not fully delete " + checkpointDir);
          }
        }
      }

      boolean formatThisOne = format;
      // if we are looking at not the first NN
      if (nnIndex++ > nnCounter && format) {
        // Don't format the second, third, etc NN in an HA setup - that
        // would result in it having a different clusterID,
        // block pool ID, etc. Instead, copy the name dirs
        // from the previous one.
        formatThisOne = false;
        assert (null != prevNNDirs);
        copyNameDirs(prevNNDirs, namespaceDirs, conf);
      }

      if (formatThisOne) {
        // Allow overriding clusterID for specific NNs to test
        // misconfiguration.
        if (nn.getClusterId() == null) {
          StartupOption.FORMAT.setClusterId(clusterId);
        } else {
          StartupOption.FORMAT.setClusterId(nn.getClusterId());
        }
        DFSTestUtil.formatNameNode(conf);
      }
      prevNNDirs = namespaceDirs;
    }

    // create all the namenodes in the namespace
    nnIndex = nnCounter;
    for (NNConf nn : nameservice.getNNs()) {
      Configuration hdfsConf = new Configuration(conf);
      initNameNodeConf(hdfsConf, nsId, nsCounter, nn.getNnId(), manageNameDfsDirs,
          enableManagedDfsDirsRedundancy, nnIndex++);
      createNameNode(hdfsConf, false, operation,
          clusterId, nsId, nn.getNnId());
      // Record the last namenode uri
      lastDefaultFileSystem = hdfsConf.get(FS_DEFAULT_NAME_KEY);
    }
    if (!federation && lastDefaultFileSystem != null) {
      // Set the default file system to the actual bind address of NN.
      conf.set(FS_DEFAULT_NAME_KEY, lastDefaultFileSystem);
    }
  }

  /**
   * Do the basic NN configuration for the topology. Does not configure things like the shared
   * edits directories
   * @param nnTopology
   * @param federation
   * @param conf
   * @throws IOException
   */
  public static void configureNameNodes(MiniDFSNNTopology nnTopology, boolean federation,
      Configuration conf) throws IOException {
    Preconditions.checkArgument(nnTopology.countNameNodes() > 0,
        "empty NN topology: no namenodes specified!");

    if (!federation && nnTopology.countNameNodes() == 1) {
      NNConf onlyNN = nnTopology.getOnlyNameNode();
      // we only had one NN, set DEFAULT_NAME for it. If not explicitly
      // specified initially, the port will be 0 to make NN bind to any
      // available port. It will be set to the right address after
      // NN is started.
      conf.set(FS_DEFAULT_NAME_KEY, "hdfs://127.0.0.1:" + onlyNN.getIpcPort());
    }

    List<String> allNsIds = Lists.newArrayList();
    for (MiniDFSNNTopology.NSConf nameservice : nnTopology.getNameservices()) {
      if (nameservice.getId() != null) {
        allNsIds.add(nameservice.getId());
      }
    }

    if (!allNsIds.isEmpty()) {
      conf.set(DFS_NAMESERVICES, Joiner.on(",").join(allNsIds));
    }

    for (MiniDFSNNTopology.NSConf nameservice : nnTopology.getNameservices()) {
      String nsId = nameservice.getId();

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
        conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX, nameservice.getId()), Joiner
            .on(",").join(nnIds));
      }
    }
  }
  
  public URI getSharedEditsDir(int minNN, int maxNN) throws IOException {
    return formatSharedEditsDir(base_dir, minNN, maxNN);
  }
  
  public static URI formatSharedEditsDir(File baseDir, int minNN, int maxNN)
      throws IOException {
    return fileAsURI(new File(baseDir, "shared-edits-" +
        minNN + "-through-" + maxNN));
  }
  
  public NameNodeInfo[] getNameNodeInfos() {
    return this.namenodes.values().toArray(new NameNodeInfo[0]);
  }

  /**
   * @param nsIndex index of the namespace id to check
   * @return all the namenodes bound to the given namespace index
   */
  public NameNodeInfo[] getNameNodeInfos(int nsIndex) {
    int i = 0;
    for (String ns : this.namenodes.keys()) {
      if (i++ == nsIndex) {
        return this.namenodes.get(ns).toArray(new NameNodeInfo[0]);
      }
    }
    return null;
  }

  /**
   * @param nameservice id of nameservice to read
   * @return all the namenodes bound to the given namespace index
   */
  public NameNodeInfo[] getNameNodeInfos(String nameservice) {
    for (String ns : this.namenodes.keys()) {
      if (nameservice.equals(ns)) {
        return this.namenodes.get(ns).toArray(new NameNodeInfo[0]);
      }
    }
    return null;
  }


  protected void initNameNodeConf(Configuration conf, String nameserviceId, int nsIndex, String nnId,
      boolean manageNameDfsDirs, boolean enableManagedDfsDirsRedundancy, int nnIndex)
      throws IOException {
    if (nameserviceId != null) {
      conf.set(DFS_NAMESERVICE_ID, nameserviceId);
    }
    if (nnId != null) {
      conf.set(DFS_HA_NAMENODE_ID_KEY, nnId);
    }
    if (manageNameDfsDirs) {
      if (enableManagedDfsDirsRedundancy) {
        File[] files = getNameNodeDirectory(nsIndex, nnIndex);
        conf.set(DFS_NAMENODE_NAME_DIR_KEY, fileAsURI(files[0]) + "," + fileAsURI(files[1]));
        files = getCheckpointDirectory(nsIndex, nnIndex);
        conf.set(DFS_NAMENODE_CHECKPOINT_DIR_KEY, fileAsURI(files[0]) + "," + fileAsURI(files[1]));
      } else {
        File[] files = getNameNodeDirectory(nsIndex, nnIndex);
        conf.set(DFS_NAMENODE_NAME_DIR_KEY, fileAsURI(files[0]).toString());
        files = getCheckpointDirectory(nsIndex, nnIndex);
        conf.set(DFS_NAMENODE_CHECKPOINT_DIR_KEY, fileAsURI(files[0]).toString());
      }
    }
  }

  private File[] getNameNodeDirectory(int nameserviceIndex, int nnIndex) {
    return getNameNodeDirectory(base_dir, nameserviceIndex, nnIndex);
  }

  public static File[] getNameNodeDirectory(String base_dir, int nsIndex, int nnIndex) {
    return getNameNodeDirectory(new File(base_dir), nsIndex, nnIndex);
  }

  public static File[] getNameNodeDirectory(File base_dir, int nsIndex, int nnIndex) {
    File[] files = new File[2];
    files[0] = new File(base_dir, "name-" + nsIndex + "-" + (2 * nnIndex + 1));
    files[1] = new File(base_dir, "name-" + nsIndex + "-" + (2 * nnIndex + 2));
    return files;
  }

  public File[] getCheckpointDirectory(int nsIndex, int nnIndex) {
    return getCheckpointDirectory(base_dir, nsIndex, nnIndex);
  }

  public static File[] getCheckpointDirectory(String base_dir, int nsIndex, int nnIndex) {
    return getCheckpointDirectory(new File(base_dir), nsIndex, nnIndex);
  }

  public static File[] getCheckpointDirectory(File base_dir, int nsIndex, int nnIndex) {
    File[] files = new File[2];
    files[0] = new File(base_dir, "namesecondary-" + nsIndex + "-" + (2 * nnIndex + 1));
    files[1] = new File(base_dir, "namesecondary-" + nsIndex + "-" + (2 * nnIndex + 2));
    return files;
  }


  public static void copyNameDirs(Collection<URI> srcDirs, Collection<URI> dstDirs,
      Configuration dstConf) throws IOException {
    URI srcDir = Lists.newArrayList(srcDirs).get(0);
    FileSystem dstFS = FileSystem.getLocal(dstConf).getRaw();
    for (URI dstDir : dstDirs) {
      Preconditions.checkArgument(!dstDir.equals(srcDir),
          "src and dst are the same: " + dstDir);
      File dstDirF = new File(dstDir);
      if (dstDirF.exists()) {
        if (!FileUtil.fullyDelete(dstDirF)) {
          throw new IOException("Unable to delete: " + dstDirF);
        }
      }
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
  
  private static String[] createArgs(StartupOption operation) {
    if (operation == StartupOption.ROLLINGUPGRADE) {
      return new String[]{operation.getName(),
          operation.getRollingUpgradeStartupOption().name()};
    }
    String[] args = (operation == null ||
        operation == StartupOption.FORMAT ||
        operation == StartupOption.REGULAR) ?
            new String[] {} : new String[] {operation.getName()};
    return args;
  }

  private void createNameNode(Configuration hdfsConf, boolean format, StartupOption operation,
      String clusterId, String nameserviceId, String nnId) throws IOException {
    // Format and clean out DataNode directories
    if (format) {
      DFSTestUtil.formatNameNode(hdfsConf);
    }
    if (operation == StartupOption.UPGRADE){
      operation.setClusterId(clusterId);
    }

    String[] args = createArgs(operation);
    NameNode nn =  NameNode.createNameNode(args, hdfsConf);
    if (operation == StartupOption.RECOVER) {
      return;
    }
    // After the NN has started, set back the bound ports into
    // the conf
    hdfsConf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY,
        nameserviceId, nnId), nn.getNameNodeAddressHostPortString());
    if (nn.getHttpAddress() != null) {
      hdfsConf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_HTTP_ADDRESS_KEY,
          nameserviceId, nnId), NetUtils.getHostPortString(nn.getHttpAddress()));
    }
    if (nn.getHttpsAddress() != null) {
      hdfsConf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_HTTPS_ADDRESS_KEY,
          nameserviceId, nnId), NetUtils.getHostPortString(nn.getHttpsAddress()));
    }
    if (hdfsConf.get(DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY) != null) {
      hdfsConf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY,
          nameserviceId, nnId),
          hdfsConf.get(DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY));
    }
    copyKeys(hdfsConf, conf, nameserviceId, nnId);
    DFSUtil.setGenericConf(hdfsConf, nameserviceId, nnId,
        DFS_NAMENODE_HTTP_ADDRESS_KEY);
    NameNodeInfo info = new NameNodeInfo(nn, nameserviceId, nnId,
        operation, hdfsConf);
    namenodes.put(nameserviceId, info);
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
    String hostPort =
        getNN(nnIndex).nameNode.getNameNodeAddressHostPortString();
    URI uri = null;
    try {
      uri = new URI("hdfs://" + hostPort);
    } catch (URISyntaxException e) {
      NameNode.LOG.warn("unexpected URISyntaxException", e);
    }
    return uri;
  }
  
  public int getInstanceId() {
    return instanceId;
  }

  /**
   * @return Configuration of for the given namenode
   */
  public Configuration getConfiguration(int nnIndex) {
    return getNN(nnIndex).conf;
  }

  private NameNodeInfo getNN(int nnIndex) {
    int count = 0;
    for (NameNodeInfo nn : namenodes.values()) {
      if (count == nnIndex) {
        return nn;
      }
      count++;
    }
    return null;
  }

  public List<Integer> getNNIndexes(String nameserviceId) {
    int count = 0;
    List<Integer> nnIndexes = new ArrayList<>();
    for (NameNodeInfo nn : namenodes.values()) {
      if (nn.getNameserviceId().equals(nameserviceId)) {
        nnIndexes.add(count);
      }
      count++;
    }
    return nnIndexes;
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
  public void waitClusterUp() throws IOException {
    int i = 0;
    if (numDataNodes > 0) {
      while (!isClusterUp()) {
        try {
          LOG.warn("Waiting for the Mini HDFS Cluster to start...");
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        if (++i > 10) {
          final String msg = "Timed out waiting for Mini HDFS Cluster to start";
          LOG.error(msg);
          throw new IOException(msg);
        }
      }
    }
  }

  String makeDataNodeDirs(int dnIndex, StorageType[] storageTypes) throws IOException {
    StringBuilder sb = new StringBuilder();
    for (int j = 0; j < storagesPerDatanode; ++j) {
      if ((storageTypes != null) && (j >= storageTypes.length)) {
        break;
      }
      File dir;
      if (storageTypes != null && storageTypes[j] == StorageType.PROVIDED) {
        dir = getProvidedStorageDir(dnIndex, j);
      } else {
        dir = getInstanceStorageDir(dnIndex, j);
      }
      dir.mkdirs();
      if (!dir.isDirectory()) {
        throw new IOException("Mkdirs failed to create directory for DataNode " + dir);
      }
      sb.append((j > 0 ? "," : "") + "[" +
          (storageTypes == null ? StorageType.DEFAULT : storageTypes[j]) +
          "]" + fileAsURI(dir));
    }
    return sb.toString();
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
   *          created and {@link DFSConfigKeys#DFS_DATANODE_DATA_DIR_KEY} will be set
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
   *          created and {@link DFSConfigKeys#DFS_DATANODE_DATA_DIR_KEY} will be
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
    startDataNodes(conf, numDataNodes, null, manageDfsDirs, operation, racks, hosts,
        null, simulatedCapacities, setupHostsFile, false, false, null);
  }

  public synchronized void startDataNodes(Configuration conf, int numDataNodes,
      boolean manageDfsDirs, StartupOption operation, 
      String[] racks, String[] hosts,
      long[] simulatedCapacities,
      boolean setupHostsFile,
      boolean checkDataNodeAddrConfig) throws IOException {
    startDataNodes(conf, numDataNodes, null, manageDfsDirs, operation, racks, hosts,
        null, simulatedCapacities, setupHostsFile, checkDataNodeAddrConfig, false, null);
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
   *          created and {@link DFSConfigKeys#DFS_DATANODE_DATA_DIR_KEY} will be
   *          set in the conf
   * @param operation the operation with which to start the DataNodes.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostnames for each DataNode
   * @param simulatedCapacities array of capacities of the simulated data nodes
   * @param setupHostsFile add new nodes to dfs hosts files
   * @param checkDataNodeAddrConfig if true, only set DataNode port addresses if not already set in config
   * @param checkDataNodeHostConfig if true, only set DataNode hostname key if not already set in config
   * @param dnConfOverlays An array of {@link Configuration} objects that will overlay the
   *              global MiniDFSCluster Configuration for the corresponding DataNode.
   * @throws IllegalStateException if NameNode has been shutdown
   */
  public synchronized void startDataNodes(Configuration conf, int numDataNodes,
      StorageType[][] storageTypes, boolean manageDfsDirs, StartupOption operation,
      String[] racks, String[] hosts,
      long[][] storageCapacities,
      long[] simulatedCapacities,
      boolean setupHostsFile,
      boolean checkDataNodeAddrConfig,
      boolean checkDataNodeHostConfig,
      Configuration[] dnConfOverlays) throws IOException {
    assert storageCapacities == null || simulatedCapacities == null;
    assert storageTypes == null || storageTypes.length == numDataNodes;
    assert storageCapacities == null || storageCapacities.length == numDataNodes;

    if (operation == StartupOption.RECOVER) {
      return;
    }
    if (checkDataNodeHostConfig) {
      conf.setIfUnset(DFS_DATANODE_HOST_NAME_KEY, "127.0.0.1");
    } else {
      conf.set(DFS_DATANODE_HOST_NAME_KEY, "127.0.0.1");
    }

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
    
    if (dnConfOverlays != null
        && numDataNodes > dnConfOverlays.length) {
      throw new IllegalArgumentException( "The length of dnConfOverlays [" 
          + dnConfOverlays.length
          + "] is less than the number of datanodes [" + numDataNodes + "].");
    }

    String [] dnArgs = (operation == null ||
                        operation != StartupOption.ROLLBACK) ?
        null : new String[] {operation.getName()};
    
    DataNode[] dns = new DataNode[numDataNodes];
    for (int i = curDatanodesNum; i < curDatanodesNum+numDataNodes; i++) {
      Configuration dnConf = new HdfsConfiguration(conf);
      if (dnConfOverlays != null) {
        dnConf.addResource(dnConfOverlays[i]);
      }
      // Set up datanode address
      setupDatanodeAddress(dnConf, setupHostsFile, checkDataNodeAddrConfig);
      if (manageDfsDirs) {
        String dirs = makeDataNodeDirs(i, storageTypes == null ?
          null : storageTypes[i - curDatanodesNum]);
        dnConf.set(DFS_DATANODE_DATA_DIR_KEY, dirs);
        conf.set(DFS_DATANODE_DATA_DIR_KEY, dirs);
      }
      if (simulatedCapacities != null) {
        SimulatedFSDataset.setFactory(dnConf);
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

      SecureResources secureResources = null;
      if (UserGroupInformation.isSecurityEnabled() &&
          conf.get(DFS_DATA_TRANSFER_PROTECTION_KEY) == null) {
        try {
          secureResources = SecureDataNodeStarter.getSecureResources(dnConf);
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
      final int maxRetriesOnSasl = conf.getInt(
        IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY,
        IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_DEFAULT);
      int numRetries = 0;
      DataNode dn = null;
      while (true) {
        try {
          dn = DataNode.instantiateDataNode(dnArgs, dnConf,
                                            secureResources);
          break;
        } catch (IOException e) {
          // Work around issue testing security where rapidly starting multiple
          // DataNodes using the same principal gets rejected by the KDC as a
          // replay attack.
          if (UserGroupInformation.isSecurityEnabled() &&
              numRetries < maxRetriesOnSasl) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              break;
            }
            ++numRetries;
            continue;
          }
          throw e;
        }
      }
      if(dn == null)
        throw new IOException("Cannot start DataNode in "
            + dnConf.get(DFS_DATANODE_DATA_DIR_KEY));
      //since the HDFS does things based on host|ip:port, we need to add the
      //mapping for the service to rackId
      String service =
          SecurityUtil.buildTokenService(dn.getXferAddress()).toString();
      if (racks != null) {
        LOG.info("Adding node with service : " + service +
                            " to rack " + racks[i-curDatanodesNum]);
        StaticMapping.addNodeToRack(service,
                                  racks[i-curDatanodesNum]);
      }
      dn.runDatanodeDaemon();
      dataNodes.add(new DataNodeProperties(dn, newconf, dnArgs,
          secureResources, dn.getIpcPort()));
      dns[i - curDatanodesNum] = dn;
    }
    this.numDataNodes += numDataNodes;
    waitActive();

    setDataNodeStorageCapacities(
        curDatanodesNum,
        numDataNodes,
        dns,
        storageCapacities);

    /* memorize storage capacities */
    if (storageCapacities != null) {
      storageCap.addAll(Arrays.asList(storageCapacities));
    }
  }

  private synchronized void setDataNodeStorageCapacities(
      final int curDatanodesNum,
      final int numDNs,
      final DataNode[] dns,
      long[][] storageCapacities) throws IOException {
    if (storageCapacities != null) {
      for (int i = curDatanodesNum; i < curDatanodesNum + numDNs; ++i) {
        final int index = i - curDatanodesNum;
        setDataNodeStorageCapacities(index, dns[index], storageCapacities);
      }
    }
  }

  private synchronized void setDataNodeStorageCapacities(
      final int curDnIdx,
      final DataNode curDn,
      long[][] storageCapacities) throws IOException {

    if (storageCapacities == null || storageCapacities.length == 0) {
      return;
    }

    try {
      waitDataNodeFullyStarted(curDn);
    } catch (TimeoutException | InterruptedException e) {
      throw new IOException(e);
    }

    try (FsDatasetSpi.FsVolumeReferences volumes = curDn.getFSDataset()
        .getFsVolumeReferences()) {
      assert storageCapacities[curDnIdx].length == storagesPerDatanode;
      assert volumes.size() == storagesPerDatanode;

      int j = 0;
      for (FsVolumeSpi fvs : volumes) {
        FsVolumeImpl volume = (FsVolumeImpl) fvs;
        LOG.info("setCapacityForTesting " + storageCapacities[curDnIdx][j]
            + " for [" + volume.getStorageType() + "]" + volume.getStorageID());
        volume.setCapacityForTesting(storageCapacities[curDnIdx][j]);
        j++;
      }
    }
    DataNodeTestUtils.triggerHeartbeat(curDn);
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
    ToolRunner.run(new DFSAdmin(conf), new String[]{"-finalizeUpgrade"});
  }
  
  /**
   * Finalize cluster for the namenode at the given index 
   * @see MiniDFSCluster#finalizeCluster(Configuration)
   * @param nnIndex index of the namenode
   * @param conf configuration
   * @throws Exception
   */
  public void finalizeCluster(int nnIndex, Configuration conf) throws Exception {
    finalizeNamenode(getNN(nnIndex).nameNode, getNN(nnIndex).conf);
  }

  /**
   * If the NameNode is running, attempt to finalize a previous upgrade.
   * When this method return, the NameNode should be finalized, but
   * DataNodes may not be since that occurs asynchronously.
   *
   * @throws IllegalStateException if the Namenode is not running.
   */
  public void finalizeCluster(Configuration conf) throws Exception {
    for (NameNodeInfo nnInfo : namenodes.values()) {
      if (nnInfo == null) {
        throw new IllegalStateException("Attempting to finalize "
            + "Namenode but it is not running");
      }
      finalizeNamenode(nnInfo.nameNode, nnInfo.conf);
    }
  }

  public int getNumNameNodes() {
    return namenodes.size();
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
    return getNameNodeRpc(0);
  }
  
  /**
   * Get an instance of the NameNode's RPC handler.
   */
  public NamenodeProtocols getNameNodeRpc(int nnIndex) {
    return getNameNode(nnIndex).getRpcServer();
  }
  
  /**
   * Gets the NameNode for the index.  May be null.
   */
  public NameNode getNameNode(int nnIndex) {
    return getNN(nnIndex).nameNode;
  }
  
  /**
   * Return the {@link FSNamesystem} object.
   * @return {@link FSNamesystem} object.
   */
  public FSNamesystem getNamesystem() {
    checkSingleNameNode();
    return NameNodeAdapter.getNamesystem(getNN(0).nameNode);
  }

  public FSNamesystem getNamesystem(int nnIndex) {
    return NameNodeAdapter.getNamesystem(getNN(nnIndex).nameNode);
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
   * Returns the corresponding FsDatasetTestUtils for a DataNode.
   * @param dnIdx the index of DataNode.
   * @return a FsDatasetTestUtils for the given DataNode.
   */
  public FsDatasetTestUtils getFsDatasetTestUtils(int dnIdx) {
    Preconditions.checkArgument(dnIdx < dataNodes.size());
    return FsDatasetTestUtils.Factory.getFactory(conf)
        .newInstance(dataNodes.get(dnIdx).datanode);
  }

  /**
   * Returns the corresponding FsDatasetTestUtils for a DataNode.
   * @param dn a DataNode
   * @return a FsDatasetTestUtils for the given DataNode.
   */
  public FsDatasetTestUtils getFsDatasetTestUtils(DataNode dn) {
    Preconditions.checkArgument(dn != null);
    return FsDatasetTestUtils.Factory.getFactory(conf)
        .newInstance(dn);
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
    return getNN(nnIndex).nameNode.getNameNodeAddress().getPort();
  }

  /**
   * @return the service rpc port used by the NameNode at the given index.
   */     
  public int getNameNodeServicePort(int nnIndex) {
    return getNN(nnIndex).nameNode.getServiceRpcAddress().getPort();
  }
    
  /**
   * Shutdown all the nodes in the cluster.
   */
  public void shutdown() {
      shutdown(false);
  }
    
  /**
   * Shutdown all the nodes in the cluster.
   */
  public void shutdown(boolean deleteDfsDir) {
    shutdown(deleteDfsDir, true);
  }

  /**
   * Shutdown all the nodes in the cluster.
   */
  public void shutdown(boolean deleteDfsDir, boolean closeFileSystem) {
    LOG.info("Shutting down the Mini HDFS Cluster");
    if (checkExitOnShutdown)  {
      if (ExitUtil.terminateCalled()) {
        LOG.error("Test resulted in an unexpected exit",
            ExitUtil.getFirstExitException());
        ExitUtil.resetFirstExitException();
        throw new AssertionError("Test resulted in an unexpected exit");
      }
    }
    if (closeFileSystem) {
      for (FileSystem fs : fileSystems) {
        try {
          fs.close();
        } catch (IOException ioe) {
          LOG.warn("Exception while closing file system", ioe);
        }
      }
      fileSystems.clear();
    }
    shutdownDataNodes();
    for (NameNodeInfo nnInfo : namenodes.values()) {
      if (nnInfo == null) continue;
      stopAndJoinNameNode(nnInfo.nameNode);
    }
    ShutdownHookManager.get().clearShutdownHooks();
    if (base_dir != null) {
      if (deleteDfsDir) {
        FileUtil.fullyDelete(base_dir);
      } else {
        FileUtil.fullyDeleteOnExit(base_dir);
      }
    }
  }
  
  /**
   * Shutdown all DataNodes started by this class.  The NameNode
   * is left running so that new DataNodes may be started.
   */
  public void shutdownDataNodes() {
    for (int i = dataNodes.size()-1; i >= 0; i--) {
      shutdownDataNode(i);
    }
  }

  /**
   * Shutdown the datanode at a given index.
   */
  public void shutdownDataNode(int dnIndex) {
    LOG.info("Shutting down DataNode " + dnIndex);
    DataNode dn = dataNodes.remove(dnIndex).datanode;
    dn.shutdown();
    numDataNodes--;
  }

  /**
   * Shutdown all the namenodes.
   */
  public synchronized void shutdownNameNodes() {
    for (int i = 0; i < namenodes.size(); i++) {
      shutdownNameNode(i);
    }
  }
  
  /**
   * Shutdown the namenode at a given index.
   */
  public synchronized void shutdownNameNode(int nnIndex) {
    NameNodeInfo info = getNN(nnIndex);
    stopAndJoinNameNode(info.nameNode);
    info.nnId = null;
    info.nameNode = null;
    info.nameserviceId = null;
  }

  /**
   * Fully stop the NameNode by stop and join.
   */
  private void stopAndJoinNameNode(NameNode nn) {
    if (nn == null) {
      return;
    }
    LOG.info("Shutting down the namenode");
    nn.stop();
    nn.join();
    nn.joinHttpServer();
  }

  /**
   * Restart all namenodes.
   */
  public synchronized void restartNameNodes() throws IOException {
    for (int i = 0; i < namenodes.size(); i++) {
      restartNameNode(i, false);
    }
    waitActive();
  }
  
  /**
   * Restart the namenode.
   */
  public synchronized void restartNameNode(String... args) throws IOException {
    checkSingleNameNode();
    restartNameNode(0, true, args);
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
  public synchronized void restartNameNode(int nnIndex, boolean waitActive,
      String... args) throws IOException {
    NameNodeInfo info = getNN(nnIndex);
    StartupOption startOpt = info.startOpt;

    shutdownNameNode(nnIndex);
    if (args.length != 0) {
      startOpt = null;
    } else {
      args = createArgs(startOpt);
    }

    NameNode nn = NameNode.createNameNode(args, info.conf);
    info.nameNode = nn;
    info.setStartOpt(startOpt);
    if (waitActive) {
      waitClusterUp();
      LOG.info("Restarted the namenode");
      waitActive();
    }
  }

  private int corruptBlockOnDataNodesHelper(ExtendedBlock block,
      boolean deleteBlockFile) throws IOException {
    int blocksCorrupted = 0;
    for (DataNode dn : getDataNodes()) {
      try {
        MaterializedReplica replica =
            getFsDatasetTestUtils(dn).getMaterializedReplica(block);
        if (deleteBlockFile) {
          replica.deleteData();
        } else {
          replica.corruptData();
        }
        blocksCorrupted++;
      } catch (ReplicaNotFoundException e) {
        // Ignore.
      }
    }
    return blocksCorrupted;
  }

  /**
   * Return the number of corrupted replicas of the given block.
   *
   * @param block block to be corrupted
   * @throws IOException on error accessing the file for the given block
   */
  public int corruptBlockOnDataNodes(ExtendedBlock block) throws IOException{
    return corruptBlockOnDataNodesHelper(block, false);
  }

  /**
   * Return the number of corrupted replicas of the given block.
   *
   * @param block block to be corrupted
   * @throws IOException on error accessing the file for the given block
   */
  public int corruptBlockOnDataNodesByDeletingBlockFile(ExtendedBlock block)
      throws IOException{
    return corruptBlockOnDataNodesHelper(block, true);
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

  public byte[] readBlockOnDataNodeAsBytes(int i, ExtendedBlock block)
      throws IOException {
    assert (i >= 0 && i < dataNodes.size()) : "Invalid datanode "+i;
    File blockFile = getBlockFile(i, block);
    if (blockFile != null && blockFile.exists()) {
      return DFSTestUtil.readFileAsBytes(blockFile);
    }
    return null;
  }

  /**
   * Corrupt a block on a particular datanode.
   *
   * @param i index of the datanode
   * @param blk name of the block
   * @throws IOException on error accessing the given block file.
   */
  public void corruptReplica(int i, ExtendedBlock blk)
      throws IOException {
    getMaterializedReplica(i, blk).corruptData();
  }

  /**
   * Corrupt a block on a particular datanode.
   *
   * @param dn the datanode
   * @param blk name of the block
   * @throws IOException on error accessing the given block file.
   */
  public void corruptReplica(DataNode dn, ExtendedBlock blk)
      throws IOException {
    getMaterializedReplica(dn, blk).corruptData();
  }

  /**
   * Corrupt the metadata of a block on a datanode.
   * @param i the index of the datanode
   * @param blk name of the block
   * @throws IOException on error accessing the given metadata file.
   */
  public void corruptMeta(int i, ExtendedBlock blk) throws IOException {
    getMaterializedReplica(i, blk).corruptMeta();
  }

  /**
   * Corrupt the metadata of a block by deleting it.
   * @param i index of the datanode
   * @param blk name of the block.
   */
  public void deleteMeta(int i, ExtendedBlock blk)
      throws IOException {
    getMaterializedReplica(i, blk).deleteMeta();
  }

  /**
   * Corrupt the metadata of a block by truncating it to a new size.
   * @param i index of the datanode.
   * @param blk name of the block.
   * @param newSize the new size of the metadata file.
   * @throws IOException if any I/O errors.
   */
  public void truncateMeta(int i, ExtendedBlock blk, int newSize)
      throws IOException {
    getMaterializedReplica(i, blk).truncateMeta(newSize);
  }

  public void changeGenStampOfBlock(int dnIndex, ExtendedBlock blk,
      long newGenStamp) throws IOException {
    getFsDatasetTestUtils(dnIndex)
        .changeStoredGenerationStamp(blk, newGenStamp);
  }

  /*
   * Shutdown a particular datanode
   * @param i node index
   * @return null if the node index is out of range, else the properties of the
   * removed node
   */
  public synchronized DataNodeProperties stopDataNode(int i) {
    if (i < 0 || i >= dataNodes.size()) {
      return null;
    }
    DataNodeProperties dnprop = dataNodes.remove(i);
    DataNode dn = dnprop.datanode;
    LOG.info("MiniDFSCluster Stopping DataNode " +
                       dn.getDisplayName() +
                       " from a total of " + (dataNodes.size() + 1) + 
                       " datanodes.");
    dn.shutdown();
    numDataNodes--;
    return dnprop;
  }

  /*
   * Shutdown a datanode by name.
   * @return the removed datanode or null if there was no match
   */
  public synchronized DataNodeProperties stopDataNode(String dnName) {
    int node = -1;
    for (int i = 0; i < dataNodes.size(); i++) {
      DataNode dn = dataNodes.get(i).datanode;
      if (dnName.equals(dn.getDatanodeId().getXferAddr())) {
        node = i;
        break;
      }
    }
    return stopDataNode(node);
  }

  /*
   * Restart a DataNode by name.
   * @return true if DataNode restart is successful else returns false
   */
  public synchronized boolean restartDataNode(String dnName)
      throws IOException {
    for (int i = 0; i < dataNodes.size(); i++) {
      DataNode dn = dataNodes.get(i).datanode;
      if (dnName.equals(dn.getDatanodeId().getXferAddr())) {
        return restartDataNode(i);
      }
    }
    return false;
  }


  /*
   * Shutdown a particular datanode
   * @param i node index
   * @return null if the node index is out of range, else the properties of the
   * removed node
   */
  public synchronized DataNodeProperties stopDataNodeForUpgrade(int i)
      throws IOException {
    if (i < 0 || i >= dataNodes.size()) {
      return null;
    }
    DataNodeProperties dnprop = dataNodes.remove(i);
    DataNode dn = dnprop.datanode;
    LOG.info("MiniDFSCluster Stopping DataNode " +
        dn.getDisplayName() +
        " from a total of " + (dataNodes.size() + 1) +
        " datanodes.");
    dn.shutdownDatanode(true);
    numDataNodes--;
    return dnprop;
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

  private void waitDataNodeFullyStarted(final DataNode dn)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return dn.isDatanodeFullyStarted();
      }
    }, 100, 60000);
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
    SecureResources secureResources = dnprop.secureResources;
    Configuration newconf = new HdfsConfiguration(conf); // save cloned config
    if (keepPort) {
      InetSocketAddress addr = dnprop.datanode.getXferAddress();
      conf.set(DFS_DATANODE_ADDRESS_KEY, 
          addr.getAddress().getHostAddress() + ":" + addr.getPort());
      conf.set(DFS_DATANODE_IPC_ADDRESS_KEY,
          addr.getAddress().getHostAddress() + ":" + dnprop.ipcPort); 
    }
    final DataNode newDn = DataNode.createDataNode(args, conf, secureResources);

    final DataNodeProperties dnp = new DataNodeProperties(
        newDn,
        newconf,
        args,
        secureResources,
        newDn.getIpcPort());
    dataNodes.add(dnp);
    numDataNodes++;

    setDataNodeStorageCapacities(
        dataNodes.lastIndexOf(dnp),
        newDn,
        storageCap.toArray(new long[][]{}));
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
    return restartDataNode(i, keepPort, false);
  }

  /**
   * Restart a particular DataNode.
   * @param idn index of the DataNode
   * @param keepPort true if should restart on the same port
   * @param expireOnNN true if NameNode should expire the DataNode heartbeat
   * @return
   * @throws IOException
   */
  public synchronized boolean restartDataNode(
      int idn, boolean keepPort, boolean expireOnNN) throws IOException {
    DataNodeProperties dnprop = stopDataNode(idn);
    if(expireOnNN) {
      setDataNodeDead(dnprop.datanode.getDatanodeId());
    }
    if (dnprop == null) {
      return false;
    } else {
      return restartDataNode(dnprop, keepPort);
    }
  }

  /**
   * Expire a DataNode heartbeat on the NameNode
   * @param dnId
   * @throws IOException
   */
  public void setDataNodeDead(DatanodeID dnId) throws IOException {
    DatanodeDescriptor dnd =
        NameNodeAdapter.getDatanode(getNamesystem(), dnId);
    DFSTestUtil.setDatanodeDead(dnd);
    BlockManagerTestUtil.checkHeartbeat(getNamesystem().getBlockManager());
  }

  public void setDataNodesDead() throws IOException {
    for (DataNodeProperties dnp : dataNodes) {
      setDataNodeDead(dnp.datanode.getDatanodeId());
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
    NameNode nameNode = getNN(nnIndex).nameNode;
    if (nameNode == null) {
      return false;
    }
    long[] sizes;
    sizes = NameNodeAdapter.getStats(nameNode.getNamesystem());
    boolean isUp = false;
    synchronized (this) {
      isUp = ((!nameNode.isInSafeMode() || !waitSafeMode) &&
          sizes[ClientProtocol.GET_STATS_CAPACITY_IDX] != 0);
    }
    return isUp;
  }

  /**
   * Returns true if all the NameNodes are running and is out of Safe Mode.
   */
  public boolean isClusterUp() {
    for (int index = 0; index < namenodes.size(); index++) {
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
  public DistributedFileSystem getFileSystem() throws IOException {
    checkSingleNameNode();
    return getFileSystem(0);
  }

  /**
   * Get a client handle to the DFS cluster for the namenode at given index.
   */
  public DistributedFileSystem getFileSystem(int nnIndex) throws IOException {
    return (DistributedFileSystem) addFileSystem(FileSystem.get(getURI(nnIndex),
        getNN(nnIndex).conf));
  }

  /**
   * Get another FileSystem instance that is different from FileSystem.get(conf).
   * This simulating different threads working on different FileSystem instances.
   */
  public FileSystem getNewFileSystemInstance(int nnIndex) throws IOException {
    return addFileSystem(FileSystem.newInstance(getURI(nnIndex), getNN(nnIndex).conf));
  }

  private <T extends FileSystem> T addFileSystem(T fs) {
    fileSystems.add(fs);
    return fs;
  }

  /**
   * @return a http URL
   */
  public String getHttpUri(int nnIndex) {
    return "http://"
        + getNN(nnIndex).conf
            .get(DFS_NAMENODE_HTTP_ADDRESS_KEY);
  }

  /**
   * Get the directories where the namenode stores its image.
   */
  public Collection<URI> getNameDirs(int nnIndex) {
    return FSNamesystem.getNamespaceDirs(getNN(nnIndex).conf);
  }

  /**
   * Get the directories where the namenode stores its edits.
   */
  public Collection<URI> getNameEditsDirs(int nnIndex) throws IOException {
    return FSNamesystem.getNamespaceEditsDirs(getNN(nnIndex).conf);
  }
  
  public void transitionToActive(int nnIndex) throws IOException,
      ServiceFailedException {
    getNameNode(nnIndex).getRpcServer().transitionToActive(
        new StateChangeRequestInfo(RequestSource.REQUEST_BY_USER_FORCED));
  }
  
  public void transitionToStandby(int nnIndex) throws IOException,
      ServiceFailedException {
    getNameNode(nnIndex).getRpcServer().transitionToStandby(
        new StateChangeRequestInfo(RequestSource.REQUEST_BY_USER_FORCED));
  }
  
  
  public void triggerBlockReports()
      throws IOException {
    for (DataNode dn : getDataNodes()) {
      DataNodeTestUtils.triggerBlockReport(dn);
    }
  }


  public void triggerDeletionReports()
      throws IOException {
    for (DataNode dn : getDataNodes()) {
      DataNodeTestUtils.triggerDeletionReport(dn);
    }
  }

  public void triggerHeartbeats()
      throws IOException {
    for (DataNode dn : getDataNodes()) {
      DataNodeTestUtils.triggerHeartbeat(dn);
    }
  }


  /** Wait until the given namenode gets registration from all the datanodes */
  public void waitActive(int nnIndex) throws IOException {
    if (namenodes.size() == 0 || getNN(nnIndex) == null || getNN(nnIndex).nameNode == null) {
      return;
    }

    NameNodeInfo info = getNN(nnIndex);
    InetSocketAddress addr = info.nameNode.getServiceRpcAddress();
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

  /** Wait until the given namenode gets first block reports from all the datanodes */
  public void waitFirstBRCompleted(int nnIndex, int timeout) throws
      IOException, TimeoutException, InterruptedException {
    if (namenodes.size() == 0 || getNN(nnIndex) == null || getNN(nnIndex).nameNode == null) {
      return;
    }

    final FSNamesystem ns = getNamesystem(nnIndex);
    final DatanodeManager dm = ns.getBlockManager().getDatanodeManager();
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        List<DatanodeDescriptor> nodes = dm.getDatanodeListForReport
            (DatanodeReportType.LIVE);
        for (DatanodeDescriptor node : nodes) {
          if (!node.checkBlockReportReceived()) {
            return false;
          }
        }
        return true;
      }
    }, 100, timeout);
  }

  /**
   * Wait until the cluster is active and running.
   */
  public void waitActive() throws IOException {
    for (int index = 0; index < namenodes.size(); index++) {
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
    LOG.info("Cluster is active");
  }

  public void printNNs() {
    for (int i = 0; i < namenodes.size(); i++) {
      LOG.info("Have namenode " + i + ", info:" + getNN(i));
      LOG.info(" has namenode: " + getNN(i).nameNode);
    }
  }

  private synchronized boolean shouldWait(DatanodeInfo[] dnInfo,
      InetSocketAddress addr) {
    // If a datanode failed to start, then do not wait
    for (DataNodeProperties dn : dataNodes) {
      // the datanode thread communicating with the namenode should be alive
      if (!dn.datanode.isConnectedToNN(addr)) {
        LOG.warn("BPOfferService in datanode " + dn.datanode
            + " failed to connect to namenode at " + addr);
        return false;
      }
    }
    
    // Wait for expected number of datanodes to start
    if (dnInfo.length != numDataNodes) {
      LOG.info("dnInfo.length != numDataNodes");
      return true;
    }
    
    // if one of the data nodes is not fully started, continue to wait
    for (DataNodeProperties dn : dataNodes) {
      if (!dn.datanode.isDatanodeFullyStarted()) {
        LOG.info("!dn.datanode.isDatanodeFullyStarted()");
        return true;
      }
    }
    
    // make sure all datanodes have sent first heartbeat to namenode,
    // using (capacity == 0) as proxy.
    for (DatanodeInfo dn : dnInfo) {
      if (dn.getCapacity() == 0 || dn.getLastUpdate() <= 0) {
        LOG.info("No heartbeat from DataNode: " + dn.toString());
        return true;
      }
    }
    
    // If datanode dataset is not initialized then wait
    for (DataNodeProperties dn : dataNodes) {
      if (DataNodeTestUtils.getFSDataset(dn.datanode) == null) {
        LOG.info("DataNodeTestUtils.getFSDataset(dn.datanode) == null");
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
  public Map<DatanodeStorage, BlockListAsLongs> getBlockReport(String bpid, int dataNodeIndex) {
    if (dataNodeIndex < 0 || dataNodeIndex > dataNodes.size()) {
      throw new IndexOutOfBoundsException();
    }
    final DataNode dn = dataNodes.get(dataNodeIndex).datanode;
    return DataNodeTestUtils.getFSDataset(dn).getBlockReports(bpid);
  }
  
  
  /**
   * 
   * @return block reports from all data nodes
   *    BlockListAsLongs is indexed in the same order as the list of datanodes returned by getDataNodes()
   */
  public List<Map<DatanodeStorage, BlockListAsLongs>> getAllBlockReports(String bpid) {
    int numDataNodes = dataNodes.size();
    final List<Map<DatanodeStorage, BlockListAsLongs>> result
        = new ArrayList<Map<DatanodeStorage, BlockListAsLongs>>(numDataNodes);
    for (int i = 0; i < numDataNodes; ++i) {
      result.add(getBlockReport(bpid, i));
    }
    return result;
  }
  
  /**
   * This method is valid only if the data nodes have simulated data
   * @param dataNodeIndex - data node i which to inject - the index is same as for getDataNodes()
   * @param blocksToInject - the blocks
   * @param bpid - (optional) the block pool id to use for injecting blocks.
   *             If not supplied then it is queried from the in-process NameNode.
   * @throws IOException
   *              if not simulatedFSDataset
   *             if any of blocks already exist in the data node
   *   
   */
  public void injectBlocks(int dataNodeIndex,
      Iterable<Block> blocksToInject, String bpid) throws IOException {
    if (dataNodeIndex < 0 || dataNodeIndex > dataNodes.size()) {
      throw new IndexOutOfBoundsException();
    }
    final DataNode dn = dataNodes.get(dataNodeIndex).datanode;
    final FsDatasetSpi<?> dataSet = DataNodeTestUtils.getFSDataset(dn);
    if (!(dataSet instanceof SimulatedFSDataset)) {
      throw new IOException("injectBlocks is valid only for" +
          " SimulatedFSDataset");
    }
    if (bpid == null) {
      bpid = getNamesystem().getBlockPoolId();
    }
    SimulatedFSDataset sdataset = (SimulatedFSDataset) dataSet;
    sdataset.injectBlocks(bpid, blocksToInject);
    dataNodes.get(dataNodeIndex).datanode.scheduleAllBlockReport(0);
  }

  /**
   * Multiple-NameNode version of injectBlocks.
   */
  public void injectBlocks(int nameNodeIndex, int dataNodeIndex,
      Iterable<Block> blocksToInject) throws IOException {
    if (dataNodeIndex < 0 || dataNodeIndex > dataNodes.size()) {
      throw new IndexOutOfBoundsException();
    }
    final DataNode dn = dataNodes.get(dataNodeIndex).datanode;
    final FsDatasetSpi<?> dataSet = DataNodeTestUtils.getFSDataset(dn);
    if (!(dataSet instanceof SimulatedFSDataset)) {
      throw new IOException("injectBlocks is valid only for" +
          " SimulatedFSDataset");
    }
    String bpid = getNamesystem(nameNodeIndex).getBlockPoolId();
    SimulatedFSDataset sdataset = (SimulatedFSDataset) dataSet;
    sdataset.injectBlocks(bpid, blocksToInject);
    dataNodes.get(dataNodeIndex).datanode.scheduleAllBlockReport(0);
  }

  /**
   * Set the softLimit and hardLimit of client lease periods
   */
  public void setLeasePeriod(long soft, long hard) {
    NameNodeAdapter.setLeasePeriod(getNamesystem(), soft, hard);
  }
  
  public void setLeasePeriod(long soft, long hard, int nnIndex) {
    NameNodeAdapter.setLeasePeriod(getNamesystem(nnIndex), soft, hard);
  }
  
  public void setWaitSafeMode(boolean wait) {
    this.waitSafeMode = wait;
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
    if (conf != null) {
      final String dfsdir = conf.get(HDFS_MINIDFS_BASEDIR, null);
      if (dfsdir != null) {
        return dfsdir;
      }
    }
    return getBaseDirectory();
  }

  /**
   * Get the base directory for any DFS cluster whose configuration does
   * not explicitly set it. This is done via
   * {@link GenericTestUtils#getTestDir()}.
   * @return a directory for use as a miniDFS filesystem.
   */
  public static String getBaseDirectory() {
    return GenericTestUtils.getTestDir("dfs").getAbsolutePath()
        + File.separator;
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
   * Get a storage directory for PROVIDED storages.
   * The PROVIDED directory to return can be set by using the configuration
   * parameter {@link #HDFS_MINIDFS_BASEDIR_PROVIDED}. If this parameter is
   * not set, this function behaves exactly the same as
   * {@link #getInstanceStorageDir(int, int)}. Currently, the two parameters
   * are ignored as only one PROVIDED storage is supported in HDFS-9806.
   *
   * @param dnIndex datanode index (starts from 0)
   * @param dirIndex directory index
   * @return Storage directory
   */
  public File getProvidedStorageDir(int dnIndex, int dirIndex) {
    String base = conf.get(HDFS_MINIDFS_BASEDIR_PROVIDED, null);
    if (base == null) {
      return getInstanceStorageDir(dnIndex, dirIndex);
    }
    return new File(base);
  }

  /**
   * Get a storage directory for a datanode.
   * <ol>
   * <li><base directory>/data/data<2*dnIndex + 1></li>
   * <li><base directory>/data/data<2*dnIndex + 2></li>
   * </ol>
   *
   * @param dnIndex datanode index (starts from 0)
   * @param dirIndex directory index.
   * @return Storage directory
   */
  public File getStorageDir(int dnIndex, int dirIndex) {
    return new File(determineDfsBaseDir(),
        getStorageDirPath(dnIndex, dirIndex));
  }

  /**
   * Calculate the DN instance-specific path for appending to the base dir
   * to determine the location of the storage of a DN instance in the mini cluster
   * @param dnIndex datanode index
   * @param dirIndex directory index.
   * @return storage directory path
   */
  private String getStorageDirPath(int dnIndex, int dirIndex) {
    return "data/data" + (storagesPerDatanode * dnIndex + 1 + dirIndex);
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
   * @param storageDir storage directory
   * @return current directory in the given storage directory
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
   * Get materialized replica that can be corrupted later.
   * @param i the index of DataNode.
   * @param blk name of the block.
   * @return a materialized replica.
   * @throws ReplicaNotFoundException if the replica does not exist on the
   * DataNode.
   */
  public MaterializedReplica getMaterializedReplica(
      int i, ExtendedBlock blk) throws ReplicaNotFoundException {
    return getFsDatasetTestUtils(i).getMaterializedReplica(blk);
  }

  /**
   * Get materialized replica that can be corrupted later.
   * @param dn the index of DataNode.
   * @param blk name of the block.
   * @return a materialized replica.
   * @throws ReplicaNotFoundException if the replica does not exist on the
   * DataNode.
   */
  public MaterializedReplica getMaterializedReplica(
      DataNode dn, ExtendedBlock blk) throws ReplicaNotFoundException {
    return getFsDatasetTestUtils(dn).getMaterializedReplica(blk);
  }

  /**
   * Get file correpsonding to a block
   * @param storageDir storage directory
   * @param blk the block
   * @return data file corresponding to the block
   */
  public static File getBlockFile(File storageDir, ExtendedBlock blk) {
    return new File(DatanodeUtil.idToBlockDir(getFinalizedDir(storageDir,
        blk.getBlockPoolId()), blk.getBlockId()), blk.getBlockName());
  }

  /**
   * Return all block files in given directory (recursive search).
   */
  public static List<File> getAllBlockFiles(File storageDir) {
    List<File> results = new ArrayList<File>();
    File[] files = storageDir.listFiles();
    if (files == null) {
      return null;
    }
    for (File f : files) {
      if (f.getName().startsWith(Block.BLOCK_FILE_PREFIX) &&
          !f.getName().endsWith(Block.METADATA_EXTENSION)) {
        results.add(f);
      } else if (f.isDirectory()) {
        List<File> subdirResults = getAllBlockFiles(f);
        if (subdirResults != null) {
          results.addAll(subdirResults);
        }
      }
    }
    return results;
  }

  /**
   * Get the latest metadata file correpsonding to a block
   * @param storageDir storage directory
   * @param blk the block
   * @return metadata file corresponding to the block
   */
  public static File getBlockMetadataFile(File storageDir, ExtendedBlock blk) {
    return new File(DatanodeUtil.idToBlockDir(getFinalizedDir(storageDir,
        blk.getBlockPoolId()), blk.getBlockId()), blk.getBlockName() + "_" +
        blk.getGenerationStamp() + Block.METADATA_EXTENSION);
  }

  /**
   * Return all block metadata files in given directory (recursive search)
   */
  public static List<File> getAllBlockMetadataFiles(File storageDir) {
    List<File> results = new ArrayList<File>();
    File[] files = storageDir.listFiles();
    if (files == null) {
      return null;
    }
    for (File f : files) {
      if (f.getName().startsWith(Block.BLOCK_FILE_PREFIX) &&
              f.getName().endsWith(Block.METADATA_EXTENSION)) {
        results.add(f);
      } else if (f.isDirectory()) {
        List<File> subdirResults = getAllBlockMetadataFiles(f);
        if (subdirResults != null) {
          results.addAll(subdirResults);
        }
      }
    }
    return results;
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
   * Get the block data file for a block from a given datanode
   * @param dnIndex Index of the datanode to get block files for
   * @param block block for which corresponding files are needed
   */
  public File getBlockFile(int dnIndex, ExtendedBlock block) {
    // Check for block file in the two storage directories of the datanode
    for (int i = 0; i <=1 ; i++) {
      File storageDir = getStorageDir(dnIndex, i);
      File blockFile = getBlockFile(storageDir, block);
      if (blockFile.exists()) {
        return blockFile;
      }
    }
    return null;
  }
  
  /**
   * Get the block metadata file for a block from a given datanode
   * 
   * @param dnIndex Index of the datanode to get block files for
   * @param block block for which corresponding files are needed
   */
  public File getBlockMetadataFile(int dnIndex, ExtendedBlock block) {
    // Check for block file in the two storage directories of the datanode
    for (int i = 0; i <=1 ; i++) {
      File storageDir = getStorageDir(dnIndex, i);
      File blockMetaFile = getBlockMetadataFile(storageDir, block);
      if (blockMetaFile.exists()) {
        return blockMetaFile;
      }
    }
    return null;
  }
  
  /**
   * Throw an exception if the MiniDFSCluster is not started with a single
   * namenode
   */
  private void checkSingleNameNode() {
    if (namenodes.size() != 1) {
      throw new IllegalArgumentException("Namenode index is needed");
    }
  }

  /**
   * Add a namenode to a federated cluster and start it. Configuration of
   * datanodes in the cluster is refreshed to register with the new namenode.
   * 
   * @return newly started namenode
   */
  public void addNameNode(Configuration conf, int namenodePort)
      throws IOException {
    if(!federation)
      throw new IOException("cannot add namenode to non-federated cluster");

    int nameServiceIndex = namenodes.keys().size();
    String nameserviceId = NAMESERVICE_ID_PREFIX + (namenodes.keys().size() + 1);

    String nameserviceIds = conf.get(DFS_NAMESERVICES);
    nameserviceIds += "," + nameserviceId;
    conf.set(DFS_NAMESERVICES, nameserviceIds);
  
    String nnId = null;
    initNameNodeAddress(conf, nameserviceId,
        new NNConf(nnId).setIpcPort(namenodePort));
    // figure out the current number of NNs
    NameNodeInfo[] infos = this.getNameNodeInfos(nameserviceId);
    int nnIndex = infos == null ? 0 : infos.length;
    initNameNodeConf(conf, nameserviceId, nameServiceIndex, nnId, true, true, nnIndex);
    createNameNode(conf, true, null, null, nameserviceId, nnId);

    // Refresh datanodes with the newly started namenode
    for (DataNodeProperties dn : dataNodes) {
      DataNode datanode = dn.datanode;
      datanode.refreshNamenodes(conf);
    }

    // Wait for new namenode to get registrations from all the datanodes
    waitActive(nnIndex);
  }

  /**
   * Sets the timeout for re-issuing a block recovery.
   */
  public void setBlockRecoveryTimeout(long timeout) {
    for (int nnIndex = 0; nnIndex < getNumNameNodes(); nnIndex++) {
      getNamesystem(nnIndex).getBlockManager().setBlockRecoveryTimeout(
          timeout);
    }
  }
  
  protected void setupDatanodeAddress(Configuration conf, boolean setupHostsFile,
                           boolean checkDataNodeAddrConfig) throws IOException {
    if (setupHostsFile) {
      String hostsFile = conf.get(DFS_HOSTS, "").trim();
      if (hostsFile.length() == 0) {
        throw new IOException("Parameter dfs.hosts is not setup in conf");
      }
      // Setup datanode in the include file, if it is defined in the conf
      String address = "127.0.0.1:" + NetUtils.getFreeSocketPort();
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
      } else {
        conf.set(DFS_DATANODE_ADDRESS_KEY, "127.0.0.1:0");
      }
    }
    if (checkDataNodeAddrConfig) {
      conf.setIfUnset(DFS_DATANODE_HTTP_ADDRESS_KEY, "127.0.0.1:0");
      conf.setIfUnset(DFS_DATANODE_IPC_ADDRESS_KEY, "127.0.0.1:0");
    } else {
      conf.set(DFS_DATANODE_HTTP_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(DFS_DATANODE_IPC_ADDRESS_KEY, "127.0.0.1:0");
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

  @Override
  public void close() {
    shutdown();
  }
}
