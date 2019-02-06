/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.TextStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerConstants;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerException;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ClusterConnector;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ConnectorFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerCluster;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolumeSet;
import org.apache.hadoop.hdfs.tools.DiskBalancerCLI;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.HostsFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Common interface for command handling.
 */
public abstract class Command extends Configured implements Closeable {
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(HashMap.class);
  static final Logger LOG = LoggerFactory.getLogger(Command.class);
  private Map<String, String> validArgs = new HashMap<>();
  private URI clusterURI;
  private FileSystem fs = null;
  private DiskBalancerCluster cluster = null;
  private int topNodes;
  private PrintStream ps;

  private static final Path DEFAULT_LOG_DIR = new Path("/system/diskbalancer");

  private Path diskBalancerLogs;

  /**
   * Constructs a command.
   */
  public Command(Configuration conf) {
    this(conf, System.out);
  }

  /**
   * Constructs a command.
   */
  public Command(Configuration conf, final PrintStream ps) {
    super(conf);
    // These arguments are valid for all commands.
    topNodes = 0;
    this.ps = ps;
  }

  /**
   * Cleans any resources held by this command.
   * <p>
   * The main goal is to delete id file created in
   * {@link org.apache.hadoop.hdfs.server.balancer
   * .NameNodeConnector#checkAndMarkRunning}
   * , otherwise, it's not allowed to run multiple commands in a row.
   * </p>
   */
  @Override
  public void close() throws IOException {
    if (fs != null) {
      fs.close();
    }
  }

  /**
   * Gets printing stream.
   * @return print stream
   */
  PrintStream getPrintStream() {
    return ps;
  }

  /**
   * Executes the Client Calls.
   *
   * @param cmd - CommandLine
   * @throws Exception
   */
  public abstract void execute(CommandLine cmd) throws Exception;

  /**
   * Gets extended help for this command.
   */
  public abstract void printHelp();

  /**
   * Process the URI and return the cluster with nodes setup. This is used in
   * all commands.
   *
   * @param cmd - CommandLine
   * @return DiskBalancerCluster
   * @throws Exception
   */
  protected DiskBalancerCluster readClusterInfo(CommandLine cmd) throws
      Exception {
    Preconditions.checkNotNull(cmd);

    setClusterURI(FileSystem.getDefaultUri(getConf()));
    LOG.debug("using name node URI : {}", this.getClusterURI());
    ClusterConnector connector = ConnectorFactory.getCluster(this.clusterURI,
        getConf());

    cluster = new DiskBalancerCluster(connector);

    LOG.debug("Reading cluster info");
    cluster.readClusterInfo();
    return cluster;
  }

  /**
   * Setup the outpath.
   *
   * @param path - Path or null to use default path.
   * @throws IOException
   */
  protected void setOutputPath(String path) throws IOException {

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MMM-dd-HH-mm-ss");
    Date now = new Date();

    fs = FileSystem.get(getClusterURI(), getConf());
    if (path == null || path.isEmpty()) {
      if (getClusterURI().getScheme().startsWith("file")) {
        diskBalancerLogs = new Path(
            System.getProperty("user.dir") + DEFAULT_LOG_DIR.toString() +
                Path.SEPARATOR + format.format(now));
      } else {
        diskBalancerLogs = new Path(DEFAULT_LOG_DIR.toString() +
            Path.SEPARATOR + format.format(now));
      }
    } else {
      diskBalancerLogs = new Path(path);
    }
    if (fs.exists(diskBalancerLogs)) {
      LOG.debug("Another Diskbalancer instance is running ? - Target " +
          "Directory already exists. {}", diskBalancerLogs);
      throw new IOException("Another DiskBalancer files already exist at the " +
          "target location. " + diskBalancerLogs.toString());
    }
    fs.mkdirs(diskBalancerLogs);
  }

  /**
   * Sets the nodes to process.
   *
   * @param node - Node
   */
  protected void setNodesToProcess(DiskBalancerDataNode node) {
    List<DiskBalancerDataNode> nodelist = new LinkedList<>();
    nodelist.add(node);
    setNodesToProcess(nodelist);
  }

  /**
   * Sets the list of Nodes to process.
   *
   * @param nodes Nodes.
   */
  protected void setNodesToProcess(List<DiskBalancerDataNode> nodes) {
    if (cluster == null) {
      throw new IllegalStateException("Set nodes to process invoked before " +
          "initializing cluster. Illegal usage.");
    }
    cluster.setNodesToProcess(nodes);
  }

  /**
   * Returns a DiskBalancer Node from the Cluster or null if not found.
   *
   * @param nodeName - can the hostname, IP address or UUID of the node.
   * @return - DataNode if found.
   */
  DiskBalancerDataNode getNode(String nodeName) {
    DiskBalancerDataNode node = null;
    if (nodeName == null || nodeName.isEmpty()) {
      return node;
    }
    if (cluster.getNodes().size() == 0) {
      return node;
    }

    node = cluster.getNodeByName(nodeName);
    if (node != null) {
      return node;
    }

    node = cluster.getNodeByIPAddress(nodeName);
    if (node != null) {
      return node;
    }
    node = cluster.getNodeByUUID(nodeName);
    return node;
  }

  /**
   * Gets the node set from a file or a string.
   *
   * @param listArg - String File URL or a comma separated list of node names.
   * @return Set of node names
   * @throws IOException
   */
  protected Set<String> getNodeList(String listArg) throws IOException {
    URL listURL;
    String nodeData;
    Set<String> resultSet = new TreeSet<>();

    if ((listArg == null) || listArg.isEmpty()) {
      return resultSet;
    }

    if (listArg.startsWith("file://")) {
      listURL = new URL(listArg);
      try {
        HostsFileReader.readFileToSet("include",
            Paths.get(listURL.getPath()).toString(), resultSet);
      } catch (FileNotFoundException e) {
        String warnMsg = String
            .format("The input host file path '%s' is not a valid path. "
                + "Please make sure the host file exists.", listArg);
        throw new DiskBalancerException(warnMsg,
            DiskBalancerException.Result.INVALID_HOST_FILE_PATH);
      }
    } else {
      nodeData = listArg;
      String[] nodes = nodeData.split(",");

      if (nodes.length == 0) {
        String warnMsg = "The number of input nodes is 0. "
            + "Please input the valid nodes.";
        throw new DiskBalancerException(warnMsg,
            DiskBalancerException.Result.INVALID_NODE);
      }

      Collections.addAll(resultSet, nodes);
    }

    return resultSet;
  }

  /**
   * Returns a DiskBalancer Node list from the Cluster or null if not found.
   *
   * @param listArg String File URL or a comma separated list of node names.
   * @return List of DiskBalancer Node
   * @throws IOException
   */
  protected List<DiskBalancerDataNode> getNodes(String listArg)
      throws IOException {
    Set<String> nodeNames = null;
    List<DiskBalancerDataNode> nodeList = Lists.newArrayList();
    List<String> invalidNodeList = Lists.newArrayList();

    if ((listArg == null) || listArg.isEmpty()) {
      return nodeList;
    }
    nodeNames = getNodeList(listArg);

    DiskBalancerDataNode node = null;
    if (!nodeNames.isEmpty()) {
      for (String name : nodeNames) {
        node = getNode(name);

        if (node != null) {
          nodeList.add(node);
        } else {
          invalidNodeList.add(name);
        }
      }
    }

    if (!invalidNodeList.isEmpty()) {
      String invalidNodes = StringUtils.join(invalidNodeList.toArray(), ",");
      String warnMsg = String.format(
          "The node(s) '%s' not found. "
          + "Please make sure that '%s' exists in the cluster.",
          invalidNodes, invalidNodes);
      throw new DiskBalancerException(warnMsg,
          DiskBalancerException.Result.INVALID_NODE);
    }

    return nodeList;
  }

  /**
   * Verifies if the command line options are sane.
   *
   * @param commandName - Name of the command
   * @param cmd         - Parsed Command Line
   */
  protected void verifyCommandOptions(String commandName, CommandLine cmd) {
    @SuppressWarnings("unchecked")
    Iterator<Option> iter = cmd.iterator();
    while (iter.hasNext()) {
      Option opt = iter.next();

      if (!validArgs.containsKey(opt.getLongOpt())) {
        String errMessage = String
            .format("%nInvalid argument found for command %s : %s%n",
                commandName, opt.getLongOpt());
        StringBuilder validArguments = new StringBuilder();
        validArguments.append(String.format("Valid arguments are : %n"));
        for (Map.Entry<String, String> args : validArgs.entrySet()) {
          String key = args.getKey();
          String desc = args.getValue();
          String s = String.format("\t %s : %s %n", key, desc);
          validArguments.append(s);
        }
        LOG.error(errMessage + validArguments.toString());
        throw new IllegalArgumentException("Invalid Arguments found.");
      }
    }
  }

  /**
   * Gets cluster URL.
   *
   * @return - URL
   */
  public URI getClusterURI() {
    return clusterURI;
  }

  /**
   * Set cluster URL.
   *
   * @param clusterURI - URL
   */
  public void setClusterURI(URI clusterURI) {
    this.clusterURI = clusterURI;
  }

  /**
   * Copied from DFSAdmin.java. -- Creates a connection to dataNode.
   *
   * @param datanode - dataNode.
   * @return ClientDataNodeProtocol
   * @throws IOException
   */
  public ClientDatanodeProtocol getDataNodeProxy(String datanode)
      throws IOException {
    InetSocketAddress datanodeAddr = NetUtils.createSocketAddr(datanode);

    // For datanode proxy the server principal should be DN's one.
    getConf().set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
        getConf().get(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, ""));

    // Create the client
    ClientDatanodeProtocol dnProtocol =
        DFSUtilClient.createClientDatanodeProtocolProxy(datanodeAddr, getUGI(),
            getConf(), NetUtils.getSocketFactory(getConf(),
                ClientDatanodeProtocol
                    .class));
    return dnProtocol;
  }

  /**
   * Returns UGI.
   *
   * @return UserGroupInformation.
   * @throws IOException
   */
  private static UserGroupInformation getUGI()
      throws IOException {
    return UserGroupInformation.getCurrentUser();
  }

  /**
   * Returns a file created in the cluster.
   *
   * @param fileName - fileName to open.
   * @return OutputStream.
   * @throws IOException
   */
  protected FSDataOutputStream create(String fileName) throws IOException {
    Preconditions.checkNotNull(fileName);
    if(fs == null) {
      fs = FileSystem.get(getConf());
    }
    return fs.create(new Path(this.diskBalancerLogs, fileName));
  }

  /**
   * Returns a InputStream to read data.
   */
  protected FSDataInputStream open(String fileName) throws IOException {
    Preconditions.checkNotNull(fileName);
    if(fs == null) {
      fs = FileSystem.get(getConf());
    }
    return  fs.open(new Path(fileName));
  }

  /**
   * Returns the output path where the plan and snapshot gets written.
   *
   * @return Path
   */
  protected Path getOutputPath() {
    return diskBalancerLogs;
  }

  /**
   * Adds valid params to the valid args table.
   *
   * @param key
   * @param desc
   */
  protected void addValidCommandParameters(String key, String desc) {
    validArgs.put(key, desc);
  }

  /**
   * Returns the cluster.
   *
   * @return Cluster.
   */
  @VisibleForTesting
  DiskBalancerCluster getCluster() {
    return cluster;
  }

  /**
   * returns default top number of nodes.
   * @return default top number of nodes.
   */
  protected int getDefaultTop() {
    return DiskBalancerCLI.DEFAULT_TOP;
  }

  /**
   * Put output line to log and string buffer.
   * */
  protected void recordOutput(final TextStringBuilder result,
      final String outputLine) {
    LOG.info(outputLine);
    result.appendln(outputLine);
  }

  /**
   * Parse top number of nodes to be processed.
   * @return top number of nodes to be processed.
   */
  protected int parseTopNodes(final CommandLine cmd, final TextStringBuilder result)
      throws IllegalArgumentException {
    String outputLine = "";
    int nodes = 0;
    final String topVal = cmd.getOptionValue(DiskBalancerCLI.TOP);
    if (StringUtils.isBlank(topVal)) {
      outputLine = String.format(
          "No top limit specified, using default top value %d.",
          getDefaultTop());
      LOG.info(outputLine);
      result.appendln(outputLine);
      nodes = getDefaultTop();
    } else {
      try {
        nodes = Integer.parseInt(topVal);
      } catch (NumberFormatException nfe) {
        outputLine = String.format(
            "Top limit input is not numeric, using default top value %d.",
            getDefaultTop());
        LOG.info(outputLine);
        result.appendln(outputLine);
        nodes = getDefaultTop();
      }
      if (nodes <= 0) {
        throw new IllegalArgumentException(
            "Top limit input should be a positive numeric value");
      }
    }

    return Math.min(nodes, cluster.getNodes().size());
  }

  /**
   * Reads the Physical path of the disks we are balancing. This is needed to
   * make the disk balancer human friendly and not used in balancing.
   *
   * @param node - Disk Balancer Node.
   */
  protected void populatePathNames(
      DiskBalancerDataNode node) throws IOException {
    // if the cluster is a local file system, there is no need to
    // invoke rpc call to dataNode.
    if (getClusterURI().getScheme().startsWith("file")) {
      return;
    }
    String dnAddress = node.getDataNodeIP() + ":" + node.getDataNodePort();
    ClientDatanodeProtocol dnClient = getDataNodeProxy(dnAddress);
    String volumeNameJson = dnClient.getDiskBalancerSetting(
        DiskBalancerConstants.DISKBALANCER_VOLUME_NAME);

    @SuppressWarnings("unchecked")
    Map<String, String> volumeMap =
        READER.readValue(volumeNameJson);
    for (DiskBalancerVolumeSet set : node.getVolumeSets().values()) {
      for (DiskBalancerVolume vol : set.getVolumes()) {
        if (volumeMap.containsKey(vol.getUuid())) {
          vol.setPath(volumeMap.get(vol.getUuid()));
        }
      }
    }
  }

  /**
   * Set top number of nodes to be processed.
   * */
  public void setTopNodes(int topNodes) {
    this.topNodes = topNodes;
  }

  /**
   * Get top number of nodes to be processed.
   * @return top number of nodes to be processed.
   * */
  public int getTopNodes() {
    return topNodes;
  }

  /**
   * Set DiskBalancer cluster
   */
  @VisibleForTesting
  public void setCluster(DiskBalancerCluster newCluster) {
    this.cluster = newCluster;
  }
}
