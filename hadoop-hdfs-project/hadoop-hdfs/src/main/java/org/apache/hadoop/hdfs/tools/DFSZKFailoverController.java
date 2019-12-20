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
package org.apache.hadoop.hdfs.tools;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.HealthMonitor;
import org.apache.hadoop.ha.ZKFailoverController;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos.ActiveNodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import com.google.protobuf.InvalidProtocolBufferException;

@InterfaceAudience.Private
public class DFSZKFailoverController extends ZKFailoverController {

  private static final Logger LOG =
      LoggerFactory.getLogger(DFSZKFailoverController.class);
  private final AccessControlList adminAcl;
  /* the same as superclass's localTarget, but with the more specfic NN type */
  private final NNHAServiceTarget localNNTarget;

  // This is used only for unit tests
  private boolean isThreadDumpCaptured = false;

  @Override
  protected HAServiceTarget dataToTarget(byte[] data) {
    ActiveNodeInfo proto;
    try {
      proto = ActiveNodeInfo.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Invalid data in ZK: " +
          StringUtils.byteToHexString(data));
    }
    NNHAServiceTarget ret = new NNHAServiceTarget(
        conf, proto.getNameserviceId(), proto.getNamenodeId());
    InetSocketAddress addressFromProtobuf = new InetSocketAddress(
        proto.getHostname(), proto.getPort());
    
    if (!addressFromProtobuf.equals(ret.getAddress())) {
      throw new RuntimeException("Mismatched address stored in ZK for " +
          ret + ": Stored protobuf was " + proto + ", address from our own " +
          "configuration for this NameNode was " + ret.getAddress());
    }
    
    ret.setZkfcPort(proto.getZkfcPort());
    return ret;
  }

  @Override
  protected byte[] targetToData(HAServiceTarget target) {
    InetSocketAddress addr = target.getAddress();

    return ActiveNodeInfo.newBuilder()
      .setHostname(addr.getHostName())
      .setPort(addr.getPort())
      .setZkfcPort(target.getZKFCAddress().getPort())
      .setNameserviceId(localNNTarget.getNameServiceId())
      .setNamenodeId(localNNTarget.getNameNodeId())
      .build()
      .toByteArray();
  }
  
  @Override
  protected InetSocketAddress getRpcAddressToBindTo() {
    int zkfcPort = getZkfcPort(conf);
    return new InetSocketAddress(localTarget.getAddress().getAddress(),
          zkfcPort);
  }
  

  @Override
  protected PolicyProvider getPolicyProvider() {
    return new HDFSPolicyProvider();
  }
  
  static int getZkfcPort(Configuration conf) {
    return conf.getInt(DFSConfigKeys.DFS_HA_ZKFC_PORT_KEY,
        DFSConfigKeys.DFS_HA_ZKFC_PORT_DEFAULT);
  }
  
  public static DFSZKFailoverController create(Configuration conf) {
    Configuration localNNConf = DFSHAAdmin.addSecurityConfiguration(conf);
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);

    if (!HAUtil.isHAEnabled(localNNConf, nsId)) {
      throw new HadoopIllegalArgumentException(
          "HA is not enabled for this namenode.");
    }
    String nnId = HAUtil.getNameNodeId(localNNConf, nsId);
    if (nnId == null) {
      String msg = "Could not get the namenode ID of this node. " +
          "You may run zkfc on the node other than namenode.";
      throw new HadoopIllegalArgumentException(msg);
    }
    NameNode.initializeGenericKeys(localNNConf, nsId, nnId);
    DFSUtil.setGenericConf(localNNConf, nsId, nnId, ZKFC_CONF_KEYS);
    
    NNHAServiceTarget localTarget = new NNHAServiceTarget(
        localNNConf, nsId, nnId);
    return new DFSZKFailoverController(localNNConf, localTarget);
  }

  private DFSZKFailoverController(Configuration conf,
      NNHAServiceTarget localTarget) {
    super(conf, localTarget);
    this.localNNTarget = localTarget;
    // Setup ACLs
    adminAcl = new AccessControlList(
        conf.get(DFSConfigKeys.DFS_ADMIN, " "));
    LOG.info("Failover controller configured for NameNode " +
        localTarget);
}
  
  
  @Override
  protected void initRPC() throws IOException {
    super.initRPC();
    localNNTarget.setZkfcPort(rpcServer.getAddress().getPort());
  }

  @Override
  public void loginAsFCUser() throws IOException {
    InetSocketAddress socAddr = DFSUtilClient.getNNAddress(conf);
    SecurityUtil.login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY,
        DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, socAddr.getHostName());
  }
  
  @Override
  protected String getScopeInsideParentNode() {
    return localNNTarget.getNameServiceId();
  }

  public static void main(String args[])
      throws Exception {
    StringUtils.startupShutdownMessage(DFSZKFailoverController.class,
        args, LOG);
    if (DFSUtil.parseHelpArgument(args, 
        ZKFailoverController.USAGE, System.out, true)) {
      System.exit(0);
    }
    
    GenericOptionsParser parser = new GenericOptionsParser(
        new HdfsConfiguration(), args);
    try {
      DFSZKFailoverController zkfc = DFSZKFailoverController.create(
          parser.getConfiguration());
      System.exit(zkfc.run(parser.getRemainingArgs()));
    } catch (Throwable t) {
      LOG.error("DFSZKFailOverController exiting due to earlier exception "
          + t);
      terminate(1, t);
    }
  }

  @Override
  protected void checkRpcAdminAccess() throws IOException, AccessControlException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    UserGroupInformation zkfcUgi = UserGroupInformation.getLoginUser();
    if (adminAcl.isUserAllowed(ugi) ||
        ugi.getShortUserName().equals(zkfcUgi.getShortUserName())) {
      LOG.info("Allowed RPC access from " + ugi + " at " + Server.getRemoteAddress());
      return;
    }
    String msg = "Disallowed RPC access from " + ugi + " at " +
        Server.getRemoteAddress() + ". Not listed in " + DFSConfigKeys.DFS_ADMIN; 
    LOG.warn(msg);
    throw new AccessControlException(msg);
  }

  /**
   * capture local NN's thread dump and write it to ZKFC's log.
   */
  private void getLocalNNThreadDump() {
    isThreadDumpCaptured = false;
    // We use the same timeout value for both connection establishment
    // timeout and read timeout.
    int httpTimeOut = conf.getInt(
        DFSConfigKeys.DFS_HA_ZKFC_NN_HTTP_TIMEOUT_KEY,
        DFSConfigKeys.DFS_HA_ZKFC_NN_HTTP_TIMEOUT_KEY_DEFAULT);
    if (httpTimeOut == 0) {
      // If timeout value is set to zero, the feature is turned off.
      return;
    }
    try {
      String stacksUrl = DFSUtil.getInfoServer(localNNTarget.getAddress(),
          conf, DFSUtil.getHttpClientScheme(conf)) + "/stacks";
      URL url = new URL(stacksUrl);
      HttpURLConnection conn = (HttpURLConnection)url.openConnection();
      conn.setReadTimeout(httpTimeOut);
      conn.setConnectTimeout(httpTimeOut);
      conn.connect();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copyBytes(conn.getInputStream(), out, 4096, true);
      StringBuilder localNNThreadDumpContent =
          new StringBuilder("-- Local NN thread dump -- \n");
      localNNThreadDumpContent.append(out)
          .append("\n -- Local NN thread dump -- ");
      LOG.info("{}", localNNThreadDumpContent.toString());
      isThreadDumpCaptured = true;
    } catch (IOException e) {
      LOG.warn("Can't get local NN thread dump due to " + e.getMessage());
    }
  }

  @Override
  protected synchronized void setLastHealthState(HealthMonitor.State newState) {
    super.setLastHealthState(newState);
    // Capture local NN thread dump when the target NN health state changes.
    if (getLastHealthState() == HealthMonitor.State.SERVICE_NOT_RESPONDING ||
        getLastHealthState() == HealthMonitor.State.SERVICE_UNHEALTHY) {
      getLocalNNThreadDump();
    }
  }

  @VisibleForTesting
  boolean isThreadDumpCaptured() {
    return isThreadDumpCaptured;
  }

  @Override
  public List<HAServiceTarget> getAllOtherNodes() {
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    List<String> otherNn = HAUtil.getNameNodeIdOfOtherNodes(conf, nsId);

    List<HAServiceTarget> targets = new ArrayList<HAServiceTarget>(otherNn.size());
    for (String nnId : otherNn) {
      targets.add(new NNHAServiceTarget(conf, nsId, nnId));
    }
    return targets;
  }
}
