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
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.ZKFailoverController;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos.ActiveNodeInfo;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

@InterfaceAudience.Private
public class DFSZKFailoverController extends ZKFailoverController {

  private static final Log LOG =
    LogFactory.getLog(DFSZKFailoverController.class);
  private NNHAServiceTarget localTarget;
  private Configuration localNNConf;
  private AccessControlList adminAcl;

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
        getConf(), proto.getNameserviceId(), proto.getNamenodeId());
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
      .setNameserviceId(localTarget.getNameServiceId())
      .setNamenodeId(localTarget.getNameNodeId())
      .build()
      .toByteArray();
  }
  
  @Override
  protected InetSocketAddress getRpcAddressToBindTo() {
    int zkfcPort = getZkfcPort(localNNConf);
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

  @Override
  public void setConf(Configuration conf) {
    localNNConf = DFSHAAdmin.addSecurityConfiguration(conf);
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);

    if (!HAUtil.isHAEnabled(localNNConf, nsId)) {
      throw new HadoopIllegalArgumentException(
          "HA is not enabled for this namenode.");
    }
    String nnId = HAUtil.getNameNodeId(localNNConf, nsId);
    NameNode.initializeGenericKeys(localNNConf, nsId, nnId);
    DFSUtil.setGenericConf(localNNConf, nsId, nnId, ZKFC_CONF_KEYS);
    
    localTarget = new NNHAServiceTarget(localNNConf, nsId, nnId);
    
    // Setup ACLs
    adminAcl = new AccessControlList(
        conf.get(DFSConfigKeys.DFS_ADMIN, " "));
    
    super.setConf(localNNConf);
    LOG.info("Failover controller configured for NameNode " +
        nsId + "." + nnId);
  }
  
  
  @Override
  protected void initRPC() throws IOException {
    super.initRPC();
    localTarget.setZkfcPort(rpcServer.getAddress().getPort());
  }

  @Override
  public HAServiceTarget getLocalTarget() {
    Preconditions.checkState(localTarget != null,
        "setConf() should have already been called");
    return localTarget;
  }
  
  @Override
  public void loginAsFCUser() throws IOException {
    InetSocketAddress socAddr = NameNode.getAddress(localNNConf);
    SecurityUtil.login(getConf(), DFS_NAMENODE_KEYTAB_FILE_KEY,
        DFS_NAMENODE_USER_NAME_KEY, socAddr.getHostName());
  }
  
  @Override
  protected String getScopeInsideParentNode() {
    return localTarget.getNameServiceId();
  }

  public static void main(String args[])
      throws Exception {
    System.exit(ToolRunner.run(
        new DFSZKFailoverController(), args));
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
}
