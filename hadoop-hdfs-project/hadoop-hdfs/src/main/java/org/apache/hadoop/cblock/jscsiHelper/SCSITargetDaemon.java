/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.cblock.jscsiHelper;

import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_ISCSI_ADVERTISED_IP;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_ISCSI_ADVERTISED_PORT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_ISCSI_ADVERTISED_PORT_DEFAULT;
import org.apache.hadoop.cblock.protocolPB.CBlockClientServerProtocolPB;
import org.apache.hadoop.cblock.protocolPB.CBlockServiceProtocolPB;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.scm.client.ContainerOperationClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.jscsi.target.Configuration;

import java.net.InetSocketAddress;

import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_CONTAINER_SIZE_GB_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_CONTAINER_SIZE_GB_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_JSCSI_CBLOCK_SERVER_ADDRESS_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_JSCSI_CBLOCK_SERVER_ADDRESS_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_JSCSI_PORT_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_JSCSI_PORT_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_JSCSI_SERVER_ADDRESS_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_JSCSI_SERVER_ADDRESS_KEY;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_KEY;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_KEY;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_KEY;

/**
 * This class runs the target server process.
 */
public final class SCSITargetDaemon {
  public static void main(String[] args) throws Exception {
    OzoneConfiguration ozoneConf = new OzoneConfiguration();

    RPC.setProtocolEngine(ozoneConf, CBlockClientServerProtocolPB.class,
        ProtobufRpcEngine.class);
    long containerSizeGB = ozoneConf.getInt(DFS_CBLOCK_CONTAINER_SIZE_GB_KEY,
        DFS_CBLOCK_CONTAINER_SIZE_GB_DEFAULT);
    ContainerOperationClient.setContainerSizeB(
        containerSizeGB * OzoneConsts.GB);
    String jscsiServerAddress = ozoneConf.get(
        DFS_CBLOCK_JSCSI_SERVER_ADDRESS_KEY,
        DFS_CBLOCK_JSCSI_SERVER_ADDRESS_DEFAULT);
    String cbmIPAddress = ozoneConf.get(
        DFS_CBLOCK_JSCSI_CBLOCK_SERVER_ADDRESS_KEY,
        DFS_CBLOCK_JSCSI_CBLOCK_SERVER_ADDRESS_DEFAULT
    );
    int cbmPort = ozoneConf.getInt(
        DFS_CBLOCK_JSCSI_PORT_KEY,
        DFS_CBLOCK_JSCSI_PORT_DEFAULT
    );

    String scmAddress = ozoneConf.get(OZONE_SCM_CLIENT_BIND_HOST_KEY,
        OZONE_SCM_CLIENT_BIND_HOST_DEFAULT);
    int scmClientPort = ozoneConf.getInt(OZONE_SCM_CLIENT_PORT_KEY,
        OZONE_SCM_CLIENT_PORT_DEFAULT);
    int scmDatanodePort = ozoneConf.getInt(OZONE_SCM_DATANODE_PORT_KEY,
        OZONE_SCM_DATANODE_PORT_DEFAULT);

    String scmClientAddress = scmAddress + ":" + scmClientPort;
    String scmDataodeAddress = scmAddress + ":" + scmDatanodePort;

    ozoneConf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, scmClientAddress);
    ozoneConf.set(OZONE_SCM_DATANODE_ADDRESS_KEY, scmDataodeAddress);

    InetSocketAddress cbmAddress = new InetSocketAddress(
        cbmIPAddress, cbmPort);
    long version = RPC.getProtocolVersion(
        CBlockServiceProtocolPB.class);
    CBlockClientProtocolClientSideTranslatorPB cbmClient =
        new CBlockClientProtocolClientSideTranslatorPB(
            RPC.getProxy(CBlockClientServerProtocolPB.class, version,
                cbmAddress, UserGroupInformation.getCurrentUser(), ozoneConf,
                NetUtils.getDefaultSocketFactory(ozoneConf), 5000)
        );
    CBlockManagerHandler cbmHandler = new CBlockManagerHandler(cbmClient);

    String advertisedAddress = ozoneConf.
        getTrimmed(DFS_CBLOCK_ISCSI_ADVERTISED_IP, jscsiServerAddress);

    int advertisedPort = ozoneConf.
        getInt(DFS_CBLOCK_ISCSI_ADVERTISED_PORT,
            DFS_CBLOCK_ISCSI_ADVERTISED_PORT_DEFAULT);

    Configuration jscsiConfig =
        new Configuration(jscsiServerAddress,
            advertisedAddress,
            advertisedPort);
    DefaultMetricsSystem.initialize("CBlockMetrics");
    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    CBlockTargetServer targetServer = new CBlockTargetServer(
        ozoneConf, jscsiConfig, cbmHandler, metrics);

    targetServer.call();
  }

  private SCSITargetDaemon() {

  }
}
