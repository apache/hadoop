/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.ha;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneIllegalArgumentException;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODE_ID_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

/**
 * Class which maintains peer information and it's own OM node information.
 */
public class OMHANodeDetails {

  private static String[] genericConfigKeys = new String[] {
      OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY,
      OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY,
      OMConfigKeys.OZONE_OM_HTTP_BIND_HOST_KEY,
      OMConfigKeys.OZONE_OM_HTTPS_BIND_HOST_KEY,
      OMConfigKeys.OZONE_OM_DB_DIRS,
      OMConfigKeys.OZONE_OM_ADDRESS_KEY,
  };

  public static final Logger LOG =
      LoggerFactory.getLogger(OMHANodeDetails.class);
  private final OMNodeDetails localNodeDetails;
  private final List<OMNodeDetails> peerNodeDetails;

  public OMHANodeDetails(OMNodeDetails localNodeDetails,
      List<OMNodeDetails> peerNodeDetails) {
    this.localNodeDetails = localNodeDetails;
    this.peerNodeDetails = peerNodeDetails;
  }

  public OMNodeDetails getLocalNodeDetails() {
    return localNodeDetails;
  }

  public List< OMNodeDetails > getPeerNodeDetails() {
    return peerNodeDetails;
  }


  /**
   * Inspects and loads OM node configurations.
   *
   * If {@link OMConfigKeys#OZONE_OM_SERVICE_IDS_KEY} is configured with
   * multiple ids and/ or if {@link OMConfigKeys#OZONE_OM_NODE_ID_KEY} is not
   * specifically configured , this method determines the omServiceId
   * and omNodeId by matching the node's address with the configured
   * addresses. When a match is found, it sets the omServicId and omNodeId from
   * the corresponding configuration key. This method also finds the OM peers
   * nodes belonging to the same OM service.
   *
   * @param conf
   */
  public static OMHANodeDetails loadOMHAConfig(OzoneConfiguration conf) {
    InetSocketAddress localRpcAddress = null;
    String localOMServiceId = null;
    String localOMNodeId = null;
    int localRatisPort = 0;
    Collection<String> omServiceIds = conf.getTrimmedStringCollection(
        OZONE_OM_SERVICE_IDS_KEY);

    String knownOMNodeId = conf.get(OZONE_OM_NODE_ID_KEY);
    int found = 0;
    boolean isOMAddressSet = false;

    for (String serviceId : omServiceIds) {
      Collection<String> omNodeIds = OmUtils.getOMNodeIds(conf, serviceId);

      if (omNodeIds.size() == 0) {
        String msg = "Configuration does not have any value set for " +
            OZONE_OM_NODES_KEY + " for service ID " + serviceId + ". List of " +
            "OM Node ID's should be specified for the service ID";
        throw new OzoneIllegalArgumentException(msg);
      }

      List<OMNodeDetails> peerNodesList = new ArrayList<>();
      boolean isPeer;
      for (String nodeId : omNodeIds) {
        if (knownOMNodeId != null && !knownOMNodeId.equals(nodeId)) {
          isPeer = true;
        } else {
          isPeer = false;
        }
        String rpcAddrKey = OmUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
            serviceId, nodeId);
        String rpcAddrStr = OmUtils.getOmRpcAddress(conf, rpcAddrKey);
        if (rpcAddrStr == null || rpcAddrStr.isEmpty()) {
          String msg = "Configuration does not have any value set for " +
              rpcAddrKey + "." + "OM Rpc Address should be set for all node " +
              "IDs for a service ID.";
          throw new OzoneIllegalArgumentException(msg);
        }

        // If OM address is set for any node id, we will not fallback to the
        // default
        isOMAddressSet = true;

        String ratisPortKey = OmUtils.addKeySuffixes(OZONE_OM_RATIS_PORT_KEY,
            serviceId, nodeId);
        int ratisPort = conf.getInt(ratisPortKey, OZONE_OM_RATIS_PORT_DEFAULT);

        InetSocketAddress addr = null;
        try {
          addr = NetUtils.createSocketAddr(rpcAddrStr);
        } catch (Exception e) {
          LOG.warn("Exception in creating socket address " + addr, e);
          continue;
        }
        if (!addr.isUnresolved()) {
          if (!isPeer && OmUtils.isAddressLocal(addr)) {
            localRpcAddress = addr;
            localOMServiceId = serviceId;
            localOMNodeId = nodeId;
            localRatisPort = ratisPort;
            found++;
          } else {
            // This OMNode belongs to same OM service as the current OMNode.
            // Add it to peerNodes list.
            // This OMNode belongs to same OM service as the current OMNode.
            // Add it to peerNodes list.
            peerNodesList.add(getHAOMNodeDetails(conf, serviceId,
                nodeId, addr, ratisPort));
          }
        }
      }
      if (found == 1) {
        LOG.debug("Found one matching OM address with service ID: {} and node" +
            " ID: {}", localOMServiceId, localOMNodeId);

        LOG.info("Found matching OM address with OMServiceId: {}, " +
                "OMNodeId: {}, RPC Address: {} and Ratis port: {}",
            localOMServiceId, localOMNodeId,
            NetUtils.getHostPortString(localRpcAddress), localRatisPort);


        setOMNodeSpecificConfigs(conf, localOMServiceId, localOMNodeId);
        return new OMHANodeDetails(getHAOMNodeDetails(conf, localOMServiceId,
            localOMNodeId, localRpcAddress, localRatisPort), peerNodesList);

      } else if (found > 1) {
        String msg = "Configuration has multiple " + OZONE_OM_ADDRESS_KEY +
            " addresses that match local node's address. Please configure the" +
            " system with " + OZONE_OM_SERVICE_IDS_KEY + " and " +
            OZONE_OM_ADDRESS_KEY;
        throw new OzoneIllegalArgumentException(msg);
      }
    }

    if (!isOMAddressSet) {
      // No OM address is set. Fallback to default
      InetSocketAddress omAddress = OmUtils.getOmAddress(conf);
      int ratisPort = conf.getInt(OZONE_OM_RATIS_PORT_KEY,
          OZONE_OM_RATIS_PORT_DEFAULT);

      LOG.info("Configuration either no {} set. Falling back to the default " +
          "OM address {}", OZONE_OM_ADDRESS_KEY, omAddress);

      return new OMHANodeDetails(getOMNodeDetails(conf, null,
          null, omAddress, ratisPort), new ArrayList<>());

    } else {
      String msg = "Configuration has no " + OZONE_OM_ADDRESS_KEY + " " +
          "address that matches local node's address. Please configure the " +
          "system with " + OZONE_OM_ADDRESS_KEY;
      LOG.info(msg);
      throw new OzoneIllegalArgumentException(msg);
    }
  }

  /**
   * Create Local OM Node Details.
   * @param serviceId - Service ID this OM belongs to,
   * @param nodeId - Node ID of this OM.
   * @param rpcAddress - Rpc Address of the OM.
   * @param ratisPort - Ratis port of the OM.
   * @return OMNodeDetails
   */
  public static OMNodeDetails getOMNodeDetails(OzoneConfiguration conf,
      String serviceId, String nodeId, InetSocketAddress rpcAddress,
      int ratisPort) {

    if (serviceId == null) {
      // If no serviceId is set, take the default serviceID om-service
      serviceId = OzoneConsts.OM_SERVICE_ID_DEFAULT;
      LOG.info("OM Service ID is not set. Setting it to the default ID: {}",
          serviceId);
    }


    // We need to pass null for serviceID and nodeID as this is set for
    // non-HA cluster. This means one node OM cluster.
    String httpAddr = OmUtils.getHttpAddressForOMPeerNode(conf,
        null, null, rpcAddress.getHostName());
    String httpsAddr = OmUtils.getHttpsAddressForOMPeerNode(conf,
        null, null, rpcAddress.getHostName());

    return new OMNodeDetails.Builder()
        .setOMServiceId(serviceId)
        .setOMNodeId(nodeId)
        .setRpcAddress(rpcAddress)
        .setRatisPort(ratisPort)
        .setHttpAddress(httpAddr)
        .setHttpsAddress(httpsAddr)
        .build();

  }


  /**
   * Create Local OM Node Details.
   * @param serviceId - Service ID this OM belongs to,
   * @param nodeId - Node ID of this OM.
   * @param rpcAddress - Rpc Address of the OM.
   * @param ratisPort - Ratis port of the OM.
   * @return OMNodeDetails
   */
  public static OMNodeDetails getHAOMNodeDetails(OzoneConfiguration conf,
      String serviceId, String nodeId, InetSocketAddress rpcAddress,
      int ratisPort) {
    Preconditions.checkNotNull(serviceId);
    Preconditions.checkNotNull(nodeId);

    String httpAddr = OmUtils.getHttpAddressForOMPeerNode(conf,
        serviceId, nodeId, rpcAddress.getHostName());
    String httpsAddr = OmUtils.getHttpsAddressForOMPeerNode(conf,
        serviceId, nodeId, rpcAddress.getHostName());

    return new OMNodeDetails.Builder()
        .setOMServiceId(serviceId)
        .setOMNodeId(nodeId)
        .setRpcAddress(rpcAddress)
        .setRatisPort(ratisPort)
        .setHttpAddress(httpAddr)
        .setHttpsAddress(httpsAddr)
        .build();

  }


  /**
   * Check if any of the following configuration keys have been set using OM
   * Node ID suffixed to the key. If yes, then set the base key with the
   * configured valued.
   *    1. {@link OMConfigKeys#OZONE_OM_HTTP_ADDRESS_KEY}
   *    2. {@link OMConfigKeys#OZONE_OM_HTTPS_ADDRESS_KEY}
   *    3. {@link OMConfigKeys#OZONE_OM_HTTP_BIND_HOST_KEY}
   *    4. {@link OMConfigKeys#OZONE_OM_HTTPS_BIND_HOST_KEY}\
   *    5. {@link OMConfigKeys#OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE}
   *    6. {@link OMConfigKeys#OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY}
   *    7. {@link OMConfigKeys#OZONE_OM_KERBEROS_KEYTAB_FILE_KEY}
   *    8. {@link OMConfigKeys#OZONE_OM_KERBEROS_PRINCIPAL_KEY}
   *    9. {@link OMConfigKeys#OZONE_OM_DB_DIRS}
   *    10. {@link OMConfigKeys#OZONE_OM_ADDRESS_KEY}
   */
  private static void setOMNodeSpecificConfigs(
      OzoneConfiguration ozoneConfiguration, String omServiceId,
      String omNodeId) {

    for (String confKey : genericConfigKeys) {
      String confValue = OmUtils.getConfSuffixedWithOMNodeId(
          ozoneConfiguration, confKey, omServiceId, omNodeId);
      if (confValue != null) {
        LOG.info("Setting configuration key {} with value of key {}: {}",
            confKey, OmUtils.addKeySuffixes(confKey, omNodeId), confValue);
        ozoneConfiguration.set(confKey, confValue);
      }
    }
  }


}
