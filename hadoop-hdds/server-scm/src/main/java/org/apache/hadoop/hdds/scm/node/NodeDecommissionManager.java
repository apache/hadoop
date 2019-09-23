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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeAdminManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

/**
 * Class used to manage datanodes scheduled for maintenance or decommission.
 */
public class NodeDecommissionManager {

  private NodeManager nodeManager;
  private PipelineManager pipeLineManager;
  private ContainerManager containerManager;
  private OzoneConfiguration conf;
  private boolean useHostnames;

  private List<DatanodeDetails> pendingNodes = new LinkedList<>();

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeAdminManager.class);


  static class HostDefinition {
    private String rawHostname;
    private String hostname;
    private int port;

    HostDefinition(String hostname) throws InvalidHostStringException {
      this.rawHostname = hostname;
      parseHostname();
    }

    public String getRawHostname() {
      return rawHostname;
    }

    public String getHostname() {
      return hostname;
    }

    public int getPort() {
      return port;
    }

    private void parseHostname() throws InvalidHostStringException{
      try {
        // A URI *must* have a scheme, so just create a fake one
        URI uri = new URI("my://"+rawHostname.trim());
        this.hostname = uri.getHost();
        this.port = uri.getPort();

        if (this.hostname == null) {
          throw new InvalidHostStringException("The string "+rawHostname+
              " does not contain a value hostname or hostname:port definition");
        }
      } catch (URISyntaxException e) {
        throw new InvalidHostStringException(
            "Unable to parse the hoststring "+rawHostname, e);
      }
    }
  }

  private List<DatanodeDetails> mapHostnamesToDatanodes(List<String> hosts)
      throws InvalidHostStringException {
    List<DatanodeDetails> results = new LinkedList<>();
    for (String hostString : hosts) {
      HostDefinition host = new HostDefinition(hostString);
      InetAddress addr;
      try {
        addr = InetAddress.getByName(host.getHostname());
      } catch (UnknownHostException e) {
        throw new InvalidHostStringException("Unable to resolve the host "
            +host.getRawHostname(), e);
      }
      String dnsName;
      if (useHostnames) {
        dnsName = addr.getHostName();
      } else {
        dnsName = addr.getHostAddress();
      }
      List<DatanodeDetails> found = nodeManager.getNodesByAddress(dnsName);
      if (found.size() == 0) {
        throw new InvalidHostStringException("The string " +
            host.getRawHostname()+" resolved to "+dnsName +
            " is not found in SCM");
      } else if (found.size() == 1) {
        if (host.getPort() != -1 &&
            !validateDNPortMatch(host.getPort(), found.get(0))) {
          throw new InvalidHostStringException("The string "+
              host.getRawHostname()+" matched a single datanode, but the "+
              "given port is not used by that Datanode");
        }
        results.add(found.get(0));
      } else if (found.size() > 1) {
        DatanodeDetails match = null;
        for(DatanodeDetails dn : found) {
          if (validateDNPortMatch(host.getPort(), dn)) {
            match = dn;
            break;
          }
        }
        if (match == null) {
          throw new InvalidHostStringException("The string " +
              host.getRawHostname()+ "matched multiple Datanodes, but no "+
              "datanode port matched the given port");
        }
        results.add(match);
      }
    }
    return results;
  }

  /**
   * Check if the passed port is used by the given DatanodeDetails object. If
   * it is, return true, otherwise return false.
   * @param port Port number to check if it is used by the datanode
   * @param dn Datanode to check if it is using the given port
   * @return True if port is used by the datanode. False otherwise.
   */
  private boolean validateDNPortMatch(int port, DatanodeDetails dn) {
    for (DatanodeDetails.Port p : dn.getPorts()) {
      if (p.getValue() == port) {
        return true;
      }
    }
    return false;
  }

  public NodeDecommissionManager(OzoneConfiguration conf,
      NodeManager nodeManager, PipelineManager pipelineManager,
      ContainerManager containerManager) {
    this.conf = conf;
    this.nodeManager = nodeManager;
    this.pipeLineManager = pipelineManager;
    this.containerManager = containerManager;

    useHostnames = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME,
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
  }

  public synchronized void decommissionNodes(List nodes)
      throws InvalidHostStringException {
    List<DatanodeDetails> dns = mapHostnamesToDatanodes(nodes);
    for (DatanodeDetails dn : dns) {
      try {
        startDecommission(dn);
      } catch (NodeNotFoundException e) {
        // We already validated the host strings and retrieved the DnDetails
        // object from the node manager. Therefore we should never get a
        // NodeNotFoundException here expect if the node is remove in the
        // very short window between validation and starting decom. Therefore
        // log a warning and ignore the exception
        LOG.warn("The host {} was not found in SCM. Ignoring the request to "+
            "decommission it", dn.getHostName());
      }
    }
  }

  public synchronized void startDecommission(DatanodeDetails dn)
      throws NodeNotFoundException {
    NodeStatus nodeStatus = getNodeStatus(dn);
    NodeOperationalState opState = nodeStatus.getOperationalState();
    LOG.info("In decommission the op state is {}", opState);
    if (opState != NodeOperationalState.DECOMMISSIONING
        && opState != NodeOperationalState.DECOMMISSIONED) {
      LOG.info("Starting Decommission for node {}", dn);
      nodeManager.setNodeOperationalState(
          dn, NodeOperationalState.DECOMMISSIONING);
      pendingNodes.add(dn);
    } else {
      LOG.info("Start Decommission called on node {} in state {}. Nothing to "+
          "do.", dn, opState);
    }
  }

  public synchronized void recommissionNodes(List nodes)
      throws InvalidHostStringException {
    List<DatanodeDetails> dns = mapHostnamesToDatanodes(nodes);
    for (DatanodeDetails dn : dns) {
      try {
        recommission(dn);
      } catch (NodeNotFoundException e) {
        // We already validated the host strings and retrieved the DnDetails
        // object from the node manager. Therefore we should never get a
        // NodeNotFoundException here expect if the node is remove in the
        // very short window between validation and starting decom. Therefore
        // log a warning and ignore the exception
        LOG.warn("The host {} was not found in SCM. Ignoring the request to "+
            "recommission it", dn.getHostName());
      }
    }
  }

  public synchronized void recommission(DatanodeDetails dn)
      throws NodeNotFoundException{
    NodeStatus nodeStatus = getNodeStatus(dn);
    NodeOperationalState opState = nodeStatus.getOperationalState();
    if (opState != NodeOperationalState.IN_SERVICE) {
      nodeManager.setNodeOperationalState(
          dn, NodeOperationalState.IN_SERVICE);
      pendingNodes.remove(dn);
      LOG.info("Recommissioned node {}", dn);
    } else {
      LOG.info("Recommission called on node {} with state {}. "+
          "Nothing to do.", dn, opState);
    }
  }

  public synchronized void startMaintenanceNodes(List nodes, int endInHours)
      throws InvalidHostStringException {
    List<DatanodeDetails> dns = mapHostnamesToDatanodes(nodes);
    for (DatanodeDetails dn : dns) {
      try {
        startMaintenance(dn, endInHours);
      } catch (NodeNotFoundException e) {
        // We already validated the host strings and retrieved the DnDetails
        // object from the node manager. Therefore we should never get a
        // NodeNotFoundException here expect if the node is remove in the
        // very short window between validation and starting decom. Therefore
        // log a warning and ignore the exception
        LOG.warn("The host {} was not found in SCM. Ignoring the request to "+
            "start maintenance on it", dn.getHostName());
      }
    }
  }

  // TODO - If startMaintenance is called on a host already in maintenance,
  //        then we should update the end time?
  public synchronized void startMaintenance(DatanodeDetails dn, int endInHours)
      throws NodeNotFoundException {
    NodeStatus nodeStatus = getNodeStatus(dn);
    NodeOperationalState opState = nodeStatus.getOperationalState();
    if (opState != NodeOperationalState.ENTERING_MAINTENANCE &&
        opState != NodeOperationalState.IN_MAINTENANCE) {
      nodeManager.setNodeOperationalState(
          dn, NodeOperationalState.ENTERING_MAINTENANCE);
      pendingNodes.add(dn);
      LOG.info("Starting Maintenance for node {}", dn);
    } else {
      LOG.info("Starting Maintenance called on node {} with state {}. "+
          "Nothing to do.", dn, opState);
    }
  }

  private NodeStatus getNodeStatus(DatanodeDetails dn)
      throws NodeNotFoundException {
    NodeStatus nodeStatus = nodeManager.getNodeStatus(dn);
    if (nodeStatus == null) {
      throw new NodeNotFoundException();
    }
    return nodeStatus;
  }

}
