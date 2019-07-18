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

package org.apache.hadoop.hdds.scm.cli;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.STALE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Callable;

/**
 * Handler of printTopology command.
 */
@CommandLine.Command(
    name = "printTopology",
    description = "Print a tree of the network topology as reported by SCM",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class TopologySubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private SCMCLI parent;

  private static List<HddsProtos.NodeState> stateArray = new ArrayList<>();

  static {
    stateArray.add(HEALTHY);
    stateArray.add(STALE);
    stateArray.add(DEAD);
    stateArray.add(DECOMMISSIONING);
    stateArray.add(DECOMMISSIONED);
  }

  @CommandLine.Option(names = {"-o", "--order"},
      description = "Print Topology ordered by network location")
  private boolean order;

  @Override
  public Void call() throws Exception {
    try (ScmClient scmClient = parent.createScmClient()) {
      for (HddsProtos.NodeState state : stateArray) {
        List<HddsProtos.Node> nodes = scmClient.queryNode(state,
            HddsProtos.QueryScope.CLUSTER, "");
        if (nodes != null && nodes.size() > 0) {
          // show node state
          System.out.println("State = " + state.toString());
          if (order) {
            printOrderedByLocation(nodes);
          } else {
            printNodesWithLocation(nodes);
          }
        }
      }
      return null;
    }
  }

  // Format
  // Location: rack1
  //  ipAddress(hostName)
  private void printOrderedByLocation(List<HddsProtos.Node> nodes) {
    HashMap<String, TreeSet<DatanodeDetails>> tree =
        new HashMap<>();
    for (HddsProtos.Node node : nodes) {
      String location = node.getNodeID().getNetworkLocation();
      if (location != null && !tree.containsKey(location)) {
        tree.put(location, new TreeSet<>());
      }
      tree.get(location).add(DatanodeDetails.getFromProtoBuf(node.getNodeID()));
    }
    ArrayList<String> locations = new ArrayList<>(tree.keySet());
    Collections.sort(locations);

    locations.forEach(location -> {
      System.out.println("Location: " + location);
      tree.get(location).forEach(node -> {
        System.out.println(" " + node.getIpAddress() + "(" + node.getHostName()
            + ")");
      });
    });
  }


  // Format "ipAddress(hostName)    networkLocation"
  private void printNodesWithLocation(Collection<HddsProtos.Node> nodes) {
    nodes.forEach(node -> {
      System.out.print(" " + node.getNodeID().getIpAddress() + "(" +
          node.getNodeID().getHostName() + ")");
      System.out.println("    " +
          (node.getNodeID().getNetworkLocation() != null ?
              node.getNodeID().getNetworkLocation() : "NA"));
    });
  }
}