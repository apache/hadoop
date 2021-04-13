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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.StringUtils;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

/**
 * A servlet to print out the network topology.
 */
@InterfaceAudience.Private
public class NetworkTopologyServlet extends DfsServlet {

  public static final String PATH_SPEC = "/topology";

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    final ServletContext context = getServletContext();
    NameNode nn = NameNodeHttpServer.getNameNodeFromContext(context);
    BlockManager bm = nn.getNamesystem().getBlockManager();
    List<Node> leaves = bm.getDatanodeManager().getNetworkTopology()
        .getLeaves(NodeBase.ROOT);

    response.setContentType("text/plain; charset=UTF-8");
    try (PrintStream out = new PrintStream(
            response.getOutputStream(), false, "UTF-8")) {
      printTopology(out, leaves);
    } catch (Throwable t) {
      String errMsg = "Print network topology failed. "
              + StringUtils.stringifyException(t);
      response.sendError(HttpServletResponse.SC_GONE, errMsg);
      throw new IOException(errMsg);
    } finally {
      response.getOutputStream().close();
    }
  }

  /**
   * Display each rack and the nodes assigned to that rack, as determined
   * by the NameNode, in a hierarchical manner.  The nodes and racks are
   * sorted alphabetically.
   *
   * @param stream print stream
   * @param leaves leaves nodes under base scope
   */
  public void printTopology(PrintStream stream, List<Node> leaves) {
    if (leaves.size() == 0) {
      stream.print("No DataNodes");
      return;
    }

    // Build a map of rack -> nodes
    HashMap<String, TreeSet<String>> tree =
        new HashMap<String, TreeSet<String>>();
    for(Node dni : leaves) {
      String location = dni.getNetworkLocation();
      String name = dni.getName();

      if(!tree.containsKey(location)) {
        tree.put(location, new TreeSet<String>());
      }

      tree.get(location).add(name);
    }

    // Sort the racks (and nodes) alphabetically, display in order
    ArrayList<String> racks = new ArrayList<String>(tree.keySet());
    Collections.sort(racks);

    for(String r : racks) {
      stream.println("Rack: " + r);
      TreeSet<String> nodes = tree.get(r);

      for(String n : nodes) {
        stream.print("   " + n);
        String hostname = NetUtils.getHostNameOfIP(n);
        if(hostname != null) {
          stream.print(" (" + hostname + ")");
        }
        stream.println();
      }
      stream.println();
    }
  }
}
