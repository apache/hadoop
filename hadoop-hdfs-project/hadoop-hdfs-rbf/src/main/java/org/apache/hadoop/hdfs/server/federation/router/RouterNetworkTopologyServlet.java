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
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NetworkTopologyServlet;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.util.StringUtils;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * A servlet to print out the network topology from router.
 */
public class RouterNetworkTopologyServlet extends NetworkTopologyServlet {

  public static final String SERVLET_NAME = "topology";
  public static final String PATH_SPEC = "/topology";

  protected static final String FORMAT_JSON = "json";
  protected static final String FORMAT_TEXT = "text";

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    final ServletContext context = getServletContext();

    String format = parseAcceptHeader(request);
    if (FORMAT_TEXT.equals(format)) {
      response.setContentType("text/plain; charset=UTF-8");
    } else if (FORMAT_JSON.equals(format)) {
      response.setContentType("application/json; charset=UTF-8");
    }

    Router router = RouterHttpServer.getRouterFromContext(context);
    DatanodeInfo[] datanodeReport =
        router.getRpcServer().getDatanodeReport(
            HdfsConstants.DatanodeReportType.ALL);

    try (PrintStream out = new PrintStream(
            response.getOutputStream(), false, "UTF-8")) {
      printTopology(out, datanodeReport, format);
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
   * @param datanodeInfos datanode Infos
   * @param format the response format
   */
  private void printTopology(PrintStream stream, DatanodeInfo[] datanodeInfos,
      String format) throws IOException, BadFormatException {
    if (datanodeInfos.length == 0) {
      stream.print("No DataNodes");
      return;
    }

    // Build a map of rack -> nodes
    Map<String, TreeSet<String>> tree = new HashMap<>();
    for(Node dni : datanodeInfos) {
      String location = dni.getNetworkLocation();
      String name = dni.getName();

      tree.putIfAbsent(location, new TreeSet<>());
      tree.get(location).add(name);
    }

    // Sort the racks (and nodes) alphabetically, display in order
    ArrayList<String> racks = new ArrayList<>(tree.keySet());
    Collections.sort(racks);

    if (FORMAT_JSON.equals(format)) {
      printJsonFormat(stream, tree, racks);
    } else if (FORMAT_TEXT.equals(format)) {
      printTextFormat(stream, tree, racks);
    } else {
      throw new BadFormatException("Bad format: " + format);
    }
  }
}