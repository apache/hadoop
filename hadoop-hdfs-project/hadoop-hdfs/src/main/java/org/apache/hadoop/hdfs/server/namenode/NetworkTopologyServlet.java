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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.StringUtils;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * A servlet to print out the network topology.
 */
@InterfaceAudience.Private
public class NetworkTopologyServlet extends DfsServlet {

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

    NameNode nn = NameNodeHttpServer.getNameNodeFromContext(context);
    BlockManager bm = nn.getNamesystem().getBlockManager();
    List<Node> leaves = bm.getDatanodeManager().getNetworkTopology()
        .getLeaves(NodeBase.ROOT);

    try (PrintStream out = new PrintStream(
            response.getOutputStream(), false, "UTF-8")) {
      printTopology(out, leaves, format);
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
   * @param format the response format
   */
  public void printTopology(PrintStream stream, List<Node> leaves,
      String format) throws BadFormatException, IOException {
    if (leaves.isEmpty()) {
      stream.print("No DataNodes");
      return;
    }

    // Build a map of rack -> nodes
    Map<String, TreeSet<String>> tree = new HashMap<>();
    for(Node dni : leaves) {
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

  private void printJsonFormat(PrintStream stream, Map<String,
      TreeSet<String>> tree, ArrayList<String> racks) throws IOException {
    JsonFactory dumpFactory = new JsonFactory();
    JsonGenerator dumpGenerator = dumpFactory.createGenerator(stream);
    dumpGenerator.writeStartArray();

    for(String r : racks) {
      dumpGenerator.writeStartObject();
      dumpGenerator.writeFieldName(r);
      TreeSet<String> nodes = tree.get(r);
      dumpGenerator.writeStartArray();

      for(String n : nodes) {
        dumpGenerator.writeStartObject();
        dumpGenerator.writeStringField("ip", n);
        String hostname = NetUtils.getHostNameOfIP(n);
        if(hostname != null) {
          dumpGenerator.writeStringField("hostname", hostname);
        }
        dumpGenerator.writeEndObject();
      }
      dumpGenerator.writeEndArray();
      dumpGenerator.writeEndObject();
    }
    dumpGenerator.writeEndArray();
    dumpGenerator.flush();

    if (!dumpGenerator.isClosed()) {
      dumpGenerator.close();
    }
  }

  private void printTextFormat(PrintStream stream, Map<String,
      TreeSet<String>> tree, ArrayList<String> racks) {
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

  @VisibleForTesting
  static String parseAcceptHeader(HttpServletRequest request) {
    String format = request.getHeader(HttpHeaders.ACCEPT);
    return format != null && format.contains(FORMAT_JSON) ?
            FORMAT_JSON : FORMAT_TEXT;
  }

  public static class BadFormatException extends Exception {
    private static final long serialVersionUID = 1L;

    public BadFormatException(String msg) {
      super(msg);
    }
  }
}
