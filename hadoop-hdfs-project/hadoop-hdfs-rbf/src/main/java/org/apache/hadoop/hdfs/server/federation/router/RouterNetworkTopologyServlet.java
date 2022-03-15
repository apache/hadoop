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
import java.util.Arrays;
import java.util.List;

/**
 * A servlet to print out the network topology from router.
 */
public class RouterNetworkTopologyServlet extends NetworkTopologyServlet {

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
    List<Node> datanodeInfos = Arrays.asList(datanodeReport);

    try (PrintStream out = new PrintStream(
            response.getOutputStream(), false, "UTF-8")) {
      printTopology(out, datanodeInfos, format);
    } catch (Throwable t) {
      String errMsg = "Print network topology failed. "
              + StringUtils.stringifyException(t);
      response.sendError(HttpServletResponse.SC_GONE, errMsg);
      throw new IOException(errMsg);
    } finally {
      response.getOutputStream().close();
    }
  }
}