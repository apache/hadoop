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
package org.apache.hadoop.dfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Redirect queries about the hosted filesystem to an appropriate datanode.
 * @see org.apache.hadoop.dfs.HftpFileSystem
 */
public class FileDataServlet extends HttpServlet {

  static URI getUri(DFSFileInfo i, NameNode nn)
      throws IOException, URISyntaxException {
    final DatanodeInfo host = pickSrcDatanode(i, nn);
    return new URI("http", null, host.getHostName(), host.getInfoPort(),
          "/streamFile", "filename=" + i.getPath(), null);
  }

  private final static int BLOCK_SAMPLE = 5;

  /** Select a datanode to service this request.
   * Currently, this looks at no more than the first five blocks of a file,
   * selecting a datanode randomly from the most represented.
   */
  protected static DatanodeInfo pickSrcDatanode(DFSFileInfo i, NameNode nn)
      throws IOException {
    long sample;
    if (i.getLen() == 0) sample = 1;
    else sample = i.getLen() / i.getBlockSize() > BLOCK_SAMPLE
        ? i.getBlockSize() * BLOCK_SAMPLE - 1
        : i.getLen();
    final LocatedBlocks blks = nn.getBlockLocations(
        i.getPath().toUri().getPath(), 0, sample);
    HashMap<DatanodeInfo, Integer> count = new HashMap<DatanodeInfo, Integer>();
    for (LocatedBlock b : blks.getLocatedBlocks()) {
      for (DatanodeInfo d : b.getLocations()) {
        if (!count.containsKey(d)) {
          count.put(d, 0);
        }
        count.put(d, count.get(d) + 1);
      }
    }
    ArrayList<DatanodeInfo> loc = new ArrayList<DatanodeInfo>();
    int max = 0;
    for (Map.Entry<DatanodeInfo, Integer> e : count.entrySet()) {
      if (e.getValue() > max) {
        loc.clear();
        max = e.getValue();
      }
      if (e.getValue() == max) {
        loc.add(e.getKey());
      }
    }
    final Random r = new Random();
    return loc.get(r.nextInt(loc.size()));
  }

  /**
   * Service a GET request as described below.
   * Request:
   * {@code
   * GET http://<nn>:<port>/data[/<path>] HTTP/1.1
   * }
   */
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {

    try {
      final String path = request.getPathInfo() != null
        ? request.getPathInfo() : "/";
      final NameNode nn = (NameNode)getServletContext().getAttribute("name.node");
      DFSFileInfo info = nn.getFileInfo(path);
      if (!info.isDir()) {
        response.sendRedirect(getUri(info, nn).toURL().toString());
      } else {
        response.sendError(400, "cat: " + path + ": is a directory");
      }
    } catch (URISyntaxException e) {
      response.getWriter().println(e.toString());
    } catch (IOException e) {
      response.sendError(400, e.getMessage());
    }
  }

}

