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

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DatanodeJspHelper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ServletUtil;
import org.znerd.xmlenc.XMLOutputter;

/** Servlets for file checksum */
@InterfaceAudience.Private
public class FileChecksumServlets {
  /** Redirect file checksum queries to an appropriate datanode. */
  @InterfaceAudience.Private
  public static class RedirectServlet extends DfsServlet {
    /** For java.io.Serializable */
    private static final long serialVersionUID = 1L;
  
    /** Create a redirection URL */
    private URL createRedirectURL(UserGroupInformation ugi, DatanodeID host,
        HttpServletRequest request, NameNode nn) 
        throws IOException {
      final String hostname = host instanceof DatanodeInfo 
          ? ((DatanodeInfo)host).getHostName() : host.getIpAddr();
      final String scheme = request.getScheme();
      final int port = "https".equals(scheme)
          ? (Integer)getServletContext().getAttribute(DFSConfigKeys.DFS_DATANODE_HTTPS_PORT_KEY)
          : host.getInfoPort();
      final String encodedPath = ServletUtil.getRawPath(request, "/fileChecksum");

      String dtParam = "";
      if (UserGroupInformation.isSecurityEnabled()) {
        String tokenString = ugi.getTokens().iterator().next().encodeToUrlString();
        dtParam = JspHelper.getDelegationTokenUrlParam(tokenString);
      }
      String addr = nn.getNameNodeAddressHostPortString();
      String addrParam = JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, addr);

      return new URL(scheme, hostname, port, 
          "/getFileChecksum" + encodedPath + '?' +
          "ugi=" + ServletUtil.encodeQueryValue(ugi.getShortUserName()) + 
          dtParam + addrParam);
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response
        ) throws ServletException, IOException {
      final ServletContext context = getServletContext();
      final Configuration conf = NameNodeHttpServer.getConfFromContext(context);
      final UserGroupInformation ugi = getUGI(request, conf);
      final NameNode namenode = NameNodeHttpServer.getNameNodeFromContext(
          context);
      final DatanodeID datanode = NamenodeJspHelper.getRandomDatanode(namenode);
      try {
        response.sendRedirect(
            createRedirectURL(ugi, datanode, request, namenode).toString());
      } catch (IOException e) {
        response.sendError(400, e.getMessage());
      }
    }
  }
  
  /** Get FileChecksum */
  @InterfaceAudience.Private
  public static class GetServlet extends DfsServlet {
    /** For java.io.Serializable */
    private static final long serialVersionUID = 1L;
    
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response
        ) throws ServletException, IOException {
      final PrintWriter out = response.getWriter();
      final String path = ServletUtil.getDecodedPath(request, "/getFileChecksum");
      final XMLOutputter xml = new XMLOutputter(out, "UTF-8");
      xml.declaration();

      final ServletContext context = getServletContext();
      final DataNode datanode = (DataNode) context.getAttribute("datanode");
      final Configuration conf = 
        new HdfsConfiguration(datanode.getConf());
      
      try {
        final DFSClient dfs = DatanodeJspHelper.getDFSClient(request, 
            datanode, conf, getUGI(request, conf));
        final MD5MD5CRC32FileChecksum checksum = dfs.getFileChecksum(path);
        MD5MD5CRC32FileChecksum.write(xml, checksum);
      } catch(IOException ioe) {
        writeXml(ioe, path, xml);
      } catch (InterruptedException e) {
        writeXml(e, path, xml);
      }
      xml.endDocument();
    }
  }
}
