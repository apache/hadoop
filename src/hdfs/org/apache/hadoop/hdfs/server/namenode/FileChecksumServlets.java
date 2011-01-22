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
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

import javax.net.SocketFactory;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.znerd.xmlenc.XMLOutputter;

/** Servlets for file checksum */
public class FileChecksumServlets {
  /** Redirect file checksum queries to an appropriate datanode. */
  public static class RedirectServlet extends DfsServlet {
    /** For java.io.Serializable */
    private static final long serialVersionUID = 1L;
  
    /** {@inheritDoc} */
    public void doGet(HttpServletRequest request, HttpServletResponse response
        ) throws ServletException, IOException {
      final ServletContext context = getServletContext();
      Configuration conf = (Configuration) context.getAttribute(JspHelper.CURRENT_CONF);
      final UserGroupInformation ugi = getUGI(request, conf);
      String tokenString = request.getParameter(JspHelper.DELEGATION_PARAMETER_NAME);
      final NameNode namenode = (NameNode)context.getAttribute("name.node");
      final DatanodeID datanode = namenode.namesystem.getRandomDatanode();
      try {
        final URI uri = 
          createRedirectUri("/getFileChecksum", ugi, datanode, request, tokenString);
        response.sendRedirect(uri.toURL().toString());
      } catch(URISyntaxException e) {
        throw new ServletException(e); 
        //response.getWriter().println(e.toString());
      } catch (IOException e) {
        response.sendError(400, e.getMessage());
      }
    }
  }
  
  /** Get FileChecksum */
  public static class GetServlet extends DfsServlet {
    /** For java.io.Serializable */
    private static final long serialVersionUID = 1L;
    
    /** {@inheritDoc} */
    public void doGet(HttpServletRequest request, HttpServletResponse response
        ) throws ServletException, IOException {
      final PrintWriter out = response.getWriter();
      final String filename = getFilename(request, response);
      final XMLOutputter xml = new XMLOutputter(out, "UTF-8");
      xml.declaration();

      final Configuration conf = new Configuration(DataNode.getDataNode().getConf());
      final int socketTimeout = conf.getInt("dfs.socket.timeout", HdfsConstants.READ_TIMEOUT);
      final SocketFactory socketFactory = NetUtils.getSocketFactory(conf, ClientProtocol.class);

      try {
        ClientProtocol nnproxy = getUGI(request, conf).doAs
        (new PrivilegedExceptionAction<ClientProtocol>() {
          @Override
          public ClientProtocol run() throws IOException {
            return DFSClient.createNamenode(conf);
          }
        });
        
        final MD5MD5CRC32FileChecksum checksum = DFSClient.getFileChecksum(
            filename, nnproxy, socketFactory, socketTimeout);
        MD5MD5CRC32FileChecksum.write(xml, checksum);
      } catch(IOException ioe) {
        writeXml(ioe, filename, xml);
      } catch (InterruptedException e) {
        writeXml(e, filename, xml);
      }
      xml.endDocument();
    }
  }
}
