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

import javax.servlet.ServletContext;
import javax.servlet.http.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.*;
import org.znerd.xmlenc.XMLOutputter;

/**
 * A base class for the servlets in DFS.
 */
abstract class DfsServlet extends HttpServlet {
  static final Log LOG = LogFactory.getLog(DfsServlet.class.getCanonicalName());

  /** Get {@link UserGroupInformation} from request */
  protected UnixUserGroupInformation getUGI(HttpServletRequest request) {
    String ugi = request.getParameter("ugi");
    try {
      return new UnixUserGroupInformation(ugi.split(","));
    }
    catch(Exception e) {
      LOG.warn("Invalid ugi (= " + ugi + ")");
    }
    return JspHelper.webUGI;
  }

  /**
   * Create a {@link NameNode} proxy from the current {@link ServletContext}. 
   */
  protected ClientProtocol createNameNodeProxy(UnixUserGroupInformation ugi
      ) throws IOException {
    ServletContext context = getServletContext();
    NameNode nn = (NameNode)context.getAttribute("name.node");
    Configuration conf = new Configuration(
        (Configuration)context.getAttribute("name.conf"));
    UnixUserGroupInformation.saveToConf(conf,
        UnixUserGroupInformation.UGI_PROPERTY_NAME, ugi);
    return DFSClient.createNamenode(nn.getNameNodeAddress(), conf);
  }

  static void writeRemoteException(String path, RemoteException re,
      XMLOutputter doc) throws IOException {
    doc.startTag("RemoteException");
    doc.attribute("path", path);
    doc.attribute("class", re.getClassName());
    String msg = re.getLocalizedMessage();
    int i = msg.indexOf("\n");
    if (i >= 0) {
      msg = msg.substring(0, i);
    }
    doc.attribute("message", msg.substring(msg.indexOf(":") + 1).trim());
    doc.endTag();
  }
}