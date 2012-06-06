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
package org.apache.hadoop.hdfs.server.journalservice;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.GetImageServlet;
import org.apache.hadoop.hdfs.server.namenode.GetImageServlet.GetImageParams;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

/**
 * This class is used by the lagging Journal service to retrieve edit file from
 * another Journal service for sync up.
 */
@InterfaceAudience.Private
public class GetJournalEditServlet extends HttpServlet {

  private static final long serialVersionUID = -4635891628211723009L;
  private static final Log LOG = LogFactory.getLog(GetJournalEditServlet.class);

  // TODO: create security tests
  protected boolean isValidRequestor(String remoteUser, Configuration conf)
      throws IOException {
    if (remoteUser == null) { // This really shouldn't happen...
      LOG.warn("Received null remoteUser while authorizing access to GetJournalEditServlet");
      return false;
    }

    String[] validRequestors = {
        SecurityUtil.getServerPrincipal(conf
            .get(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY), NameNode
            .getAddress(conf).getHostName()),
        SecurityUtil.getServerPrincipal(
            conf.get(DFSConfigKeys.DFS_JOURNAL_KRB_HTTPS_USER_NAME_KEY),
            NameNode.getAddress(conf).getHostName()),
        SecurityUtil.getServerPrincipal(conf
            .get(DFSConfigKeys.DFS_JOURNAL_USER_NAME_KEY),
            NameNode.getAddress(conf).getHostName()) };

    for (String v : validRequestors) {
      if (v != null && v.equals(remoteUser)) {
        if (LOG.isDebugEnabled())
          LOG.debug("isValidRequestor is allowing: " + remoteUser);
        return true;
      }
    }
    if (LOG.isDebugEnabled())
      LOG.debug("isValidRequestor is rejecting: " + remoteUser);
    return false;
  }

  @Override
  public void doGet(final HttpServletRequest request,
      final HttpServletResponse response) throws ServletException, IOException {
    try {
      ServletContext context = getServletContext();
      final NNStorage storage = JournalHttpServer
          .getJournalFromContext(context).getStorage();

      final GetImageParams parsedParams = new GetImageParams(request, response);

      final Configuration conf = (Configuration) getServletContext()
          .getAttribute(JspHelper.CURRENT_CONF);

      if (UserGroupInformation.isSecurityEnabled()
          && !isValidRequestor(request.getRemoteUser(), conf)) {
        response
            .sendError(HttpServletResponse.SC_FORBIDDEN,
                "Only Namenode and another Journal service may access this servlet");
        LOG.warn("Received non-NN/Journal request for edits from "
            + request.getRemoteHost());
        return;
      }

      String myStorageInfoString = storage.toColonSeparatedString();
      String theirStorageInfoString = parsedParams.getStorageInfoString();
      if (theirStorageInfoString != null
          && !myStorageInfoString.equals(theirStorageInfoString)) {
        response
            .sendError(HttpServletResponse.SC_FORBIDDEN,
                "This node has storage info " + myStorageInfoString
                    + " but the requesting node expected "
                    + theirStorageInfoString);
        LOG.warn("Received an invalid request file transfer request "
            + " with storage info " + theirStorageInfoString);
        return;
      }

      UserGroupInformation.getCurrentUser().doAs(
          new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              if (parsedParams.isGetEdit()) {
                long startTxId = parsedParams.getStartTxId();
                long endTxId = parsedParams.getEndTxId();
                File editFile = storage.findFinalizedEditsFile(startTxId,
                    endTxId);

                GetImageServlet.setVerificationHeaders(response, editFile);
                GetImageServlet.setFileNameHeaders(response, editFile);
                
                DataTransferThrottler throttler = GetImageServlet.getThrottler(conf);

                // send edits
                FaultInjector.instance.beforeSendEdits();
                ServletOutputStream output = response.getOutputStream();
                try {
                  TransferFsImage.getFileServer(output, editFile, throttler);
                } finally {
                  if (output != null)
                    output.close();
                }

              } else {
                response
                    .sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                        "The server only accepts getedit request. This request is not getedit.");
              }
              return null;
            }
          });

    } catch (Exception ie) {
      String errMsg = "getedit failed. " + StringUtils.stringifyException(ie);
      response.sendError(HttpServletResponse.SC_GONE, errMsg);
      throw new IOException(errMsg);
    }
  }
  
  /**
   * Static nested class only for fault injection. Typical usage of this class
   * is to make a Mockito object of this class, and then use the Mackito object
   * to control the behavior of the fault injection.
   */
  public static class FaultInjector {
    public static FaultInjector instance = 
        new FaultInjector();
    
    public void beforeSendEdits() throws IOException {}
  }
}