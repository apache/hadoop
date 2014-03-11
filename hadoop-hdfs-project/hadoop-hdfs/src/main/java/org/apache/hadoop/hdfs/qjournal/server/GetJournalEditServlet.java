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
package org.apache.hadoop.hdfs.qjournal.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.GetImageServlet;
import org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ServletUtil;
import org.apache.hadoop.util.StringUtils;

/**
 * This servlet is used in two cases:
 * <ul>
 * <li>The QuorumJournalManager, when reading edits, fetches the edit streams
 * from the journal nodes.</li>
 * <li>During edits synchronization, one journal node will fetch edits from
 * another journal node.</li>
 * </ul>
 */
@InterfaceAudience.Private
public class GetJournalEditServlet extends HttpServlet {

  private static final long serialVersionUID = -4635891628211723009L;
  private static final Log LOG = LogFactory.getLog(GetJournalEditServlet.class);

  static final String STORAGEINFO_PARAM = "storageInfo";
  static final String JOURNAL_ID_PARAM = "jid";
  static final String SEGMENT_TXID_PARAM = "segmentTxId";

  protected boolean isValidRequestor(HttpServletRequest request, Configuration conf)
      throws IOException {
    String remotePrincipal = request.getUserPrincipal().getName();
    String remoteShortName = request.getRemoteUser();
    if (remotePrincipal == null) { // This really shouldn't happen...
      LOG.warn("Received null remoteUser while authorizing access to " +
          "GetJournalEditServlet");
      return false;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Validating request made by " + remotePrincipal +
          " / " + remoteShortName + ". This user is: " +
          UserGroupInformation.getLoginUser());
    }

    Set<String> validRequestors = new HashSet<String>();
    validRequestors.addAll(DFSUtil.getAllNnPrincipals(conf));
    validRequestors.add(
        SecurityUtil.getServerPrincipal(conf
            .get(DFSConfigKeys.DFS_SECONDARY_NAMENODE_USER_NAME_KEY),
            SecondaryNameNode.getHttpAddress(conf).getHostName()));

    // Check the full principal name of all the configured valid requestors.
    for (String v : validRequestors) {
      if (LOG.isDebugEnabled())
        LOG.debug("isValidRequestor is comparing to valid requestor: " + v);
      if (v != null && v.equals(remotePrincipal)) {
        if (LOG.isDebugEnabled())
          LOG.debug("isValidRequestor is allowing: " + remotePrincipal);
        return true;
      }
    }

    // Additionally, we compare the short name of the requestor to this JN's
    // username, because we want to allow requests from other JNs during
    // recovery, but we can't enumerate the full list of JNs.
    if (remoteShortName.equals(
          UserGroupInformation.getLoginUser().getShortUserName())) {
      if (LOG.isDebugEnabled())
        LOG.debug("isValidRequestor is allowing other JN principal: " +
            remotePrincipal);
      return true;
    }

    if (LOG.isDebugEnabled())
      LOG.debug("isValidRequestor is rejecting: " + remotePrincipal);
    return false;
  }
  
  private boolean checkRequestorOrSendError(Configuration conf,
      HttpServletRequest request, HttpServletResponse response)
          throws IOException {
    if (UserGroupInformation.isSecurityEnabled()
        && !isValidRequestor(request, conf)) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN,
          "Only Namenode and another JournalNode may access this servlet");
      LOG.warn("Received non-NN/JN request for edits from "
          + request.getRemoteHost());
      return false;
    }
    return true;
  }
  
  private boolean checkStorageInfoOrSendError(JNStorage storage,
      HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    String myStorageInfoString = storage.toColonSeparatedString();
    String theirStorageInfoString = StringEscapeUtils.escapeHtml(
        request.getParameter(STORAGEINFO_PARAM));

    if (theirStorageInfoString != null
        && !myStorageInfoString.equals(theirStorageInfoString)) {
      String msg = "This node has storage info '" + myStorageInfoString
          + "' but the requesting node expected '"
          + theirStorageInfoString + "'";
      
      response.sendError(HttpServletResponse.SC_FORBIDDEN, msg);
      LOG.warn("Received an invalid request file transfer request from " +
          request.getRemoteAddr() + ": " + msg);
      return false;
    }
    return true;
  }
  
  @Override
  public void doGet(final HttpServletRequest request,
      final HttpServletResponse response) throws ServletException, IOException {
    FileInputStream editFileIn = null;
    try {
      final ServletContext context = getServletContext();
      final Configuration conf = (Configuration) getServletContext()
          .getAttribute(JspHelper.CURRENT_CONF);
      final String journalId = request.getParameter(JOURNAL_ID_PARAM);
      QuorumJournalManager.checkJournalId(journalId);
      final JNStorage storage = JournalNodeHttpServer
          .getJournalFromContext(context, journalId).getStorage();

      // Check security
      if (!checkRequestorOrSendError(conf, request, response)) {
        return;
      }

      // Check that the namespace info is correct
      if (!checkStorageInfoOrSendError(storage, request, response)) {
        return;
      }
      
      long segmentTxId = ServletUtil.parseLongParam(request,
          SEGMENT_TXID_PARAM);

      FileJournalManager fjm = storage.getJournalManager();
      File editFile;

      synchronized (fjm) {
        // Synchronize on the FJM so that the file doesn't get finalized
        // out from underneath us while we're in the process of opening
        // it up.
        EditLogFile elf = fjm.getLogFile(
            segmentTxId);
        if (elf == null) {
          response.sendError(HttpServletResponse.SC_NOT_FOUND,
              "No edit log found starting at txid " + segmentTxId);
          return;
        }
        editFile = elf.getFile();
        GetImageServlet.setVerificationHeaders(response, editFile);
        GetImageServlet.setFileNameHeaders(response, editFile);
        editFileIn = new FileInputStream(editFile);
      }
      
      DataTransferThrottler throttler = GetImageServlet.getThrottler(conf);

      // send edits
      TransferFsImage.getFileServer(response, editFile, editFileIn, throttler);

    } catch (Throwable t) {
      String errMsg = "getedit failed. " + StringUtils.stringifyException(t);
      response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, errMsg);
      throw new IOException(errMsg);
    } finally {
      IOUtils.closeStream(editFileIn);
    }
  }

  public static String buildPath(String journalId, long segmentTxId,
      NamespaceInfo nsInfo) {
    StringBuilder path = new StringBuilder("/getJournal?");
    try {
      path.append(JOURNAL_ID_PARAM).append("=")
          .append(URLEncoder.encode(journalId, "UTF-8"));
      path.append("&" + SEGMENT_TXID_PARAM).append("=")
          .append(segmentTxId);
      path.append("&" + STORAGEINFO_PARAM).append("=")
          .append(URLEncoder.encode(nsInfo.toColonSeparatedString(), "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      // Never get here -- everyone supports UTF-8
      throw new RuntimeException(e);
    }
    return path.toString();
  }
}
