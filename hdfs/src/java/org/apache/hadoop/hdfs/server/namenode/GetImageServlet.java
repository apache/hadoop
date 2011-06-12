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

import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.io.*;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.security.SecurityUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;

/**
 * This class is used in Namesystem's jetty to retrieve a file.
 * Typically used by the Secondary NameNode to retrieve image and
 * edit file for periodic checkpointing.
 */
@InterfaceAudience.Private
public class GetImageServlet extends HttpServlet {
  private static final long serialVersionUID = -7669068179452648952L;

  private static final Log LOG = LogFactory.getLog(GetImageServlet.class);

  private static final String TXID_PARAM = "txid";
  private static final String START_TXID_PARAM = "startTxId";
  private static final String END_TXID_PARAM = "endTxId";
  
  private static Set<Long> currentlyDownloadingCheckpoints =
    Collections.<Long>synchronizedSet(new HashSet<Long>());
  
  public void doGet(final HttpServletRequest request,
                    final HttpServletResponse response
                    ) throws ServletException, IOException {
    try {
      ServletContext context = getServletContext();
      final FSImage nnImage = (FSImage)context.getAttribute("name.system.image");
      final GetImageParams parsedParams = new GetImageParams(request, response);
      final Configuration conf = 
        (Configuration)getServletContext().getAttribute(JspHelper.CURRENT_CONF);
      
      if(UserGroupInformation.isSecurityEnabled() && 
          !isValidRequestor(request.getRemoteUser(), conf)) {
        response.sendError(HttpServletResponse.SC_FORBIDDEN, 
            "Only Namenode and Secondary Namenode may access this servlet");
        LOG.warn("Received non-NN/SNN request for image or edits from " 
            + request.getRemoteHost());
        return;
      }
      
      UserGroupInformation.getCurrentUser().doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          if (parsedParams.isGetImage()) {
            long txid = parsedParams.getTxId();
            File imageFile = nnImage.getStorage().getFsImageName(txid);
            setVerificationHeaders(response, imageFile);
            // send fsImage
            TransferFsImage.getFileServer(response.getOutputStream(), imageFile,
                getThrottler(conf)); 
          } else if (parsedParams.isGetEdit()) {
            long startTxId = parsedParams.getStartTxId();
            long endTxId = parsedParams.getEndTxId();
            
            File editFile = nnImage.getStorage()
                .findFinalizedEditsFile(startTxId, endTxId);
            setVerificationHeaders(response, editFile);
            
            // send edits
            TransferFsImage.getFileServer(response.getOutputStream(), editFile,
                getThrottler(conf));
          } else if (parsedParams.isPutImage()) {
            final long txid = parsedParams.getTxId();

            if (! currentlyDownloadingCheckpoints.add(txid)) {
              throw new IOException(
                  "Another checkpointer is already in the process of uploading a" +
                  " checkpoint made at transaction ID " + txid);
            }

            try {
              if (nnImage.getStorage().findImageFile(txid) != null) {
                throw new IOException(
                    "Another checkpointer already uploaded an checkpoint " +
                    "for txid " + txid);
              }
              
              // issue a HTTP get request to download the new fsimage 
              nnImage.validateCheckpointUpload(parsedParams.getToken());
              MD5Hash downloadImageDigest = reloginIfNecessary().doAs(
                  new PrivilegedExceptionAction<MD5Hash>() {
                  @Override
                  public MD5Hash run() throws Exception {
                    return TransferFsImage.downloadImageToStorage(
                        parsedParams.getInfoServer(), txid,
                        nnImage.getStorage(), true);
                    }
              });
              nnImage.saveDigestAndRenameCheckpointImage(txid, downloadImageDigest);
              
              // Now that we have a new checkpoint, we might be able to
              // remove some old ones.
              nnImage.getStorage().archiveOldStorage();
            } finally {
              currentlyDownloadingCheckpoints.remove(txid);
            }
          }
          return null;
        }
        
        // We may have lost our ticket since the last time we tried to open
        // an http connection, so log in just in case.
        private UserGroupInformation reloginIfNecessary() throws IOException {
          // This method is only called on the NN, therefore it is safe to
          // use these key values.
          return UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                  SecurityUtil.getServerPrincipal(conf
                      .get(DFSConfigKeys.DFS_NAMENODE_KRB_HTTPS_USER_NAME_KEY),
                      NameNode.getAddress(conf).getHostName()),
              conf.get(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY));
        }       
      });
      
    } catch (Exception ie) {
      String errMsg = "GetImage failed. " + StringUtils.stringifyException(ie);
      response.sendError(HttpServletResponse.SC_GONE, errMsg);
      throw new IOException(errMsg);
    } finally {
      response.getOutputStream().close();
    }
  }
  
  /**
   * Construct a throttler from conf
   * @param conf configuration
   * @return a data transfer throttler
   */
  private final DataTransferThrottler getThrottler(Configuration conf) {
    long transferBandwidth = 
      conf.getLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY,
                   DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT);
    DataTransferThrottler throttler = null;
    if (transferBandwidth > 0) {
      throttler = new DataTransferThrottler(transferBandwidth);
    }
    return throttler;
  }
  
  @SuppressWarnings("deprecation")
  protected boolean isValidRequestor(String remoteUser, Configuration conf)
      throws IOException {
    if(remoteUser == null) { // This really shouldn't happen...
      LOG.warn("Received null remoteUser while authorizing access to getImage servlet");
      return false;
    }

    String[] validRequestors = {
        SecurityUtil.getServerPrincipal(conf
            .get(DFSConfigKeys.DFS_NAMENODE_KRB_HTTPS_USER_NAME_KEY), NameNode
            .getAddress(conf).getHostName()),
        SecurityUtil.getServerPrincipal(conf
            .get(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY), NameNode
            .getAddress(conf).getHostName()),
        SecurityUtil.getServerPrincipal(conf
            .get(DFSConfigKeys.DFS_SECONDARY_NAMENODE_KRB_HTTPS_USER_NAME_KEY),
            SecondaryNameNode.getHttpAddress(conf).getHostName()),
        SecurityUtil.getServerPrincipal(conf
            .get(DFSConfigKeys.DFS_SECONDARY_NAMENODE_USER_NAME_KEY),
            SecondaryNameNode.getHttpAddress(conf).getHostName()) };

    for(String v : validRequestors) {
      if(v != null && v.equals(remoteUser)) {
        if(LOG.isDebugEnabled()) LOG.debug("isValidRequestor is allowing: " + remoteUser);
        return true;
      }
    }
    if(LOG.isDebugEnabled()) LOG.debug("isValidRequestor is rejecting: " + remoteUser);
    return false;
  }
  
  /**
   * Set headers for content length, and, if available, md5.
   * @throws IOException 
   */
  private void setVerificationHeaders(HttpServletResponse response, File file)
  throws IOException {
    response.setHeader(TransferFsImage.CONTENT_LENGTH,
        String.valueOf(file.length()));
    MD5Hash hash = MD5FileUtils.readStoredMd5ForFile(file);
    if (hash != null) {
      response.setHeader(TransferFsImage.MD5_HEADER, hash.toString());
    }
  }

  static String getParamStringForImage(long txid) {
    return "getimage=1&" + TXID_PARAM + "=" + txid;
  }

  static String getParamStringForLog(RemoteEditLog log) {
    return "getedit=1&" + START_TXID_PARAM + "=" + log.getStartTxId()
        + "&" + END_TXID_PARAM + "=" + log.getEndTxId();
  }
  
  static class GetImageParams {
    private boolean isGetImage;
    private boolean isGetEdit;
    private boolean isPutImage;
    private int remoteport;
    private String machineName;
    private CheckpointSignature token;
    private long startTxId, endTxId, txId;

    /**
     * @param request the object from which this servlet reads the url contents
     * @param response the object into which this servlet writes the url contents
     * @throws IOException if the request is bad
     */
    public GetImageParams(HttpServletRequest request,
                          HttpServletResponse response
                           ) throws IOException {
      @SuppressWarnings("unchecked")
      Map<String, String[]> pmap = request.getParameterMap();
      isGetImage = isGetEdit = isPutImage = false;
      remoteport = 0;
      machineName = null;
      token = null;

      for (Iterator<String> it = pmap.keySet().iterator(); it.hasNext();) {
        String key = it.next();
        if (key.equals("getimage")) { 
          isGetImage = true;
          txId = parseLongParam(request, TXID_PARAM);
        } else if (key.equals("getedit")) { 
          isGetEdit = true;
          startTxId = parseLongParam(request, START_TXID_PARAM);
          endTxId = parseLongParam(request, END_TXID_PARAM);
        } else if (key.equals("putimage")) { 
          isPutImage = true;
          txId = parseLongParam(request, TXID_PARAM);
        } else if (key.equals("port")) { 
          remoteport = new Integer(pmap.get("port")[0]).intValue();
        } else if (key.equals("machine")) { 
          machineName = pmap.get("machine")[0];
        } else if (key.equals("token")) { 
          token = new CheckpointSignature(pmap.get("token")[0]);
        }
      }

      int numGets = (isGetImage?1:0) + (isGetEdit?1:0);
      if ((numGets > 1) || (numGets == 0) && !isPutImage) {
        throw new IOException("Illegal parameters to TransferFsImage");
      }
    }

    public long getTxId() {
      Preconditions.checkState(isGetImage || isPutImage);
      return txId;
    }
    
    public long getStartTxId() {
      Preconditions.checkState(isGetEdit);
      return startTxId;
    }
    
    public long getEndTxId() {
      Preconditions.checkState(isGetEdit);
      return endTxId;
    }

    boolean isGetEdit() {
      return isGetEdit;
    }

    boolean isGetImage() {
      return isGetImage;
    }

    boolean isPutImage() {
      return isPutImage;
    }

    CheckpointSignature getToken() {
      return token;
    }
    
    String getInfoServer() throws IOException{
      if (machineName == null || remoteport == 0) {
        throw new IOException ("MachineName and port undefined");
      }
      return machineName + ":" + remoteport;
    }
    
    private static long parseLongParam(HttpServletRequest request, String param)
        throws IOException {
      // Parse the 'txid' parameter which indicates which image is to be
      // fetched.
      String paramStr = request.getParameter(param);
      if (paramStr == null) {
        throw new IOException("Invalid request has no " + param + " parameter");
      }
      
      return Long.valueOf(paramStr);
    }
  }
}
