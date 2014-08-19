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

import static org.apache.hadoop.util.Time.monotonicNow;

import java.net.HttpURLConnection;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.io.*;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ServletUtil;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * This class is used in Namesystem's jetty to retrieve/upload a file 
 * Typically used by the Secondary NameNode to retrieve image and
 * edit file for periodic checkpointing in Non-HA deployments.
 * Standby NameNode uses to upload checkpoints in HA deployments.
 */
@InterfaceAudience.Private
public class ImageServlet extends HttpServlet {

  public static final String PATH_SPEC = "/imagetransfer";

  private static final long serialVersionUID = -7669068179452648952L;

  private static final Log LOG = LogFactory.getLog(ImageServlet.class);

  public final static String CONTENT_DISPOSITION = "Content-Disposition";
  public final static String HADOOP_IMAGE_EDITS_HEADER = "X-Image-Edits-Name";
  
  private static final String TXID_PARAM = "txid";
  private static final String START_TXID_PARAM = "startTxId";
  private static final String END_TXID_PARAM = "endTxId";
  private static final String STORAGEINFO_PARAM = "storageInfo";
  private static final String LATEST_FSIMAGE_VALUE = "latest";
  private static final String IMAGE_FILE_TYPE = "imageFile";

  private static final Set<Long> currentlyDownloadingCheckpoints =
    Collections.synchronizedSet(new HashSet<Long>());
  
  @Override
  public void doGet(final HttpServletRequest request,
      final HttpServletResponse response) throws ServletException, IOException {
    try {
      final ServletContext context = getServletContext();
      final FSImage nnImage = NameNodeHttpServer.getFsImageFromContext(context);
      final GetImageParams parsedParams = new GetImageParams(request, response);
      final Configuration conf = (Configuration) context
          .getAttribute(JspHelper.CURRENT_CONF);
      final NameNodeMetrics metrics = NameNode.getNameNodeMetrics();

      validateRequest(context, conf, request, response, nnImage,
          parsedParams.getStorageInfoString());

      UserGroupInformation.getCurrentUser().doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          if (parsedParams.isGetImage()) {
            long txid = parsedParams.getTxId();
            File imageFile = null;
            String errorMessage = "Could not find image";
            if (parsedParams.shouldFetchLatest()) {
              imageFile = nnImage.getStorage().getHighestFsImageName();
            } else {
              errorMessage += " with txid " + txid;
              imageFile = nnImage.getStorage().getFsImage(txid,
                  EnumSet.of(NameNodeFile.IMAGE, NameNodeFile.IMAGE_ROLLBACK));
            }
            if (imageFile == null) {
              throw new IOException(errorMessage);
            }
            CheckpointFaultInjector.getInstance().beforeGetImageSetsHeaders();
            long start = monotonicNow();
            serveFile(imageFile);

            if (metrics != null) { // Metrics non-null only when used inside name node
              long elapsed = monotonicNow() - start;
              metrics.addGetImage(elapsed);
            }
          } else if (parsedParams.isGetEdit()) {
            long startTxId = parsedParams.getStartTxId();
            long endTxId = parsedParams.getEndTxId();
            
            File editFile = nnImage.getStorage()
                .findFinalizedEditsFile(startTxId, endTxId);
            long start = monotonicNow();
            serveFile(editFile);

            if (metrics != null) { // Metrics non-null only when used inside name node
              long elapsed = monotonicNow() - start;
              metrics.addGetEdit(elapsed);
            }
          }
          return null;
        }

        private void serveFile(File file) throws IOException {
          FileInputStream fis = new FileInputStream(file);
          try {
            setVerificationHeadersForGet(response, file);
            setFileNameHeaders(response, file);
            if (!file.exists()) {
              // Potential race where the file was deleted while we were in the
              // process of setting headers!
              throw new FileNotFoundException(file.toString());
              // It's possible the file could be deleted after this point, but
              // we've already opened the 'fis' stream.
              // It's also possible length could change, but this would be
              // detected by the client side as an inaccurate length header.
            }
            // send file
            TransferFsImage.copyFileToStream(response.getOutputStream(),
               file, fis, getThrottler(conf));
          } finally {
            IOUtils.closeStream(fis);
          }
        }
      });
      
    } catch (Throwable t) {
      String errMsg = "GetImage failed. " + StringUtils.stringifyException(t);
      response.sendError(HttpServletResponse.SC_GONE, errMsg);
      throw new IOException(errMsg);
    } finally {
      response.getOutputStream().close();
    }
  }

  private void validateRequest(ServletContext context, Configuration conf,
      HttpServletRequest request, HttpServletResponse response,
      FSImage nnImage, String theirStorageInfoString) throws IOException {

    if (UserGroupInformation.isSecurityEnabled()
        && !isValidRequestor(context, request.getUserPrincipal().getName(),
            conf)) {
      String errorMsg = "Only Namenode, Secondary Namenode, and administrators may access "
          + "this servlet";
      response.sendError(HttpServletResponse.SC_FORBIDDEN, errorMsg);
      LOG.warn("Received non-NN/SNN/administrator request for image or edits from "
          + request.getUserPrincipal().getName()
          + " at "
          + request.getRemoteHost());
      throw new IOException(errorMsg);
    }

    String myStorageInfoString = nnImage.getStorage().toColonSeparatedString();
    if (theirStorageInfoString != null
        && !myStorageInfoString.equals(theirStorageInfoString)) {
      String errorMsg = "This namenode has storage info " + myStorageInfoString
          + " but the secondary expected " + theirStorageInfoString;
      response.sendError(HttpServletResponse.SC_FORBIDDEN, errorMsg);
      LOG.warn("Received an invalid request file transfer request "
          + "from a secondary with storage info " + theirStorageInfoString);
      throw new IOException(errorMsg);
    }
  }

  public static void setFileNameHeaders(HttpServletResponse response,
      File file) {
    response.setHeader(CONTENT_DISPOSITION, "attachment; filename=" +
        file.getName());
    response.setHeader(HADOOP_IMAGE_EDITS_HEADER, file.getName());
  }
  
  /**
   * Construct a throttler from conf
   * @param conf configuration
   * @return a data transfer throttler
   */
  public final static DataTransferThrottler getThrottler(Configuration conf) {
    long transferBandwidth = 
      conf.getLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY,
                   DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT);
    DataTransferThrottler throttler = null;
    if (transferBandwidth > 0) {
      throttler = new DataTransferThrottler(transferBandwidth);
    }
    return throttler;
  }
  
  @VisibleForTesting
  static boolean isValidRequestor(ServletContext context, String remoteUser,
      Configuration conf) throws IOException {
    if (remoteUser == null) { // This really shouldn't happen...
      LOG.warn("Received null remoteUser while authorizing access to getImage servlet");
      return false;
    }

    Set<String> validRequestors = new HashSet<String>();

    validRequestors.add(SecurityUtil.getServerPrincipal(conf
        .get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY),
        NameNode.getAddress(conf).getHostName()));
    validRequestors.add(SecurityUtil.getServerPrincipal(
        conf.get(DFSConfigKeys.DFS_SECONDARY_NAMENODE_KERBEROS_PRINCIPAL_KEY),
        SecondaryNameNode.getHttpAddress(conf).getHostName()));

    if (HAUtil.isHAEnabled(conf, DFSUtil.getNamenodeNameServiceId(conf))) {
      Configuration otherNnConf = HAUtil.getConfForOtherNode(conf);
      validRequestors.add(SecurityUtil.getServerPrincipal(otherNnConf
          .get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY),
          NameNode.getAddress(otherNnConf).getHostName()));
    }

    for (String v : validRequestors) {
      if (v != null && v.equals(remoteUser)) {
        LOG.info("ImageServlet allowing checkpointer: " + remoteUser);
        return true;
      }
    }

    if (HttpServer2.userHasAdministratorAccess(context, remoteUser)) {
      LOG.info("ImageServlet allowing administrator: " + remoteUser);
      return true;
    }

    LOG.info("ImageServlet rejecting: " + remoteUser);
    return false;
  }
  
  /**
   * Set headers for content length, and, if available, md5.
   * @throws IOException 
   */
  public static void setVerificationHeadersForGet(HttpServletResponse response,
      File file) throws IOException {
    response.setHeader(TransferFsImage.CONTENT_LENGTH,
        String.valueOf(file.length()));
    MD5Hash hash = MD5FileUtils.readStoredMd5ForFile(file);
    if (hash != null) {
      response.setHeader(TransferFsImage.MD5_HEADER, hash.toString());
    }
  }
  
  static String getParamStringForMostRecentImage() {
    return "getimage=1&" + TXID_PARAM + "=" + LATEST_FSIMAGE_VALUE;
  }

  static String getParamStringForImage(NameNodeFile nnf, long txid,
      StorageInfo remoteStorageInfo) {
    final String imageType = nnf == null ? "" : "&" + IMAGE_FILE_TYPE + "="
        + nnf.name();
    return "getimage=1&" + TXID_PARAM + "=" + txid
      + imageType
      + "&" + STORAGEINFO_PARAM + "=" +
      remoteStorageInfo.toColonSeparatedString();
  }

  static String getParamStringForLog(RemoteEditLog log,
      StorageInfo remoteStorageInfo) {
    return "getedit=1&" + START_TXID_PARAM + "=" + log.getStartTxId()
        + "&" + END_TXID_PARAM + "=" + log.getEndTxId()
        + "&" + STORAGEINFO_PARAM + "=" +
          remoteStorageInfo.toColonSeparatedString();
  }

  static class GetImageParams {
    private boolean isGetImage;
    private boolean isGetEdit;
    private NameNodeFile nnf;
    private long startTxId, endTxId, txId;
    private String storageInfoString;
    private boolean fetchLatest;

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
      isGetImage = isGetEdit = fetchLatest = false;

      for (Map.Entry<String, String[]> entry : pmap.entrySet()) {
        String key = entry.getKey();
        String[] val = entry.getValue();
        if (key.equals("getimage")) { 
          isGetImage = true;
          try {
            txId = ServletUtil.parseLongParam(request, TXID_PARAM);
            String imageType = ServletUtil.getParameter(request, IMAGE_FILE_TYPE);
            nnf = imageType == null ? NameNodeFile.IMAGE : NameNodeFile
                .valueOf(imageType);
          } catch (NumberFormatException nfe) {
            if (request.getParameter(TXID_PARAM).equals(LATEST_FSIMAGE_VALUE)) {
              fetchLatest = true;
            } else {
              throw nfe;
            }
          }
        } else if (key.equals("getedit")) { 
          isGetEdit = true;
          startTxId = ServletUtil.parseLongParam(request, START_TXID_PARAM);
          endTxId = ServletUtil.parseLongParam(request, END_TXID_PARAM);
        } else if (key.equals(STORAGEINFO_PARAM)) {
          storageInfoString = val[0];
        }
      }

      int numGets = (isGetImage?1:0) + (isGetEdit?1:0);
      if ((numGets > 1) || (numGets == 0)) {
        throw new IOException("Illegal parameters to TransferFsImage");
      }
    }

    public String getStorageInfoString() {
      return storageInfoString;
    }

    public long getTxId() {
      Preconditions.checkState(isGetImage);
      return txId;
    }

    public NameNodeFile getNameNodeFile() {
      Preconditions.checkState(isGetImage);
      return nnf;
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

    boolean shouldFetchLatest() {
      return fetchLatest;
    }
    
  }

  /**
   * Set headers for image length and if available, md5.
   * 
   * @throws IOException
   */
  static void setVerificationHeadersForPut(HttpURLConnection connection,
      File file) throws IOException {
    connection.setRequestProperty(TransferFsImage.CONTENT_LENGTH,
        String.valueOf(file.length()));
    MD5Hash hash = MD5FileUtils.readStoredMd5ForFile(file);
    if (hash != null) {
      connection
          .setRequestProperty(TransferFsImage.MD5_HEADER, hash.toString());
    }
  }

  /**
   * Set the required parameters for uploading image
   * 
   * @param httpMethod instance of method to set the parameters
   * @param storage colon separated storageInfo string
   * @param txid txid of the image
   * @param imageFileSize size of the imagefile to be uploaded
   * @param nnf NameNodeFile Type
   * @return Returns map of parameters to be used with PUT request.
   */
  static Map<String, String> getParamsForPutImage(Storage storage, long txid,
      long imageFileSize, NameNodeFile nnf) {
    Map<String, String> params = new HashMap<String, String>();
    params.put(TXID_PARAM, Long.toString(txid));
    params.put(STORAGEINFO_PARAM, storage.toColonSeparatedString());
    // setting the length of the file to be uploaded in separate property as
    // Content-Length only supports up to 2GB
    params.put(TransferFsImage.FILE_LENGTH, Long.toString(imageFileSize));
    params.put(IMAGE_FILE_TYPE, nnf.name());
    return params;
  }

  @Override
  protected void doPut(final HttpServletRequest request,
      final HttpServletResponse response) throws ServletException, IOException {
    try {
      ServletContext context = getServletContext();
      final FSImage nnImage = NameNodeHttpServer.getFsImageFromContext(context);
      final Configuration conf = (Configuration) getServletContext()
          .getAttribute(JspHelper.CURRENT_CONF);
      final PutImageParams parsedParams = new PutImageParams(request, response,
          conf);
      final NameNodeMetrics metrics = NameNode.getNameNodeMetrics();

      validateRequest(context, conf, request, response, nnImage,
          parsedParams.getStorageInfoString());

      UserGroupInformation.getCurrentUser().doAs(
          new PrivilegedExceptionAction<Void>() {

            @Override
            public Void run() throws Exception {

              final long txid = parsedParams.getTxId();

              final NameNodeFile nnf = parsedParams.getNameNodeFile();

              if (!currentlyDownloadingCheckpoints.add(txid)) {
                response.sendError(HttpServletResponse.SC_CONFLICT,
                    "Another checkpointer is already in the process of uploading a"
                        + " checkpoint made at transaction ID " + txid);
                return null;
              }
              try {
                if (nnImage.getStorage().findImageFile(nnf, txid) != null) {
                  response.sendError(HttpServletResponse.SC_CONFLICT,
                      "Another checkpointer already uploaded an checkpoint "
                          + "for txid " + txid);
                  return null;
                }

                InputStream stream = request.getInputStream();
                try {
                  long start = monotonicNow();
                  MD5Hash downloadImageDigest = TransferFsImage
                      .handleUploadImageRequest(request, txid,
                          nnImage.getStorage(), stream,
                          parsedParams.getFileSize(), getThrottler(conf));
                  nnImage.saveDigestAndRenameCheckpointImage(nnf, txid,
                      downloadImageDigest);
                  // Metrics non-null only when used inside name node
                  if (metrics != null) {
                    long elapsed = monotonicNow() - start;
                    metrics.addPutImage(elapsed);
                  }
                  // Now that we have a new checkpoint, we might be able to
                  // remove some old ones.
                  nnImage.purgeOldStorage(nnf);
                } finally {
                  stream.close();
                }
              } finally {
                currentlyDownloadingCheckpoints.remove(txid);
              }
              return null;
            }

          });
    } catch (Throwable t) {
      String errMsg = "PutImage failed. " + StringUtils.stringifyException(t);
      response.sendError(HttpServletResponse.SC_GONE, errMsg);
      throw new IOException(errMsg);
    }
  }

  /*
   * Params required to handle put image request
   */
  static class PutImageParams {
    private long txId = -1;
    private String storageInfoString = null;
    private long fileSize = 0L;
    private NameNodeFile nnf;

    public PutImageParams(HttpServletRequest request,
        HttpServletResponse response, Configuration conf) throws IOException {
      txId = ServletUtil.parseLongParam(request, TXID_PARAM);
      storageInfoString = ServletUtil.getParameter(request, STORAGEINFO_PARAM);
      fileSize = ServletUtil.parseLongParam(request,
          TransferFsImage.FILE_LENGTH);
      String imageType = ServletUtil.getParameter(request, IMAGE_FILE_TYPE);
      nnf = imageType == null ? NameNodeFile.IMAGE : NameNodeFile
          .valueOf(imageType);
      if (fileSize == 0 || txId == -1 || storageInfoString == null
          || storageInfoString.isEmpty()) {
        throw new IOException("Illegal parameters to TransferFsImage");
      }
    }

    public long getTxId() {
      return txId;
    }

    public String getStorageInfoString() {
      return storageInfoString;
    }

    public long getFileSize() {
      return fileSize;
    }

    public NameNodeFile getNameNodeFile() {
      return nnf;
    }
  }
}
