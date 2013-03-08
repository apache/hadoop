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

import java.io.*;
import java.net.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.lang.Math;

import javax.servlet.ServletOutputStream;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.MD5Hash;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;


/**
 * This class provides fetching a specified file from the NameNode.
 */
@InterfaceAudience.Private
public class TransferFsImage {
  
  public final static String CONTENT_LENGTH = "Content-Length";
  public final static String MD5_HEADER = "X-MD5-Digest";
  @VisibleForTesting
  static int timeout = 0;

  private static final Log LOG = LogFactory.getLog(TransferFsImage.class);
  
  public static void downloadMostRecentImageToDirectory(String fsName,
      File dir) throws IOException {
    String fileId = GetImageServlet.getParamStringForMostRecentImage();
    getFileClient(fsName, fileId, Lists.newArrayList(dir),
        null, false);
  }

  public static MD5Hash downloadImageToStorage(
      String fsName, long imageTxId, Storage dstStorage, boolean needDigest)
      throws IOException {
    String fileid = GetImageServlet.getParamStringForImage(
        imageTxId, dstStorage);
    String fileName = NNStorage.getCheckpointImageFileName(imageTxId);
    
    List<File> dstFiles = dstStorage.getFiles(
        NameNodeDirType.IMAGE, fileName);
    if (dstFiles.isEmpty()) {
      throw new IOException("No targets in destination storage!");
    }
    
    MD5Hash hash = getFileClient(fsName, fileid, dstFiles, dstStorage, needDigest);
    LOG.info("Downloaded file " + dstFiles.get(0).getName() + " size " +
        dstFiles.get(0).length() + " bytes.");
    return hash;
  }
  
  static void downloadEditsToStorage(String fsName, RemoteEditLog log,
      NNStorage dstStorage) throws IOException {
    assert log.getStartTxId() > 0 && log.getEndTxId() > 0 :
      "bad log: " + log;
    String fileid = GetImageServlet.getParamStringForLog(
        log, dstStorage);
    String fileName = NNStorage.getFinalizedEditsFileName(
        log.getStartTxId(), log.getEndTxId());

    List<File> dstFiles = dstStorage.getFiles(NameNodeDirType.EDITS, fileName);
    assert !dstFiles.isEmpty() : "No checkpoint targets.";
    
    for (File f : dstFiles) {
      if (f.exists() && f.canRead()) {
        LOG.info("Skipping download of remote edit log " +
            log + " since it already is stored locally at " + f);
        return;
      } else {
        LOG.debug("Dest file: " + f);
      }
    }

    getFileClient(fsName, fileid, dstFiles, dstStorage, false);
    LOG.info("Downloaded file " + dstFiles.get(0).getName() + " size " +
        dstFiles.get(0).length() + " bytes.");
  }
 
  /**
   * Requests that the NameNode download an image from this node.
   *
   * @param fsName the http address for the remote NN
   * @param imageListenAddress the host/port where the local node is running an
   *                           HTTPServer hosting GetImageServlet
   * @param storage the storage directory to transfer the image from
   * @param txid the transaction ID of the image to be uploaded
   */
  public static void uploadImageFromStorage(String fsName,
      InetSocketAddress imageListenAddress,
      Storage storage, long txid) throws IOException {
    
    String fileid = GetImageServlet.getParamStringToPutImage(
        txid, imageListenAddress, storage);
    // this doesn't directly upload an image, but rather asks the NN
    // to connect back to the 2NN to download the specified image.
    try {
      TransferFsImage.getFileClient(fsName, fileid, null, null, false);
    } catch (HttpGetFailedException e) {
      if (e.getResponseCode() == HttpServletResponse.SC_CONFLICT) {
        // this is OK - this means that a previous attempt to upload
        // this checkpoint succeeded even though we thought it failed.
        LOG.info("Image upload with txid " + txid + 
            " conflicted with a previous image upload to the " +
            "same NameNode. Continuing...", e);
        return;
      } else {
        throw e;
      }
    }
    LOG.info("Uploaded image with txid " + txid + " to namenode at " +
    		fsName);
  }

  
  /**
   * A server-side method to respond to a getfile http request
   * Copies the contents of the local file into the output stream.
   */
  public static void getFileServer(ServletResponse response, File localfile,
      FileInputStream infile,
      DataTransferThrottler throttler) 
    throws IOException {
    byte buf[] = new byte[HdfsConstants.IO_FILE_BUFFER_SIZE];
    ServletOutputStream out = null;
    try {
      CheckpointFaultInjector.getInstance()
          .aboutToSendFile(localfile);
      out = response.getOutputStream();

      if (CheckpointFaultInjector.getInstance().
            shouldSendShortFile(localfile)) {
          // Test sending image shorter than localfile
          long len = localfile.length();
          buf = new byte[(int)Math.min(len/2, HdfsConstants.IO_FILE_BUFFER_SIZE)];
          // This will read at most half of the image
          // and the rest of the image will be sent over the wire
          infile.read(buf);
      }
      int num = 1;
      while (num > 0) {
        num = infile.read(buf);
        if (num <= 0) {
          break;
        }
        if (CheckpointFaultInjector.getInstance()
              .shouldCorruptAByte(localfile)) {
          // Simulate a corrupted byte on the wire
          LOG.warn("SIMULATING A CORRUPT BYTE IN IMAGE TRANSFER!");
          buf[0]++;
        }
        
        out.write(buf, 0, num);
        if (throttler != null) {
          throttler.throttle(num);
        }
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  /**
   * Client-side Method to fetch file from a server
   * Copies the response from the URL to a list of local files.
   * @param dstStorage if an error occurs writing to one of the files,
   *                   this storage object will be notified. 
   * @Return a digest of the received file if getChecksum is true
   */
  static MD5Hash getFileClient(String nnHostPort,
      String queryString, List<File> localPaths,
      Storage dstStorage, boolean getChecksum) throws IOException {

    String str = HttpConfig.getSchemePrefix() + nnHostPort + "/getimage?" +
        queryString;
    LOG.info("Opening connection to " + str);
    //
    // open connection to remote server
    //
    URL url = new URL(str);
    return doGetUrl(url, localPaths, dstStorage, getChecksum);
  }
  
  public static MD5Hash doGetUrl(URL url, List<File> localPaths,
      Storage dstStorage, boolean getChecksum) throws IOException {
    long startTime = Time.monotonicNow();

    HttpURLConnection connection = (HttpURLConnection)
      SecurityUtil.openSecureHttpConnection(url);

    if (timeout <= 0) {
      Configuration conf = new HdfsConfiguration();
      timeout = conf.getInt(DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_KEY,
          DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT);
    }

    if (timeout > 0) {
      connection.setConnectTimeout(timeout);
      connection.setReadTimeout(timeout);
    }

    if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new HttpGetFailedException(
          "Image transfer servlet at " + url +
          " failed with status code " + connection.getResponseCode() +
          "\nResponse message:\n" + connection.getResponseMessage(),
          connection);
    }
    
    long advertisedSize;
    String contentLength = connection.getHeaderField(CONTENT_LENGTH);
    if (contentLength != null) {
      advertisedSize = Long.parseLong(contentLength);
    } else {
      throw new IOException(CONTENT_LENGTH + " header is not provided " +
                            "by the namenode when trying to fetch " + url);
    }
    
    if (localPaths != null) {
      String fsImageName = connection.getHeaderField(
          GetImageServlet.HADOOP_IMAGE_EDITS_HEADER);
      // If the local paths refer to directories, use the server-provided header
      // as the filename within that directory
      List<File> newLocalPaths = new ArrayList<File>();
      for (File localPath : localPaths) {
        if (localPath.isDirectory()) {
          if (fsImageName == null) {
            throw new IOException("No filename header provided by server");
          }
          newLocalPaths.add(new File(localPath, fsImageName));
        } else {
          newLocalPaths.add(localPath);
        }
      }
      localPaths = newLocalPaths;
    }
    
    MD5Hash advertisedDigest = parseMD5Header(connection);

    long received = 0;
    InputStream stream = connection.getInputStream();
    MessageDigest digester = null;
    if (getChecksum) {
      digester = MD5Hash.getDigester();
      stream = new DigestInputStream(stream, digester);
    }
    boolean finishedReceiving = false;

    List<FileOutputStream> outputStreams = Lists.newArrayList();

    try {
      if (localPaths != null) {
        for (File f : localPaths) {
          try {
            if (f.exists()) {
              LOG.warn("Overwriting existing file " + f
                  + " with file downloaded from " + url);
            }
            outputStreams.add(new FileOutputStream(f));
          } catch (IOException ioe) {
            LOG.warn("Unable to download file " + f, ioe);
            // This will be null if we're downloading the fsimage to a file
            // outside of an NNStorage directory.
            if (dstStorage != null &&
                (dstStorage instanceof StorageErrorReporter)) {
              ((StorageErrorReporter)dstStorage).reportErrorOnFile(f);
            }
          }
        }
        
        if (outputStreams.isEmpty()) {
          throw new IOException(
              "Unable to download to any storage directory");
        }
      }
      
      int num = 1;
      byte[] buf = new byte[HdfsConstants.IO_FILE_BUFFER_SIZE];
      while (num > 0) {
        num = stream.read(buf);
        if (num > 0) {
          received += num;
          for (FileOutputStream fos : outputStreams) {
            fos.write(buf, 0, num);
          }
        }
      }
      finishedReceiving = true;
    } finally {
      stream.close();
      for (FileOutputStream fos : outputStreams) {
        fos.getChannel().force(true);
        fos.close();
      }
      if (finishedReceiving && received != advertisedSize) {
        // only throw this exception if we think we read all of it on our end
        // -- otherwise a client-side IOException would be masked by this
        // exception that makes it look like a server-side problem!
        throw new IOException("File " + url + " received length " + received +
                              " is not of the advertised size " +
                              advertisedSize);
      }
    }
    double xferSec = Math.max(
        ((float)(Time.monotonicNow() - startTime)) / 1000.0, 0.001);
    long xferKb = received / 1024;
    LOG.info(String.format("Transfer took %.2fs at %.2f KB/s",
        xferSec, xferKb / xferSec));

    if (digester != null) {
      MD5Hash computedDigest = new MD5Hash(digester.digest());
      
      if (advertisedDigest != null &&
          !computedDigest.equals(advertisedDigest)) {
        throw new IOException("File " + url + " computed digest " +
            computedDigest + " does not match advertised digest " + 
            advertisedDigest);
      }
      return computedDigest;
    } else {
      return null;
    }    
  }

  private static MD5Hash parseMD5Header(HttpURLConnection connection) {
    String header = connection.getHeaderField(MD5_HEADER);
    return (header != null) ? new MD5Hash(header) : null;
  }
  
  public static class HttpGetFailedException extends IOException {
    private static final long serialVersionUID = 1L;
    private final int responseCode;

    HttpGetFailedException(String msg, HttpURLConnection connection) throws IOException {
      super(msg);
      this.responseCode = connection.getResponseCode();
    }
    
    public int getResponseCode() {
      return responseCode;
    }
  }

}
