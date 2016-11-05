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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.Time;
import org.apache.http.client.utils.URIBuilder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.eclipse.jetty.io.EofException;

/**
 * This class provides fetching a specified file from the NameNode.
 */
@InterfaceAudience.Private
public class TransferFsImage {

  public enum TransferResult{
    SUCCESS(HttpServletResponse.SC_OK, false),
    AUTHENTICATION_FAILURE(HttpServletResponse.SC_FORBIDDEN, true),
    NOT_ACTIVE_NAMENODE_FAILURE(HttpServletResponse.SC_EXPECTATION_FAILED, false),
    OLD_TRANSACTION_ID_FAILURE(HttpServletResponse.SC_CONFLICT, false),
    UNEXPECTED_FAILURE(-1, true);

    private final int response;
    private final boolean shouldReThrowException;

    private TransferResult(int response, boolean rethrow) {
      this.response = response;
      this.shouldReThrowException = rethrow;
    }

    public static TransferResult getResultForCode(int code){
      TransferResult ret = UNEXPECTED_FAILURE;
      for(TransferResult result:TransferResult.values()){
        if(result.response == code){
          return result;
        }
      }
      return ret;
    }
  }

  public final static String CONTENT_LENGTH = "Content-Length";
  public final static String FILE_LENGTH = "File-Length";
  public final static String MD5_HEADER = "X-MD5-Digest";

  private final static String CONTENT_TYPE = "Content-Type";
  private final static String CONTENT_TRANSFER_ENCODING = "Content-Transfer-Encoding";
  private final static int IO_FILE_BUFFER_SIZE;

  @VisibleForTesting
  static int timeout = 0;
  private static final URLConnectionFactory connectionFactory;
  private static final boolean isSpnegoEnabled;

  static {
    Configuration conf = new Configuration();
    connectionFactory = URLConnectionFactory
        .newDefaultURLConnectionFactory(conf);
    isSpnegoEnabled = UserGroupInformation.isSecurityEnabled();
    IO_FILE_BUFFER_SIZE = DFSUtilClient.getIoFileBufferSize(conf);
  }

  private static final Log LOG = LogFactory.getLog(TransferFsImage.class);
  
  public static void downloadMostRecentImageToDirectory(URL infoServer,
      File dir) throws IOException {
    String fileId = ImageServlet.getParamStringForMostRecentImage();
    getFileClient(infoServer, fileId, Lists.newArrayList(dir),
        null, false);
  }

  public static MD5Hash downloadImageToStorage(URL fsName, long imageTxId,
      Storage dstStorage, boolean needDigest, boolean isBootstrapStandby)
      throws IOException {
    String fileid = ImageServlet.getParamStringForImage(null,
        imageTxId, dstStorage, isBootstrapStandby);
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

  static MD5Hash handleUploadImageRequest(HttpServletRequest request,
      long imageTxId, Storage dstStorage, InputStream stream,
      long advertisedSize, DataTransferThrottler throttler) throws IOException {

    String fileName = NNStorage.getCheckpointImageFileName(imageTxId);

    List<File> dstFiles = dstStorage.getFiles(NameNodeDirType.IMAGE, fileName);
    if (dstFiles.isEmpty()) {
      throw new IOException("No targets in destination storage!");
    }

    MD5Hash advertisedDigest = parseMD5Header(request);
    MD5Hash hash = receiveFile(fileName, dstFiles, dstStorage, true,
        advertisedSize, advertisedDigest, fileName, stream, throttler);
    LOG.info("Downloaded file " + dstFiles.get(0).getName() + " size "
        + dstFiles.get(0).length() + " bytes.");
    return hash;
  }

  static void downloadEditsToStorage(URL fsName, RemoteEditLog log,
      NNStorage dstStorage) throws IOException {
    assert log.getStartTxId() > 0 && log.getEndTxId() > 0 :
      "bad log: " + log;
    String fileid = ImageServlet.getParamStringForLog(
        log, dstStorage);
    String finalFileName = NNStorage.getFinalizedEditsFileName(
        log.getStartTxId(), log.getEndTxId());

    List<File> finalFiles = dstStorage.getFiles(NameNodeDirType.EDITS,
        finalFileName);
    assert !finalFiles.isEmpty() : "No checkpoint targets.";
    
    for (File f : finalFiles) {
      if (f.exists() && FileUtil.canRead(f)) {
        LOG.info("Skipping download of remote edit log " +
            log + " since it already is stored locally at " + f);
        return;
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("Dest file: " + f);
      }
    }

    final long milliTime = Time.monotonicNow();
    String tmpFileName = NNStorage.getTemporaryEditsFileName(
        log.getStartTxId(), log.getEndTxId(), milliTime);
    List<File> tmpFiles = dstStorage.getFiles(NameNodeDirType.EDITS,
        tmpFileName);
    getFileClient(fsName, fileid, tmpFiles, dstStorage, false);
    LOG.info("Downloaded file " + tmpFiles.get(0).getName() + " size " +
        finalFiles.get(0).length() + " bytes.");

    CheckpointFaultInjector.getInstance().beforeEditsRename();

    for (StorageDirectory sd : dstStorage.dirIterable(NameNodeDirType.EDITS)) {
      File tmpFile = NNStorage.getTemporaryEditsFile(sd,
          log.getStartTxId(), log.getEndTxId(), milliTime);
      File finalizedFile = NNStorage.getFinalizedEditsFile(sd,
          log.getStartTxId(), log.getEndTxId());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Renaming " + tmpFile + " to " + finalizedFile);
      }
      boolean success = tmpFile.renameTo(finalizedFile);
      if (!success) {
        LOG.warn("Unable to rename edits file from " + tmpFile
            + " to " + finalizedFile);
      }
    }
  }
 
  /**
   * Requests that the NameNode download an image from this node.
   *
   * @param fsName the http address for the remote NN
   * @param conf Configuration
   * @param storage the storage directory to transfer the image from
   * @param nnf the NameNodeFile type of the image
   * @param txid the transaction ID of the image to be uploaded
   * @throws IOException if there is an I/O error
   */
  public static TransferResult uploadImageFromStorage(URL fsName, Configuration conf,
      NNStorage storage, NameNodeFile nnf, long txid) throws IOException {
    return uploadImageFromStorage(fsName, conf, storage, nnf, txid, null);
  }

  /**
   * Requests that the NameNode download an image from this node.  Allows for
   * optional external cancelation.
   *
   * @param fsName the http address for the remote NN
   * @param conf Configuration
   * @param storage the storage directory to transfer the image from
   * @param nnf the NameNodeFile type of the image
   * @param txid the transaction ID of the image to be uploaded
   * @param canceler optional canceler to check for abort of upload
   * @throws IOException if there is an I/O error or cancellation
   */
  public static TransferResult uploadImageFromStorage(URL fsName, Configuration conf,
      NNStorage storage, NameNodeFile nnf, long txid, Canceler canceler)
      throws IOException {
    URL url = new URL(fsName, ImageServlet.PATH_SPEC);
    long startTime = Time.monotonicNow();
    try {
      uploadImage(url, conf, storage, nnf, txid, canceler);
    } catch (HttpPutFailedException e) {
      // translate the error code to a result, which is a bit more obvious in usage
      TransferResult result = TransferResult.getResultForCode(e.getResponseCode());
      if (result.shouldReThrowException) {
        throw e;
      }
      return result;
    }
    double xferSec = Math.max(
        ((float) (Time.monotonicNow() - startTime)) / 1000.0, 0.001);
    LOG.info("Uploaded image with txid " + txid + " to namenode at " + fsName
        + " in " + xferSec + " seconds");
    return TransferResult.SUCCESS;
  }

  /*
   * Uploads the imagefile using HTTP PUT method
   */
  private static void uploadImage(URL url, Configuration conf,
      NNStorage storage, NameNodeFile nnf, long txId, Canceler canceler)
      throws IOException {

    File imageFile = storage.findImageFile(nnf, txId);
    if (imageFile == null) {
      throw new IOException("Could not find image with txid " + txId);
    }

    HttpURLConnection connection = null;
    try {
      URIBuilder uriBuilder = new URIBuilder(url.toURI());

      // write all params for image upload request as query itself.
      // Request body contains the image to be uploaded.
      Map<String, String> params = ImageServlet.getParamsForPutImage(storage,
          txId, imageFile.length(), nnf);
      for (Entry<String, String> entry : params.entrySet()) {
        uriBuilder.addParameter(entry.getKey(), entry.getValue());
      }

      URL urlWithParams = uriBuilder.build().toURL();
      connection = (HttpURLConnection) connectionFactory.openConnection(
          urlWithParams, UserGroupInformation.isSecurityEnabled());
      // Set the request to PUT
      connection.setRequestMethod("PUT");
      connection.setDoOutput(true);

      
      int chunkSize = conf.getInt(
          DFSConfigKeys.DFS_IMAGE_TRANSFER_CHUNKSIZE_KEY,
          DFSConfigKeys.DFS_IMAGE_TRANSFER_CHUNKSIZE_DEFAULT);
      if (imageFile.length() > chunkSize) {
        // using chunked streaming mode to support upload of 2GB+ files and to
        // avoid internal buffering.
        // this mode should be used only if more than chunkSize data is present
        // to upload. otherwise upload may not happen sometimes.
        connection.setChunkedStreamingMode(chunkSize);
      }

      setTimeout(connection);

      // set headers for verification
      ImageServlet.setVerificationHeadersForPut(connection, imageFile);

      // Write the file to output stream.
      writeFileToPutRequest(conf, connection, imageFile, canceler);

      int responseCode = connection.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new HttpPutFailedException(String.format(
            "Image uploading failed, status: %d, url: %s, message: %s",
            responseCode, urlWithParams, connection.getResponseMessage()),
            responseCode);
      }
    } catch (AuthenticationException e) {
      throw new IOException(e);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private static void writeFileToPutRequest(Configuration conf,
      HttpURLConnection connection, File imageFile, Canceler canceler)
      throws FileNotFoundException, IOException {
    connection.setRequestProperty(CONTENT_TYPE, "application/octet-stream");
    connection.setRequestProperty(CONTENT_TRANSFER_ENCODING, "binary");
    OutputStream output = connection.getOutputStream();
    FileInputStream input = new FileInputStream(imageFile);
    try {
      copyFileToStream(output, imageFile, input,
          ImageServlet.getThrottler(conf), canceler);
    } finally {
      IOUtils.closeStream(input);
      IOUtils.closeStream(output);
    }
  }

  /**
   * A server-side method to respond to a getfile http request
   * Copies the contents of the local file into the output stream.
   */
  public static void copyFileToStream(OutputStream out, File localfile,
      FileInputStream infile, DataTransferThrottler throttler)
    throws IOException {
    copyFileToStream(out, localfile, infile, throttler, null);
  }

  private static void copyFileToStream(OutputStream out, File localfile,
      FileInputStream infile, DataTransferThrottler throttler,
      Canceler canceler) throws IOException {
    byte buf[] = new byte[IO_FILE_BUFFER_SIZE];
    try {
      CheckpointFaultInjector.getInstance()
          .aboutToSendFile(localfile);

      if (CheckpointFaultInjector.getInstance().
            shouldSendShortFile(localfile)) {
          // Test sending image shorter than localfile
          long len = localfile.length();
          buf = new byte[(int)Math.min(len/2, IO_FILE_BUFFER_SIZE)];
          // This will read at most half of the image
          // and the rest of the image will be sent over the wire
          infile.read(buf);
      }
      int num = 1;
      while (num > 0) {
        if (canceler != null && canceler.isCancelled()) {
          throw new SaveNamespaceCancelledException(
            canceler.getCancellationReason());
        }
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
          throttler.throttle(num, canceler);
        }
      }
    } catch (EofException e) {
      LOG.info("Connection closed by client");
      out = null; // so we don't close in the finally
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
  static MD5Hash getFileClient(URL infoServer,
      String queryString, List<File> localPaths,
      Storage dstStorage, boolean getChecksum) throws IOException {
    URL url = new URL(infoServer, ImageServlet.PATH_SPEC + "?" + queryString);
    LOG.info("Opening connection to " + url);
    return doGetUrl(url, localPaths, dstStorage, getChecksum);
  }
  
  public static MD5Hash doGetUrl(URL url, List<File> localPaths,
      Storage dstStorage, boolean getChecksum) throws IOException {
    HttpURLConnection connection;
    try {
      connection = (HttpURLConnection)
        connectionFactory.openConnection(url, isSpnegoEnabled);
    } catch (AuthenticationException e) {
      throw new IOException(e);
    }

    setTimeout(connection);

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
    MD5Hash advertisedDigest = parseMD5Header(connection);
    String fsImageName = connection
        .getHeaderField(ImageServlet.HADOOP_IMAGE_EDITS_HEADER);
    InputStream stream = connection.getInputStream();

    return receiveFile(url.toExternalForm(), localPaths, dstStorage,
        getChecksum, advertisedSize, advertisedDigest, fsImageName, stream,
        null);
  }

  private static void setTimeout(HttpURLConnection connection) {
    if (timeout <= 0) {
      Configuration conf = new HdfsConfiguration();
      timeout = conf.getInt(DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_KEY,
          DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT);
      LOG.info("Image Transfer timeout configured to " + timeout
          + " milliseconds");
    }

    if (timeout > 0) {
      connection.setConnectTimeout(timeout);
      connection.setReadTimeout(timeout);
    }
  }

  private static MD5Hash receiveFile(String url, List<File> localPaths,
      Storage dstStorage, boolean getChecksum, long advertisedSize,
      MD5Hash advertisedDigest, String fsImageName, InputStream stream,
      DataTransferThrottler throttler) throws IOException {
    long startTime = Time.monotonicNow();
    Map<FileOutputStream, File> streamPathMap = new HashMap<>();
    StringBuilder xferStats = new StringBuilder();
    double xferCombined = 0;
    if (localPaths != null) {
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
    

    long received = 0;
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
            FileOutputStream fos = new FileOutputStream(f);
            outputStreams.add(fos);
            streamPathMap.put(fos, f);
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
      byte[] buf = new byte[IO_FILE_BUFFER_SIZE];
      while (num > 0) {
        num = stream.read(buf);
        if (num > 0) {
          received += num;
          for (FileOutputStream fos : outputStreams) {
            fos.write(buf, 0, num);
          }
          if (throttler != null) {
            throttler.throttle(num);
          }
        }
      }
      finishedReceiving = true;
      double xferSec = Math.max(
                 ((float)(Time.monotonicNow() - startTime)) / 1000.0, 0.001);
      long xferKb = received / 1024;
      xferCombined += xferSec;
      xferStats.append(
          String.format(" The fsimage download took %.2fs at %.2f KB/s.",
              xferSec, xferKb / xferSec));
    } finally {
      stream.close();
      for (FileOutputStream fos : outputStreams) {
        long flushStartTime = Time.monotonicNow();
        fos.getChannel().force(true);
        fos.close();
        double writeSec = Math.max(((float)
               (flushStartTime - Time.monotonicNow())) / 1000.0, 0.001);
        xferCombined += writeSec;
        xferStats.append(String
                .format(" Synchronous (fsync) write to disk of " +
                 streamPathMap.get(fos).getAbsolutePath() +
                " took %.2fs.", writeSec));
      }

      // Something went wrong and did not finish reading.
      // Remove the temporary files.
      if (!finishedReceiving) {
        deleteTmpFiles(localPaths);
      }

      if (finishedReceiving && received != advertisedSize) {
        // only throw this exception if we think we read all of it on our end
        // -- otherwise a client-side IOException would be masked by this
        // exception that makes it look like a server-side problem!
        deleteTmpFiles(localPaths);
        throw new IOException("File " + url + " received length " + received +
                              " is not of the advertised size " +
                              advertisedSize);
      }
    }
    xferStats.insert(
        0, String.format(
            "Combined time for fsimage download and fsync " +
            "to all disks took %.2fs.", xferCombined));
    LOG.info(xferStats.toString());

    if (digester != null) {
      MD5Hash computedDigest = new MD5Hash(digester.digest());
      
      if (advertisedDigest != null &&
          !computedDigest.equals(advertisedDigest)) {
        deleteTmpFiles(localPaths);
        throw new IOException("File " + url + " computed digest " +
            computedDigest + " does not match advertised digest " + 
            advertisedDigest);
      }
      return computedDigest;
    } else {
      return null;
    }    
  }

  private static void deleteTmpFiles(List<File> files) {
    if (files == null) {
      return;
    }

    LOG.info("Deleting temporary files: " + files);
    for (File file : files) {
      if (!file.delete()) {
        LOG.warn("Deleting " + file + " has failed");
      }
    }
  }

  private static MD5Hash parseMD5Header(HttpURLConnection connection) {
    String header = connection.getHeaderField(MD5_HEADER);
    return (header != null) ? new MD5Hash(header) : null;
  }

  private static MD5Hash parseMD5Header(HttpServletRequest request) {
    String header = request.getHeader(MD5_HEADER);
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

  public static class HttpPutFailedException extends IOException {
    private static final long serialVersionUID = 1L;
    private final int responseCode;

    HttpPutFailedException(String msg, int responseCode) throws IOException {
      super(msg);
      this.responseCode = responseCode;
    }

    public int getResponseCode() {
      return responseCode;
    }
  }

}
