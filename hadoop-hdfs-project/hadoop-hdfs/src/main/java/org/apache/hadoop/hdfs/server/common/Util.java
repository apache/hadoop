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
package org.apache.hadoop.hdfs.server.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.server.namenode.ImageServlet;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;


@InterfaceAudience.Private
public final class Util {
  private final static Logger LOG =
      LoggerFactory.getLogger(Util.class.getName());

  public final static String FILE_LENGTH = "File-Length";
  public final static String CONTENT_LENGTH = "Content-Length";
  public final static String MD5_HEADER = "X-MD5-Digest";
  public final static String CONTENT_TYPE = "Content-Type";
  public final static String CONTENT_TRANSFER_ENCODING = "Content-Transfer-Encoding";

  public final static int IO_FILE_BUFFER_SIZE;
  private static final boolean isSpnegoEnabled;
  public static final URLConnectionFactory connectionFactory;

  static {
    Configuration conf = new Configuration();
    connectionFactory = URLConnectionFactory
        .newDefaultURLConnectionFactory(conf);
    isSpnegoEnabled = UserGroupInformation.isSecurityEnabled();
    IO_FILE_BUFFER_SIZE = DFSUtilClient.getIoFileBufferSize(conf);
  }

  /**
   * Interprets the passed string as a URI. In case of error it 
   * assumes the specified string is a file.
   *
   * @param s the string to interpret
   * @return the resulting URI
   */
  static URI stringAsURI(String s) throws IOException {
    URI u = null;
    // try to make a URI
    try {
      u = new URI(s);
    } catch (URISyntaxException e){
      LOG.error("Syntax error in URI " + s
          + ". Please check hdfs configuration.", e);
    }

    // if URI is null or scheme is undefined, then assume it's file://
    if(u == null || u.getScheme() == null){
      LOG.info("Assuming 'file' scheme for path " + s + " in configuration.");
      u = fileAsURI(new File(s));
    }
    return u;
  }

  /**
   * Converts the passed File to a URI. This method trims the trailing slash if
   * one is appended because the underlying file is in fact a directory that
   * exists.
   * 
   * @param f the file to convert
   * @return the resulting URI
   */
  public static URI fileAsURI(File f) throws IOException {
    URI u = f.getCanonicalFile().toURI();
    
    // trim the trailing slash, if it's present
    if (u.getPath().endsWith("/")) {
      String uriAsString = u.toString();
      try {
        u = new URI(uriAsString.substring(0, uriAsString.length() - 1));
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
    }
    
    return u;
  }

  /**
   * Converts a collection of strings into a collection of URIs.
   * @param names collection of strings to convert to URIs
   * @return collection of URIs
   */
  public static List<URI> stringCollectionAsURIs(
                                  Collection<String> names) {
    List<URI> uris = new ArrayList<>(names.size());
    for(String name : names) {
      try {
        uris.add(stringAsURI(name));
      } catch (IOException e) {
        LOG.error("Error while processing URI: " + name, e);
      }
    }
    return uris;
  }

  /**
   * Downloads the files at the specified url location into destination
   * storage.
   */
  public static MD5Hash doGetUrl(URL url, List<File> localPaths,
      Storage dstStorage, boolean getChecksum, int timeout,
      DataTransferThrottler throttler) throws IOException {
    HttpURLConnection connection;
    try {
      connection = (HttpURLConnection)
          connectionFactory.openConnection(url, isSpnegoEnabled);
    } catch (AuthenticationException e) {
      throw new IOException(e);
    }

    setTimeout(connection, timeout);

    if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new HttpGetFailedException("Image transfer servlet at " + url +
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
        throttler);
  }

  /**
   * Receives file at the url location from the input stream and puts them in
   * the specified destination storage location.
   */
  public static MD5Hash receiveFile(String url, List<File> localPaths,
      Storage dstStorage, boolean getChecksum, long advertisedSize,
      MD5Hash advertisedDigest, String fsImageName, InputStream stream,
      DataTransferThrottler throttler) throws
      IOException {
    long startTime = Time.monotonicNow();
    Map<FileOutputStream, File> streamPathMap = new HashMap<>();
    StringBuilder xferStats = new StringBuilder();
    double xferCombined = 0;
    if (localPaths != null) {
      // If the local paths refer to directories, use the server-provided header
      // as the filename within that directory
      List<File> newLocalPaths = new ArrayList<>();
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
    int num = 1;

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
          String.format(" The file download took %.2fs at %.2f KB/s.",
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
            " is not of the advertised size " + advertisedSize +
            ". Fsimage name: " + fsImageName + " lastReceived: " + num);
      }
    }
    xferStats.insert(0, String.format("Combined time for file download and" +
        " fsync to all disks took %.2fs.", xferCombined));
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

  /**
   * Sets a timeout value in millisecods for the Http connection.
   * @param connection the Http connection for which timeout needs to be set
   * @param timeout value to be set as timeout in milliseconds
   */
  public static void setTimeout(HttpURLConnection connection, int timeout) {
    if (timeout > 0) {
      connection.setConnectTimeout(timeout);
      connection.setReadTimeout(timeout);
    }
  }

  private static MD5Hash parseMD5Header(HttpURLConnection connection) {
    String header = connection.getHeaderField(MD5_HEADER);
    return (header != null) ? new MD5Hash(header) : null;
  }

  public static List<InetSocketAddress> getAddressesList(URI uri)
      throws IOException{
    String authority = uri.getAuthority();
    Preconditions.checkArgument(authority != null && !authority.isEmpty(),
        "URI has no authority: " + uri);

    String[] parts = StringUtils.split(authority, ';');
    for (int i = 0; i < parts.length; i++) {
      parts[i] = parts[i].trim();
    }

    List<InetSocketAddress> addrs = Lists.newArrayList();
    for (String addr : parts) {
      InetSocketAddress isa = NetUtils.createSocketAddr(
          addr, DFSConfigKeys.DFS_JOURNALNODE_RPC_PORT_DEFAULT);
      if (isa.isUnresolved()) {
        throw new UnknownHostException(addr);
      }
      addrs.add(isa);
    }
    return addrs;
  }

  public static List<InetSocketAddress> getLoggerAddresses(URI uri,
      Set<InetSocketAddress> addrsToExclude) throws IOException {
    List<InetSocketAddress> addrsList = getAddressesList(uri);
    addrsList.removeAll(addrsToExclude);
    return addrsList;
  }

  public static boolean isDiskStatsEnabled(int fileIOSamplingPercentage) {
    final boolean isEnabled;
    if (fileIOSamplingPercentage <= 0) {
      LOG.info(DFSConfigKeys
          .DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY + " set to "
          + fileIOSamplingPercentage + ". Disabling file IO profiling");
      isEnabled = false;
    } else {
      LOG.info(DFSConfigKeys
          .DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY + " set to "
          + fileIOSamplingPercentage + ". Enabling file IO profiling");
      isEnabled = true;
    }

    return isEnabled;
  }
}
