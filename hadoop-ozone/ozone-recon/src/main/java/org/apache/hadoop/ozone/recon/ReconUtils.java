/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.hdds.server.ServerUtils.getDirectoryFromConfig;
import static org.apache.hadoop.hdds.server.ServerUtils.getOzoneMetaDirPath;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon Utility class.
 */
public final class ReconUtils {

  private final static int WRITE_BUFFER = 1048576; //1MB

  private ReconUtils() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(
      ReconUtils.class);

  /**
   * Get configured Recon DB directory value based on config. If not present,
   * fallback to ozone.metadata.dirs
   *
   * @param conf         configuration bag
   * @param dirConfigKey key to check
   * @return Return File based on configured or fallback value.
   */
  public static File getReconDbDir(Configuration conf, String dirConfigKey) {

    File metadataDir = getDirectoryFromConfig(conf, dirConfigKey,
        "Recon");
    if (metadataDir != null) {
      return metadataDir;
    }

    LOG.warn("{} is not configured. We recommend adding this setting. " +
            "Falling back to {} instead.",
        dirConfigKey, HddsConfigKeys.OZONE_METADATA_DIRS);
    return getOzoneMetaDirPath(conf);
  }

  /**
   * Untar DB snapshot tar file to recon OM snapshot directory.
   *
   * @param tarFile  source tar file
   * @param destPath destination path to untar to.
   * @throws IOException ioException
   */
  public static void untarCheckpointFile(File tarFile, Path destPath)
      throws IOException {

    FileInputStream fileInputStream = null;
    BufferedInputStream buffIn = null;
    GzipCompressorInputStream gzIn = null;
    try {
      fileInputStream = new FileInputStream(tarFile);
      buffIn = new BufferedInputStream(fileInputStream);
      gzIn = new GzipCompressorInputStream(buffIn);

      //Create Destination directory if it does not exist.
      if (!destPath.toFile().exists()) {
        boolean success = destPath.toFile().mkdirs();
        if (!success) {
          throw new IOException("Unable to create Destination directory.");
        }
      }

      try (TarArchiveInputStream tarInStream =
               new TarArchiveInputStream(gzIn)) {
        TarArchiveEntry entry = null;

        while ((entry = (TarArchiveEntry) tarInStream.getNextEntry()) != null) {
          //If directory, create a directory.
          if (entry.isDirectory()) {
            File f = new File(Paths.get(destPath.toString(),
                entry.getName()).toString());
            boolean success = f.mkdirs();
            if (!success) {
              LOG.error("Unable to create directory found in tar.");
            }
          } else {
            //Write contents of file in archive to a new file.
            int count;
            byte[] data = new byte[WRITE_BUFFER];

            FileOutputStream fos = new FileOutputStream(
                Paths.get(destPath.toString(), entry.getName()).toString());
            try (BufferedOutputStream dest =
                     new BufferedOutputStream(fos, WRITE_BUFFER)) {
              while ((count =
                  tarInStream.read(data, 0, WRITE_BUFFER)) != -1) {
                dest.write(data, 0, count);
              }
            }
          }
        }
      }
    } finally {
      IOUtils.closeStream(gzIn);
      IOUtils.closeStream(buffIn);
      IOUtils.closeStream(fileInputStream);
    }
  }

  /**
   * Make HTTP GET call on the URL and return inputstream to the response.
   * @param httpClient HttpClient to use.
   * @param url url to call
   * @return Inputstream to the response of the HTTP call.
   * @throws IOException While reading the response.
   */
  public static InputStream makeHttpCall(CloseableHttpClient httpClient,
                                         String url)
      throws IOException {

    HttpGet httpGet = new HttpGet(url);
    HttpResponse response = httpClient.execute(httpGet);
    int errorCode = response.getStatusLine().getStatusCode();
    HttpEntity entity = response.getEntity();

    if ((errorCode == HTTP_OK) || (errorCode == HTTP_CREATED)) {
      return entity.getContent();
    }

    if (entity != null) {
      throw new IOException("Unexpected exception when trying to reach Ozone " +
          "Manager, " + EntityUtils.toString(entity));
    } else {
      throw new IOException("Unexpected null in http payload," +
          " while processing request");
    }
  }

}
