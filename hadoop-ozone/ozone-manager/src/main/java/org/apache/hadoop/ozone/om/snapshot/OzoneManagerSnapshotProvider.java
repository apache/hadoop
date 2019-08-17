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

package org.apache.hadoop.ozone.om.snapshot;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.ozone.om.OMNodeDetails;
import org.apache.hadoop.utils.db.DBCheckpoint;
import org.apache.hadoop.utils.db.RocksDBCheckpoint;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.OzoneConsts.OM_RATIS_SNAPSHOT_INDEX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_PROVIDER_CONNECTION_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_PROVIDER_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_PROVIDER_REQUEST_TIMEOUT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_PROVIDER_CONNECTION_TIMEOUT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_PROVIDER_SOCKET_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_PROVIDER_SOCKET_TIMEOUT_KEY;

/**
 * OzoneManagerSnapshotProvider downloads the latest checkpoint from the
 * leader OM and loads the checkpoint into State Machine.
 */
public class OzoneManagerSnapshotProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerSnapshotProvider.class);

  private final File omSnapshotDir;
  private Map<String, OMNodeDetails> peerNodesMap;
  private final HttpConfig.Policy httpPolicy;
  private final RequestConfig httpRequestConfig;
  private CloseableHttpClient httpClient;

  private static final String OM_SNAPSHOT_DB = "om.snapshot.db";

  public OzoneManagerSnapshotProvider(Configuration conf,
      File omRatisSnapshotDir, List<OMNodeDetails> peerNodes) {

    LOG.info("Initializing OM Snapshot Provider");
    this.omSnapshotDir = omRatisSnapshotDir;

    this.peerNodesMap = new HashMap<>();
    for (OMNodeDetails peerNode : peerNodes) {
      this.peerNodesMap.put(peerNode.getOMNodeId(), peerNode);
    }

    this.httpPolicy = DFSUtil.getHttpPolicy(conf);
    this.httpRequestConfig = getHttpRequestConfig(conf);
  }

  private RequestConfig getHttpRequestConfig(Configuration conf) {
    TimeUnit socketTimeoutUnit =
        OZONE_OM_SNAPSHOT_PROVIDER_SOCKET_TIMEOUT_DEFAULT.getUnit();
    int socketTimeoutMS = (int) conf.getTimeDuration(
        OZONE_OM_SNAPSHOT_PROVIDER_SOCKET_TIMEOUT_KEY,
        OZONE_OM_SNAPSHOT_PROVIDER_SOCKET_TIMEOUT_DEFAULT.getDuration(),
        socketTimeoutUnit);

    TimeUnit connectionTimeoutUnit =
        OZONE_OM_SNAPSHOT_PROVIDER_CONNECTION_TIMEOUT_DEFAULT.getUnit();
    int connectionTimeoutMS = (int) conf.getTimeDuration(
        OZONE_OM_SNAPSHOT_PROVIDER_CONNECTION_TIMEOUT_KEY,
        OZONE_OM_SNAPSHOT_PROVIDER_CONNECTION_TIMEOUT_DEFAULT.getDuration(),
        connectionTimeoutUnit);

    TimeUnit requestTimeoutUnit =
        OZONE_OM_SNAPSHOT_PROVIDER_REQUEST_TIMEOUT_DEFAULT.getUnit();
    int requestTimeoutMS = (int) conf.getTimeDuration(
        OZONE_OM_SNAPSHOT_PROVIDER_REQUEST_TIMEOUT_KEY,
        OZONE_OM_SNAPSHOT_PROVIDER_REQUEST_TIMEOUT_DEFAULT.getDuration(),
        requestTimeoutUnit);

    RequestConfig requestConfig = RequestConfig.custom()
        .setSocketTimeout(socketTimeoutMS)
        .setConnectTimeout(connectionTimeoutMS)
        .setConnectionRequestTimeout(requestTimeoutMS)
        .build();

    return requestConfig;
  }

  /**
   * Create and return http client object.
   */
  private HttpClient getHttpClient() {
    if (httpClient == null) {
      httpClient = HttpClientBuilder
          .create()
          .setDefaultRequestConfig(httpRequestConfig)
          .build();
    }
    return httpClient;
  }

  /**
   * Close http client object.
   */
  private void closeHttpClient() throws IOException {
    if (httpClient != null) {
      httpClient.close();
      httpClient = null;
    }
  }

  /**
   * Download the latest checkpoint from OM Leader via HTTP.
   * @param leaderOMNodeID leader OM Node ID.
   * @return the DB checkpoint (including the ratis snapshot index)
   */
  public DBCheckpoint getOzoneManagerDBSnapshot(String leaderOMNodeID)
      throws IOException {
    String snapshotFileName = OM_SNAPSHOT_DB + "_" + System.currentTimeMillis();
    File targetFile = new File(omSnapshotDir, snapshotFileName + ".tar.gz");

    String omCheckpointUrl = peerNodesMap.get(leaderOMNodeID)
        .getOMDBCheckpointEnpointUrl(httpPolicy);

    LOG.info("Downloading latest checkpoint from Leader OM {}. Checkpoint " +
        "URL: {}", leaderOMNodeID, omCheckpointUrl);

    try {
      HttpGet httpGet = new HttpGet(omCheckpointUrl);
      HttpResponse response = getHttpClient().execute(httpGet);
      int errorCode = response.getStatusLine().getStatusCode();
      HttpEntity entity = response.getEntity();

      if ((errorCode == HTTP_OK) || (errorCode == HTTP_CREATED)) {

        Header header = response.getFirstHeader(OM_RATIS_SNAPSHOT_INDEX);
        if (header == null) {
          throw new IOException("The HTTP response header " +
              OM_RATIS_SNAPSHOT_INDEX + " is missing.");
        }

        long snapshotIndex = Long.parseLong(header.getValue());

        try (InputStream inputStream = entity.getContent()) {
          FileUtils.copyInputStreamToFile(inputStream, targetFile);
        }

        // Untar the checkpoint file.
        Path untarredDbDir = Paths.get(omSnapshotDir.getAbsolutePath(),
            snapshotFileName);
        FileUtil.unTar(targetFile, untarredDbDir.toFile());
        FileUtils.deleteQuietly(targetFile);

        LOG.info("Sucessfully downloaded latest checkpoint with snapshot " +
            "index {} from leader OM: {}",  snapshotIndex, leaderOMNodeID);

        RocksDBCheckpoint omCheckpoint = new RocksDBCheckpoint(untarredDbDir);
        omCheckpoint.setRatisSnapshotIndex(snapshotIndex);
        return omCheckpoint;
      }

      if (entity != null) {
        throw new IOException("Unexpected exception when trying to reach " +
            "OM to download latest checkpoint. Checkpoint URL: " +
            omCheckpointUrl + ". Entity: " + EntityUtils.toString(entity));
      } else {
        throw new IOException("Unexpected null in http payload, while " +
            "processing request to OM to download latest checkpoint. " +
            "Checkpoint Url: " + omCheckpointUrl);
      }
    } finally {
      closeHttpClient();
    }
  }
}
