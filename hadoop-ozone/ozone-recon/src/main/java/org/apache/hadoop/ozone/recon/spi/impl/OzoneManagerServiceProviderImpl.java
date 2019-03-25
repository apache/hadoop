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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OM_SNAPSHOT_DB;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_CONNECTION_REQUEST_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_CONNECTION_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_CONNECTION_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_FLUSH_PARAM;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_SOCKET_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_SOCKET_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconUtils.getReconDbDir;
import static org.apache.hadoop.ozone.recon.ReconUtils.makeHttpCall;
import static org.apache.hadoop.ozone.recon.ReconUtils.untarCheckpointFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.apache.hadoop.utils.db.DBCheckpoint;
import org.apache.hadoop.utils.db.RocksDBCheckpoint;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Implementation of the OzoneManager Service provider.
 */
@Singleton
public class OzoneManagerServiceProviderImpl
    implements OzoneManagerServiceProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerServiceProviderImpl.class);

  private final String dbCheckpointEndPoint = "/dbCheckpoint";
  private final CloseableHttpClient httpClient;
  private File omSnapshotDBParentDir = null;
  private String omDBSnapshotUrl;

  @Inject
  private OzoneConfiguration configuration;

  @Inject
  private ReconOMMetadataManager omMetadataManager;

  @Inject
  public OzoneManagerServiceProviderImpl(OzoneConfiguration configuration) {

    String ozoneManagerHttpAddress = configuration.get(OMConfigKeys
        .OZONE_OM_HTTP_ADDRESS_KEY);

    String ozoneManagerHttpsAddress = configuration.get(OMConfigKeys
        .OZONE_OM_HTTPS_ADDRESS_KEY);

    omSnapshotDBParentDir = getReconDbDir(configuration,
        OZONE_RECON_OM_SNAPSHOT_DB_DIR);

    boolean ozoneSecurityEnabled = configuration.getBoolean(
        OZONE_SECURITY_ENABLED_KEY, false);

    int socketTimeout = (int) configuration.getTimeDuration(
        RECON_OM_SOCKET_TIMEOUT, RECON_OM_SOCKET_TIMEOUT_DEFAULT,
            TimeUnit.MILLISECONDS);
    int connectionTimeout = (int) configuration.getTimeDuration(
        RECON_OM_CONNECTION_TIMEOUT,
        RECON_OM_CONNECTION_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    int connectionRequestTimeout = (int)configuration.getTimeDuration(
        RECON_OM_CONNECTION_REQUEST_TIMEOUT,
        RECON_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);

    RequestConfig config = RequestConfig.custom()
        .setConnectTimeout(socketTimeout)
        .setConnectionRequestTimeout(connectionTimeout)
        .setSocketTimeout(connectionRequestTimeout).build();

    httpClient = HttpClientBuilder
        .create()
        .setDefaultRequestConfig(config)
        .build();

    omDBSnapshotUrl = "http://" + ozoneManagerHttpAddress +
        dbCheckpointEndPoint;

    if (ozoneSecurityEnabled) {
      omDBSnapshotUrl = "https://" + ozoneManagerHttpsAddress +
          dbCheckpointEndPoint;
    }

    boolean flushParam = configuration.getBoolean(
        RECON_OM_SNAPSHOT_TASK_FLUSH_PARAM, false);

    if (flushParam) {
      omDBSnapshotUrl += "?" + OZONE_DB_CHECKPOINT_REQUEST_FLUSH + "=true";
    }

  }

  @Override
  public void init() throws IOException {
    updateReconOmDBWithNewSnapshot();
  }

  @Override
  public void updateReconOmDBWithNewSnapshot() throws IOException {
    //Obtain the current DB snapshot from OM and
    //update the in house OM metadata managed DB instance.
    DBCheckpoint dbSnapshot = getOzoneManagerDBSnapshot();
    if (dbSnapshot != null && dbSnapshot.getCheckpointLocation() != null) {
      try {
        omMetadataManager.updateOmDB(dbSnapshot.getCheckpointLocation()
            .toFile());
      } catch (IOException e) {
        LOG.error("Unable to refresh Recon OM DB Snapshot. ", e);
      }
    } else {
      LOG.error("Null snapshot location got from OM.");
    }
  }

  @Override
  public OMMetadataManager getOMMetadataManagerInstance() {
    return omMetadataManager;
  }

  /**
   * Method to obtain current OM DB Snapshot.
   * @return DBCheckpoint instance.
   */
  @VisibleForTesting
  protected DBCheckpoint getOzoneManagerDBSnapshot() {
    String snapshotFileName = RECON_OM_SNAPSHOT_DB + "_" + System
        .currentTimeMillis();
    File targetFile = new File(omSnapshotDBParentDir, snapshotFileName +
        ".tar.gz");
    try {
      try (InputStream inputStream = makeHttpCall(httpClient,
          omDBSnapshotUrl)) {
        FileUtils.copyInputStreamToFile(inputStream, targetFile);
      }

      //Untar the checkpoint file.
      Path untarredDbDir = Paths.get(omSnapshotDBParentDir.getAbsolutePath(),
          snapshotFileName);
      untarCheckpointFile(targetFile, untarredDbDir);
      FileUtils.deleteQuietly(targetFile);

      //TODO Create Checkpoint based on OM DB type.
      // Currently, OM DB type is not configurable. Hence, defaulting to
      // RocksDB.
      return new RocksDBCheckpoint(untarredDbDir);
    } catch (IOException e) {
      LOG.error("Unable to obtain Ozone Manager DB Snapshot. ", e);
    }
    return null;
  }
}

