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

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_DB_CHECKPOINT_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OM_SNAPSHOT_DB;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_CONNECTION_REQUEST_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_CONNECTION_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_CONNECTION_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_FLUSH_PARAM;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_INTERVAL;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_SOCKET_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_SOCKET_TIMEOUT_DEFAULT;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DBUpdatesRequest;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.apache.hadoop.ozone.recon.tasks.OMDBUpdatesHandler;
import org.apache.hadoop.ozone.recon.tasks.OMUpdateEventBatch;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskController;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBUpdatesWrapper;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.ratis.protocol.ClientId;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
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

  private final CloseableHttpClient httpClient;
  private File omSnapshotDBParentDir = null;
  private String omDBSnapshotUrl;

  private OzoneManagerProtocol ozoneManagerClient;
  private final ClientId clientId = ClientId.randomId();
  private final OzoneConfiguration configuration;
  private final ScheduledExecutorService scheduler =
      Executors.newScheduledThreadPool(1);

  private ReconOMMetadataManager omMetadataManager;
  private ReconTaskController reconTaskController;
  private ReconTaskStatusDao reconTaskStatusDao;
  private ReconUtils reconUtils;
  private enum OmSnapshotTaskName {
    OM_DB_FULL_SNAPSHOT,
    OM_DB_DELTA_UPDATES
  }

  @Inject
  public OzoneManagerServiceProviderImpl(
      OzoneConfiguration configuration,
      ReconOMMetadataManager omMetadataManager,
      ReconTaskController reconTaskController,
      ReconUtils reconUtils,
      OzoneManagerProtocol ozoneManagerClient) throws IOException {

    String ozoneManagerHttpAddress = configuration.get(OMConfigKeys
        .OZONE_OM_HTTP_ADDRESS_KEY);

    String ozoneManagerHttpsAddress = configuration.get(OMConfigKeys
        .OZONE_OM_HTTPS_ADDRESS_KEY);

    omSnapshotDBParentDir = reconUtils.getReconDbDir(configuration,
        OZONE_RECON_OM_SNAPSHOT_DB_DIR);

    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(configuration);

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
        OZONE_OM_DB_CHECKPOINT_HTTP_ENDPOINT;

    if (policy.isHttpsEnabled()) {
      omDBSnapshotUrl = "https://" + ozoneManagerHttpsAddress +
          OZONE_OM_DB_CHECKPOINT_HTTP_ENDPOINT;
    }

    boolean flushParam = configuration.getBoolean(
        RECON_OM_SNAPSHOT_TASK_FLUSH_PARAM, false);

    if (flushParam) {
      omDBSnapshotUrl += "?" + OZONE_DB_CHECKPOINT_REQUEST_FLUSH + "=true";
    }

    this.reconUtils = reconUtils;
    this.omMetadataManager = omMetadataManager;
    this.reconTaskController = reconTaskController;
    this.reconTaskStatusDao = reconTaskController.getReconTaskStatusDao();
    this.ozoneManagerClient = ozoneManagerClient;
    this.configuration = configuration;
  }

  @Override
  public OMMetadataManager getOMMetadataManagerInstance() {
    return omMetadataManager;
  }

  @Override
  public void start() {
    try {
      omMetadataManager.start(configuration);
    } catch (IOException ioEx) {
      LOG.error("Error staring Recon OM Metadata Manager.", ioEx);
    }
    long initialDelay = configuration.getTimeDuration(
        RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY,
        RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY_DEFAULT,
        TimeUnit.MILLISECONDS);
    long interval = configuration.getTimeDuration(
        RECON_OM_SNAPSHOT_TASK_INTERVAL,
        RECON_OM_SNAPSHOT_TASK_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    scheduler.scheduleWithFixedDelay(this::syncDataFromOM,
        initialDelay,
        interval,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    reconTaskController.stop();
    scheduler.shutdownNow();
  }

  /**
   * Method to obtain current OM DB Snapshot.
   * @return DBCheckpoint instance.
   */
  @VisibleForTesting
  DBCheckpoint getOzoneManagerDBSnapshot() {
    String snapshotFileName = RECON_OM_SNAPSHOT_DB + "_" + System
        .currentTimeMillis();
    File targetFile = new File(omSnapshotDBParentDir, snapshotFileName +
        ".tar.gz");
    try {
      try (InputStream inputStream = reconUtils.makeHttpCall(httpClient,
          omDBSnapshotUrl)) {
        FileUtils.copyInputStreamToFile(inputStream, targetFile);
      }

      // Untar the checkpoint file.
      Path untarredDbDir = Paths.get(omSnapshotDBParentDir.getAbsolutePath(),
          snapshotFileName);
      reconUtils.untarCheckpointFile(targetFile, untarredDbDir);
      FileUtils.deleteQuietly(targetFile);

      // TODO Create Checkpoint based on OM DB type.
      // Currently, OM DB type is not configurable. Hence, defaulting to
      // RocksDB.
      return new RocksDBCheckpoint(untarredDbDir);
    } catch (IOException e) {
      LOG.error("Unable to obtain Ozone Manager DB Snapshot. ", e);
    }
    return null;
  }

  /**
   * Update Local OM DB with new OM DB snapshot.
   * @throws IOException
   */
  @VisibleForTesting
  boolean updateReconOmDBWithNewSnapshot() throws IOException {
    // Obtain the current DB snapshot from OM and
    // update the in house OM metadata managed DB instance.
    DBCheckpoint dbSnapshot = getOzoneManagerDBSnapshot();
    if (dbSnapshot != null && dbSnapshot.getCheckpointLocation() != null) {
      LOG.info("Got new checkpoint from OM : " +
          dbSnapshot.getCheckpointLocation());
      try {
        omMetadataManager.updateOmDB(dbSnapshot.getCheckpointLocation()
            .toFile());
        return true;
      } catch (IOException e) {
        LOG.error("Unable to refresh Recon OM DB Snapshot. ", e);
      }
    } else {
      LOG.error("Null snapshot location got from OM.");
    }
    return false;
  }

  /**
   * Get Delta updates from OM through RPC call and apply to local OM DB as
   * well as accumulate in a buffer.
   * @param fromSequenceNumber from sequence number to request from.
   * @param omdbUpdatesHandler OM DB updates handler to buffer updates.
   * @throws IOException when OM RPC request fails.
   * @throws RocksDBException when writing to RocksDB fails.
   */
  @VisibleForTesting
  void getAndApplyDeltaUpdatesFromOM(
      long fromSequenceNumber, OMDBUpdatesHandler omdbUpdatesHandler)
      throws IOException, RocksDBException {
    DBUpdatesRequest dbUpdatesRequest = DBUpdatesRequest.newBuilder()
        .setSequenceNumber(fromSequenceNumber).build();
    DBUpdatesWrapper dbUpdates = ozoneManagerClient.getDBUpdates(
        dbUpdatesRequest);
    if (null != dbUpdates) {
      RDBStore rocksDBStore = (RDBStore)omMetadataManager.getStore();
      RocksDB rocksDB = rocksDBStore.getDb();
      LOG.debug("Number of updates received from OM : " +
          dbUpdates.getData().size());
      for (byte[] data : dbUpdates.getData()) {
        WriteBatch writeBatch = new WriteBatch(data);
        writeBatch.iterate(omdbUpdatesHandler);
        RDBBatchOperation rdbBatchOperation = new RDBBatchOperation(writeBatch);
        rdbBatchOperation.commit(rocksDB, new WriteOptions());
      }
    }
  }

  /**
   * Based on current state of Recon's OM DB, we either get delta updates or
   * full snapshot from Ozone Manager.
   */
  @VisibleForTesting
  void syncDataFromOM() {
    LOG.info("Syncing data from Ozone Manager.");
    long currentSequenceNumber = getCurrentOMDBSequenceNumber();
    boolean fullSnapshot = false;

    if (currentSequenceNumber <= 0) {
      fullSnapshot = true;
    } else {
      OMDBUpdatesHandler omdbUpdatesHandler =
          new OMDBUpdatesHandler(omMetadataManager);
      try {
        LOG.info("Obtaining delta updates from Ozone Manager");
        // Get updates from OM and apply to local Recon OM DB.
        getAndApplyDeltaUpdatesFromOM(currentSequenceNumber,
            omdbUpdatesHandler);
        // Update timestamp of successful delta updates query.
        ReconTaskStatus reconTaskStatusRecord = new ReconTaskStatus(
            OmSnapshotTaskName.OM_DB_DELTA_UPDATES.name(),
                System.currentTimeMillis(), getCurrentOMDBSequenceNumber());
        reconTaskStatusDao.update(reconTaskStatusRecord);
        // Pass on DB update events to tasks that are listening.
        reconTaskController.consumeOMEvents(new OMUpdateEventBatch(
            omdbUpdatesHandler.getEvents()), omMetadataManager);
      } catch (IOException | InterruptedException | RocksDBException e) {
        LOG.warn("Unable to get and apply delta updates from OM.", e);
        fullSnapshot = true;
      }
    }

    if (fullSnapshot) {
      try {
        LOG.info("Obtaining full snapshot from Ozone Manager");
        // Update local Recon OM DB to new snapshot.
        boolean success = updateReconOmDBWithNewSnapshot();
        // Update timestamp of successful delta updates query.
        if (success) {
          ReconTaskStatus reconTaskStatusRecord =
              new ReconTaskStatus(
                  OmSnapshotTaskName.OM_DB_FULL_SNAPSHOT.name(),
                  System.currentTimeMillis(), getCurrentOMDBSequenceNumber());
          reconTaskStatusDao.update(reconTaskStatusRecord);
          // Reinitialize tasks that are listening.
          LOG.info("Calling reprocess on Recon tasks.");
          reconTaskController.reInitializeTasks(omMetadataManager);
        }
      } catch (IOException | InterruptedException e) {
        LOG.error("Unable to update Recon's OM DB with new snapshot ", e);
      }
    }
  }

  /**
   * Get OM RocksDB's latest sequence number.
   * @return latest sequence number.
   */
  private long getCurrentOMDBSequenceNumber() {
    RDBStore rocksDBStore = (RDBStore)omMetadataManager.getStore();
    if (null == rocksDBStore) {
      return 0;
    } else {
      return rocksDBStore.getDb().getLatestSequenceNumber();
    }
  }
}

