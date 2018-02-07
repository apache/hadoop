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
package org.apache.hadoop.hdfs.server.namenode.sps;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfyPathStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.sps.ExternalStoragePolicySatisfier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This manages satisfy storage policy invoked path ids and expose methods to
 * process these path ids. It maintains sps mode(INTERNAL/EXTERNAL/NONE)
 * configured by the administrator.
 *
 * <p>
 * If the configured mode is {@link StoragePolicySatisfierMode.INTERNAL}, then
 * it will start internal sps daemon service inside namenode and process sps
 * invoked path ids to satisfy the storage policy.
 *
 * <p>
 * If the configured mode is {@link StoragePolicySatisfierMode.EXTERNAL}, then
 * it won't do anything, just maintains the sps invoked path ids. Administrator
 * requires to start external sps service explicitly, to fetch the sps invoked
 * path ids from namenode, then do necessary computations and block movement in
 * order to satisfy the storage policy. Please refer
 * {@link ExternalStoragePolicySatisfier} class to understand more about the
 * external sps service functionality.
 *
 * <p>
 * If the configured mode is {@link StoragePolicySatisfierMode.NONE}, then it
 * will disable the sps feature completely by clearing all queued up sps path's
 * hint.
 *
 * This class is instantiated by the BlockManager.
 */
public class StoragePolicySatisfyManager {
  private static final Logger LOG = LoggerFactory
      .getLogger(StoragePolicySatisfyManager.class);
  private final StoragePolicySatisfier spsService;
  private final boolean storagePolicyEnabled;
  private volatile StoragePolicySatisfierMode mode;
  private final Queue<Long> pathsToBeTraveresed;
  private final int outstandingPathsLimit;
  private final Namesystem namesystem;
  private final BlockManager blkMgr;

  public StoragePolicySatisfyManager(Configuration conf, Namesystem namesystem,
      BlockManager blkMgr) {
    // StoragePolicySatisfier(SPS) configs
    storagePolicyEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY,
        DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_DEFAULT);
    String modeVal = conf.get(
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_DEFAULT);
    outstandingPathsLimit = conf.getInt(
        DFSConfigKeys.DFS_SPS_MAX_OUTSTANDING_PATHS_KEY,
        DFSConfigKeys.DFS_SPS_MAX_OUTSTANDING_PATHS_DEFAULT);
    mode = StoragePolicySatisfierMode.fromString(modeVal);
    pathsToBeTraveresed = new LinkedList<Long>();
    // instantiate SPS service by just keeps config reference and not starting
    // any supporting threads.
    spsService = new StoragePolicySatisfier(conf);
    this.namesystem = namesystem;
    this.blkMgr = blkMgr;
  }

  /**
   * This function will do following logic based on the configured sps mode:
   *
   * <p>
   * If the configured mode is {@link StoragePolicySatisfierMode.INTERNAL}, then
   * starts internal daemon service inside namenode.
   *
   * <p>
   * If the configured mode is {@link StoragePolicySatisfierMode.EXTERNAL}, then
   * it won't do anything. Administrator requires to start external sps service
   * explicitly.
   *
   * <p>
   * If the configured mode is {@link StoragePolicySatisfierMode.NONE}, then the
   * service is disabled and won't do any action.
   */
  public void start() {
    if (!storagePolicyEnabled) {
      LOG.info("Disabling StoragePolicySatisfier service as {} set to {}.",
          DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY, storagePolicyEnabled);
      return;
    }

    switch (mode) {
    case INTERNAL:
      if (spsService.isRunning()) {
        LOG.info("Storage policy satisfier is already running"
            + " as internal daemon service inside namenode.");
        return;
      }
      // starts internal daemon service inside namenode
      spsService.init(
          new IntraSPSNameNodeContext(namesystem, blkMgr, spsService),
          new IntraSPSNameNodeFileIdCollector(namesystem.getFSDirectory(),
              spsService),
          new IntraSPSNameNodeBlockMoveTaskHandler(blkMgr, namesystem), null);
      spsService.start(false, mode);
      break;
    case EXTERNAL:
      LOG.info("Storage policy satisfier is configured as external, "
          + "please start external sps service explicitly to satisfy policy");
      break;
    case NONE:
      LOG.info("Storage policy satisfier is disabled");
      break;
    default:
      LOG.info("Given mode: {} is invalid", mode);
      break;
    }
  }

  /**
   * This function will do following logic based on the configured sps mode:
   *
   * <p>
   * If the configured mode is {@link StoragePolicySatisfierMode.INTERNAL}, then
   * stops internal daemon service inside namenode.
   *
   * <p>
   * If the configured mode is {@link StoragePolicySatisfierMode.EXTERNAL}, then
   * it won't do anything. Administrator requires to stop external sps service
   * explicitly, if needed.
   *
   * <p>
   * If the configured mode is {@link StoragePolicySatisfierMode.NONE}, then the
   * service is disabled and won't do any action.
   */
  public void stop() {
    if (!storagePolicyEnabled) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Storage policy is not enabled, ignoring");
      }
      return;
    }

    switch (mode) {
    case INTERNAL:
      removeAllPathIds();
      if (!spsService.isRunning()) {
        LOG.info("Internal storage policy satisfier daemon service"
            + " is not running");
        return;
      }
      // stops internal daemon service running inside namenode
      spsService.stop(false);
      break;
    case EXTERNAL:
      removeAllPathIds();
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Storage policy satisfier service is running outside namenode"
            + ", ignoring");
      }
      break;
    case NONE:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Storage policy satisfier is not enabled, ignoring");
      }
      break;
    default:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Invalid mode:{}, ignoring", mode);
      }
      break;
    }
  }

  /**
   * Sets new sps mode. If the new mode is internal, then it will start internal
   * sps service inside namenode. If the new mode is external, then stops
   * internal sps service running(if any) inside namenode. If the new mode is
   * none, then it will disable the sps feature completely by clearing all
   * queued up sps path's hint.
   */
  public void changeModeEvent(StoragePolicySatisfierMode newMode) {
    if (!storagePolicyEnabled) {
      LOG.info("Failed to change storage policy satisfier as {} set to {}.",
          DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY, storagePolicyEnabled);
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Updating SPS service status, current mode:{}, new mode:{}",
          mode, newMode);
    }

    switch (newMode) {
    case INTERNAL:
      if (spsService.isRunning()) {
        LOG.info("Storage policy satisfier is already running as {} mode.",
            mode);
        return;
      }
      spsService.init(
          new IntraSPSNameNodeContext(this.namesystem, this.blkMgr, spsService),
          new IntraSPSNameNodeFileIdCollector(this.namesystem.getFSDirectory(),
              spsService),
          new IntraSPSNameNodeBlockMoveTaskHandler(this.blkMgr,
              this.namesystem),
          null);
      spsService.start(true, newMode);
      break;
    case EXTERNAL:
      if (mode == newMode) {
        LOG.info("Storage policy satisfier is already in mode:{},"
            + " so ignoring change mode event.", newMode);
        return;
      }
      spsService.stopGracefully();
      break;
    case NONE:
      if (mode == newMode) {
        LOG.info("Storage policy satisfier is already disabled, mode:{}"
            + " so ignoring change mode event.", newMode);
        return;
      }
      LOG.info("Disabling StoragePolicySatisfier, mode:{}", newMode);
      spsService.stop(true);
      removeAllPathIds();
      break;
    default:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Given mode: {} is invalid", newMode);
      }
      break;
    }

    // update sps mode
    mode = newMode;
  }

  /**
   * This function will do following logic based on the configured sps mode:
   *
   * <p>
   * If the configured mode is {@link StoragePolicySatisfierMode.INTERNAL}, then
   * timed wait to stop internal storage policy satisfier daemon threads.
   *
   * <p>
   * If the configured mode is {@link StoragePolicySatisfierMode.EXTERNAL}, then
   * it won't do anything, just ignore it.
   *
   * <p>
   * If the configured mode is {@link StoragePolicySatisfierMode.NONE}, then the
   * service is disabled. It won't do any action, just ignore it.
   */
  public void stopGracefully() {
    switch (mode) {
    case INTERNAL:
      spsService.stopGracefully();
      break;
    case EXTERNAL:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Ignoring, StoragePolicySatisfier feature is running"
            + " outside namenode");
      }
      break;
    case NONE:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Ignoring, StoragePolicySatisfier feature is disabled");
      }
      break;
    default:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Invalid mode:{}", mode);
      }
      break;
    }
  }

  /**
   * @return true if the internal storage policy satisfier daemon is running,
   *         false otherwise.
   */
  public boolean isInternalSatisfierRunning() {
    return spsService.isRunning();
  }

  /**
   * @return internal SPS service instance.
   */
  public SPSService getInternalSPSService() {
    return this.spsService;
  }

  /**
   * @return status Storage policy satisfy status of the path. It is supported
   *         only for the internal sps daemon service.
   * @throws IOException
   *           if the Satisfier is not running inside namenode.
   */
  public StoragePolicySatisfyPathStatus checkStoragePolicySatisfyPathStatus(
      String path) throws IOException {
    if (mode != StoragePolicySatisfierMode.INTERNAL) {
      LOG.debug("Satisfier is not running inside namenode, so status "
          + "can't be returned.");
      throw new IOException("Satisfier is not running inside namenode, "
          + "so status can't be returned.");
    }
    return spsService.checkStoragePolicySatisfyPathStatus(path);
  }

  /**
   * @return the next SPS path id, on which path users has invoked to satisfy
   *         storages.
   */
  public Long getNextPathId() {
    synchronized (pathsToBeTraveresed) {
      return pathsToBeTraveresed.poll();
    }
  }

  /**
   * Verify that satisfier queue limit exceeds allowed outstanding limit.
   */
  public void verifyOutstandingPathQLimit() throws IOException {
    long size = pathsToBeTraveresed.size();
    // Checking that the SPS call Q exceeds the allowed limit.
    if (outstandingPathsLimit - size <= 0) {
      LOG.debug("Satisifer Q - outstanding limit:{}, current size:{}",
          outstandingPathsLimit, size);
      throw new IOException("Outstanding satisfier queue limit: "
          + outstandingPathsLimit + " exceeded, try later!");
    }
  }

  /**
   * Removes the SPS path id from the list of sps paths.
   */
  public void removePathId(long trackId) {
    synchronized (pathsToBeTraveresed) {
      pathsToBeTraveresed.remove(trackId);
    }
  }

  /**
   * Clean up all sps path ids.
   */
  public void removeAllPathIds() {
    synchronized (pathsToBeTraveresed) {
      pathsToBeTraveresed.clear();
    }
  }

  /**
   * Adds the sps path to SPSPathIds list.
   */
  public void addPathId(long id) {
    synchronized (pathsToBeTraveresed) {
      pathsToBeTraveresed.add(id);
    }
  }

  /**
   * @return true if sps is configured as an internal service or external
   *         service, false otherwise.
   */
  public boolean isEnabled() {
    return mode == StoragePolicySatisfierMode.INTERNAL
        || mode == StoragePolicySatisfierMode.EXTERNAL;
  }

  /**
   * @return sps service mode.
   */
  public StoragePolicySatisfierMode getMode() {
    return mode;
  }
}
