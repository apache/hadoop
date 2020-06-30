/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi
    .FsVolumeReferences;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus
    .DiskBalancerWorkEntry;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus.Result;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerConstants;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerException;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.NodePlan;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.Step;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Worker class for Disk Balancer.
 * <p>
 * Here is the high level logic executed by this class. Users can submit disk
 * balancing plans using submitPlan calls. After a set of sanity checks the plan
 * is admitted and put into workMap.
 * <p>
 * The executePlan launches a thread that picks up work from workMap and hands
 * it over to the BlockMover#copyBlocks function.
 * <p>
 * Constraints :
 * <p>
 * Only one plan can be executing in a datanode at any given time. This is
 * ensured by checking the future handle of the worker thread in submitPlan.
 */
@InterfaceAudience.Private
public class DiskBalancer {

  @VisibleForTesting
  public static final Logger LOG = LoggerFactory.getLogger(DiskBalancer
      .class);
  private final FsDatasetSpi<?> dataset;
  private final String dataNodeUUID;
  private final BlockMover blockMover;
  private final ReentrantLock lock;
  private final ConcurrentHashMap<VolumePair, DiskBalancerWorkItem> workMap;
  private boolean isDiskBalancerEnabled = false;
  private ExecutorService scheduler;
  private Future future;
  private String planID;
  private String planFile;
  private DiskBalancerWorkStatus.Result currentResult;
  private long bandwidth;
  private long planValidityInterval;
  private final Configuration config;

  /**
   * Constructs a Disk Balancer object. This object takes care of reading a
   * NodePlan and executing it against a set of volumes.
   *
   * @param dataNodeUUID - Data node UUID
   * @param conf         - Hdfs Config
   * @param blockMover   - Object that supports moving blocks.
   */
  public DiskBalancer(String dataNodeUUID,
                      Configuration conf, BlockMover blockMover) {
    this.config = conf;
    this.currentResult = Result.NO_PLAN;
    this.blockMover = blockMover;
    this.dataset = this.blockMover.getDataset();
    this.dataNodeUUID = dataNodeUUID;
    scheduler = Executors.newSingleThreadExecutor();
    lock = new ReentrantLock();
    workMap = new ConcurrentHashMap<>();
    this.planID = "";  // to keep protobuf happy.
    this.planFile = "";  // to keep protobuf happy.
    this.isDiskBalancerEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_DISK_BALANCER_ENABLED,
        DFSConfigKeys.DFS_DISK_BALANCER_ENABLED_DEFAULT);
    this.bandwidth = conf.getInt(
        DFSConfigKeys.DFS_DISK_BALANCER_MAX_DISK_THROUGHPUT,
        DFSConfigKeys.DFS_DISK_BALANCER_MAX_DISK_THROUGHPUT_DEFAULT);
    this.planValidityInterval = conf.getTimeDuration(
        DFSConfigKeys.DFS_DISK_BALANCER_PLAN_VALID_INTERVAL,
        DFSConfigKeys.DFS_DISK_BALANCER_PLAN_VALID_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Shutdown  disk balancer services.
   */
  public void shutdown() {
    lock.lock();
    boolean needShutdown = false;
    try {
      this.isDiskBalancerEnabled = false;
      this.currentResult = Result.NO_PLAN;
      if ((this.future != null) && (!this.future.isDone())) {
        this.currentResult = Result.PLAN_CANCELLED;
        this.blockMover.setExitFlag();
        scheduler.shutdown();
        needShutdown = true;
      }
    } finally {
      lock.unlock();
    }
    // no need to hold lock while shutting down executor.
    if (needShutdown) {
      shutdownExecutor();
    }
  }

  /**
   * Shutdown the executor.
   */
  private void shutdownExecutor() {
    final int secondsTowait = 10;
    try {
      if (!scheduler.awaitTermination(secondsTowait, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
        if (!scheduler.awaitTermination(secondsTowait, TimeUnit.SECONDS)) {
          LOG.error("Disk Balancer : Scheduler did not terminate.");
        }
      }
    } catch (InterruptedException ex) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Takes a client submitted plan and converts into a set of work items that
   * can be executed by the blockMover.
   *
   * @param planId      - A SHA-1 of the plan string
   * @param planVersion - version of the plan string - for future use.
   * @param planFileName    - Plan file name
   * @param planData    - Plan data in json format
   * @param force       - Skip some validations and execute the plan file.
   * @throws DiskBalancerException
   */
  public void submitPlan(String planId, long planVersion, String planFileName,
                         String planData, boolean force)
          throws DiskBalancerException {
    lock.lock();
    try {
      checkDiskBalancerEnabled();
      if ((this.future != null) && (!this.future.isDone())) {
        LOG.error("Disk Balancer - Executing another plan, submitPlan failed.");
        throw new DiskBalancerException("Executing another plan",
            DiskBalancerException.Result.PLAN_ALREADY_IN_PROGRESS);
      }
      NodePlan nodePlan = verifyPlan(planId, planVersion, planData, force);
      createWorkPlan(nodePlan);
      this.planID = planId;
      this.planFile = planFileName;
      this.currentResult = Result.PLAN_UNDER_PROGRESS;
      executePlan();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get FsVolume by volume UUID.
   * @param fsDataset
   * @param volUuid
   * @return FsVolumeSpi
   */
  private static FsVolumeSpi getFsVolume(final FsDatasetSpi<?> fsDataset,
      final String volUuid) {
    FsVolumeSpi fsVolume = null;
    try (FsVolumeReferences volumeReferences =
           fsDataset.getFsVolumeReferences()) {
      for (int i = 0; i < volumeReferences.size(); i++) {
        if (volumeReferences.get(i).getStorageID().equals(volUuid)) {
          fsVolume = volumeReferences.get(i);
          break;
        }
      }
    } catch (IOException e) {
      LOG.warn("Disk Balancer - Error when closing volume references: ", e);
    }
    return fsVolume;
  }

  /**
   * Returns the current work status of a previously submitted Plan.
   *
   * @return DiskBalancerWorkStatus.
   * @throws DiskBalancerException
   */
  public DiskBalancerWorkStatus queryWorkStatus() throws DiskBalancerException {
    lock.lock();
    try {
      checkDiskBalancerEnabled();
      // if we had a plan in progress, check if it is finished.
      if (this.currentResult == Result.PLAN_UNDER_PROGRESS &&
          this.future != null &&
          this.future.isDone()) {
        this.currentResult = Result.PLAN_DONE;
      }

      DiskBalancerWorkStatus status =
          new DiskBalancerWorkStatus(this.currentResult, this.planID,
                  this.planFile);
      for (Map.Entry<VolumePair, DiskBalancerWorkItem> entry :
          workMap.entrySet()) {
        DiskBalancerWorkEntry workEntry = new DiskBalancerWorkEntry(
            entry.getKey().getSourceVolBasePath(),
            entry.getKey().getDestVolBasePath(),
            entry.getValue());
        status.addWorkEntry(workEntry);
      }
      return status;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Cancels a running plan.
   *
   * @param planID - Hash of the plan to cancel.
   * @throws DiskBalancerException
   */
  public void cancelPlan(String planID) throws DiskBalancerException {
    lock.lock();
    boolean needShutdown = false;
    try {
      checkDiskBalancerEnabled();
      if (this.planID == null ||
          !this.planID.equals(planID) ||
          this.planID.isEmpty()) {
        LOG.error("Disk Balancer - No such plan. Cancel plan failed. PlanID: " +
            planID);
        throw new DiskBalancerException("No such plan.",
            DiskBalancerException.Result.NO_SUCH_PLAN);
      }
      if (!this.future.isDone()) {
        this.currentResult = Result.PLAN_CANCELLED;
        this.blockMover.setExitFlag();
        scheduler.shutdown();
        needShutdown = true;
      }
    } finally {
      lock.unlock();
    }
    // no need to hold lock while shutting down executor.
    if (needShutdown) {
      shutdownExecutor();
    }
  }

  /**
   * Returns a volume ID to Volume base path map.
   *
   * @return Json string of the volume map.
   * @throws DiskBalancerException
   */
  public String getVolumeNames() throws DiskBalancerException {
    lock.lock();
    try {
      checkDiskBalancerEnabled();
      return JsonUtil.toJsonString(getStorageIDToVolumeBasePathMap());
    } catch (DiskBalancerException ex) {
      throw ex;
    } catch (IOException e) {
      throw new DiskBalancerException("Internal error, Unable to " +
          "create JSON string.", e,
          DiskBalancerException.Result.INTERNAL_ERROR);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns the current bandwidth.
   *
   * @return string representation of bandwidth.
   * @throws DiskBalancerException
   */
  public long getBandwidth() throws DiskBalancerException {
    lock.lock();
    try {
      checkDiskBalancerEnabled();
      return this.bandwidth;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Throws if Disk balancer is disabled.
   *
   * @throws DiskBalancerException
   */
  private void checkDiskBalancerEnabled()
      throws DiskBalancerException {
    if (!isDiskBalancerEnabled) {
      throw new DiskBalancerException("Disk Balancer is not enabled.",
          DiskBalancerException.Result.DISK_BALANCER_NOT_ENABLED);
    }
  }

  /**
   * Verifies that user provided plan is valid.
   *
   * @param planID      - SHA-1 of the plan.
   * @param planVersion - Version of the plan, for future use.
   * @param plan        - Plan String in Json.
   * @param force       - Skip verifying when the plan was generated.
   * @return a NodePlan Object.
   * @throws DiskBalancerException
   */
  private NodePlan verifyPlan(String planID, long planVersion, String plan,
                              boolean force) throws DiskBalancerException {

    Preconditions.checkState(lock.isHeldByCurrentThread());
    verifyPlanVersion(planVersion);
    NodePlan nodePlan = verifyPlanHash(planID, plan);
    if (!force) {
      verifyTimeStamp(nodePlan);
    }
    verifyNodeUUID(nodePlan);
    return nodePlan;
  }

  /**
   * Verifies the plan version is something that we support.
   *
   * @param planVersion - Long version.
   * @throws DiskBalancerException
   */
  private void verifyPlanVersion(long planVersion)
      throws DiskBalancerException {
    if ((planVersion < DiskBalancerConstants.DISKBALANCER_MIN_VERSION) ||
        (planVersion > DiskBalancerConstants.DISKBALANCER_MAX_VERSION)) {
      LOG.error("Disk Balancer - Invalid plan version.");
      throw new DiskBalancerException("Invalid plan version.",
          DiskBalancerException.Result.INVALID_PLAN_VERSION);
    }
  }

  /**
   * Verifies that plan matches the SHA-1 provided by the client.
   *
   * @param planID - SHA-1 Hex Bytes
   * @param plan   - Plan String
   * @throws DiskBalancerException
   */
  private NodePlan verifyPlanHash(String planID, String plan)
      throws DiskBalancerException {
    final long sha1Length = 40;
    if (plan == null || plan.length() == 0) {
      LOG.error("Disk Balancer -  Invalid plan.");
      throw new DiskBalancerException("Invalid plan.",
          DiskBalancerException.Result.INVALID_PLAN);
    }

    if ((planID == null) ||
        (planID.length() != sha1Length) ||
        !DigestUtils.shaHex(plan.getBytes(Charset.forName("UTF-8")))
            .equalsIgnoreCase(planID)) {
      LOG.error("Disk Balancer - Invalid plan hash.");
      throw new DiskBalancerException("Invalid or mis-matched hash.",
          DiskBalancerException.Result.INVALID_PLAN_HASH);
    }

    try {
      return NodePlan.parseJson(plan);
    } catch (IOException ex) {
      throw new DiskBalancerException("Parsing plan failed.", ex,
          DiskBalancerException.Result.MALFORMED_PLAN);
    }
  }

  /**
   * Verifies that this plan is not older than 24 hours.
   *
   * @param plan - Node Plan
   */
  private void verifyTimeStamp(NodePlan plan) throws DiskBalancerException {
    long now = Time.now();
    long planTime = plan.getTimeStamp();

    if ((planTime + planValidityInterval) < now) {
      String planValidity = config.get(
          DFSConfigKeys.DFS_DISK_BALANCER_PLAN_VALID_INTERVAL,
          DFSConfigKeys.DFS_DISK_BALANCER_PLAN_VALID_INTERVAL_DEFAULT);
      if (planValidity.matches("[0-9]$")) {
        planValidity += "ms";
      }
      String errorString = "Plan was generated more than " + planValidity
          + " ago";
      LOG.error("Disk Balancer - " + errorString);
      throw new DiskBalancerException(errorString,
          DiskBalancerException.Result.OLD_PLAN_SUBMITTED);
    }
  }

  /**
   * Verify Node UUID.
   *
   * @param plan - Node Plan
   */
  private void verifyNodeUUID(NodePlan plan) throws DiskBalancerException {
    if ((plan.getNodeUUID() == null) ||
        !plan.getNodeUUID().equals(this.dataNodeUUID)) {
      LOG.error("Disk Balancer - Plan was generated for another node.");
      throw new DiskBalancerException(
          "Plan was generated for another node.",
          DiskBalancerException.Result.DATANODE_ID_MISMATCH);
    }
  }

  /**
   * Convert a node plan to DiskBalancerWorkItem that Datanode can execute.
   *
   * @param plan - Node Plan
   */
  private void createWorkPlan(NodePlan plan) throws DiskBalancerException {
    Preconditions.checkState(lock.isHeldByCurrentThread());

    // Cleanup any residual work in the map.
    workMap.clear();
    Map<String, String> storageIDToVolBasePathMap =
        getStorageIDToVolumeBasePathMap();

    for (Step step : plan.getVolumeSetPlans()) {
      String sourceVolUuid = step.getSourceVolume().getUuid();
      String destVolUuid = step.getDestinationVolume().getUuid();

      String sourceVolBasePath = storageIDToVolBasePathMap.get(sourceVolUuid);
      if (sourceVolBasePath == null) {
        final String errMsg = "Disk Balancer - Unable to find volume: "
            + step.getSourceVolume().getPath() + ". SubmitPlan failed.";
        LOG.error(errMsg);
        throw new DiskBalancerException(errMsg,
            DiskBalancerException.Result.INVALID_VOLUME);
      }

      String destVolBasePath = storageIDToVolBasePathMap.get(destVolUuid);
      if (destVolBasePath == null) {
        final String errMsg = "Disk Balancer - Unable to find volume: "
            + step.getDestinationVolume().getPath() + ". SubmitPlan failed.";
        LOG.error(errMsg);
        throw new DiskBalancerException(errMsg,
            DiskBalancerException.Result.INVALID_VOLUME);
      }
      VolumePair volumePair = new VolumePair(sourceVolUuid,
          sourceVolBasePath, destVolUuid, destVolBasePath);
      createWorkPlan(volumePair, step);
    }
  }

  /**
   * Returns volume UUID to volume base path map.
   *
   * @return Map
   * @throws DiskBalancerException
   */
  private Map<String, String> getStorageIDToVolumeBasePathMap()
      throws DiskBalancerException {
    Map<String, String> storageIDToVolBasePathMap = new HashMap<>();
    FsDatasetSpi.FsVolumeReferences references;
    try {
      try(AutoCloseableLock lock = this.dataset.acquireDatasetReadLock()) {
        references = this.dataset.getFsVolumeReferences();
        for (int ndx = 0; ndx < references.size(); ndx++) {
          FsVolumeSpi vol = references.get(ndx);
          storageIDToVolBasePathMap.put(vol.getStorageID(),
              vol.getBaseURI().getPath());
        }
        references.close();
      }
    } catch (IOException ex) {
      LOG.error("Disk Balancer - Internal Error.", ex);
      throw new DiskBalancerException("Internal error", ex,
          DiskBalancerException.Result.INTERNAL_ERROR);
    }
    return storageIDToVolBasePathMap;
  }

  /**
   * Starts Executing the plan, exits when the plan is done executing.
   */
  private void executePlan() {
    Preconditions.checkState(lock.isHeldByCurrentThread());
    this.blockMover.setRunnable();
    if (this.scheduler.isShutdown()) {
      this.scheduler = Executors.newSingleThreadExecutor();
    }

    this.future = scheduler.submit(new Runnable() {
      @Override
      public void run() {
        Thread.currentThread().setName("DiskBalancerThread");
        LOG.info("Executing Disk balancer plan. Plan File: {}, Plan ID: {}",
            planFile, planID);
        for (Map.Entry<VolumePair, DiskBalancerWorkItem> entry :
            workMap.entrySet()) {
          blockMover.setRunnable();
          blockMover.copyBlocks(entry.getKey(), entry.getValue());
        }
      }
    });
  }

  /**
   * Insert work items to work map.
   * @param volumePair - VolumePair
   * @param step - Move Step
   */
  private void createWorkPlan(final VolumePair volumePair, Step step)
      throws DiskBalancerException {
    if (volumePair.getSourceVolUuid().equals(volumePair.getDestVolUuid())) {
      final String errMsg = "Disk Balancer - Source and destination volumes " +
          "are same: " + volumePair.getSourceVolUuid();
      LOG.warn(errMsg);
      throw new DiskBalancerException(errMsg,
          DiskBalancerException.Result.INVALID_MOVE);
    }
    long bytesToMove = step.getBytesToMove();
    // In case we have a plan with more than
    // one line of same VolumePair
    // we compress that into one work order.
    if (workMap.containsKey(volumePair)) {
      bytesToMove += workMap.get(volumePair).getBytesToCopy();
    }

    DiskBalancerWorkItem work = new DiskBalancerWorkItem(bytesToMove, 0);

    // all these values can be zero, if so we will use
    // values from configuration.
    work.setBandwidth(step.getBandwidth());
    work.setTolerancePercent(step.getTolerancePercent());
    work.setMaxDiskErrors(step.getMaxDiskErrors());
    workMap.put(volumePair, work);
  }

  /**
   * BlockMover supports moving blocks across Volumes.
   */
  public interface BlockMover {
    /**
     * Copies blocks from a set of volumes.
     *
     * @param pair - Source and Destination Volumes.
     * @param item - Number of bytes to move from volumes.
     */
    void copyBlocks(VolumePair pair, DiskBalancerWorkItem item);

    /**
     * Begin the actual copy operations. This is useful in testing.
     */
    void setRunnable();

    /**
     * Tells copyBlocks to exit from the copy routine.
     */
    void setExitFlag();

    /**
     * Returns a pointer to the current dataset we are operating against.
     *
     * @return FsDatasetSpi
     */
    FsDatasetSpi getDataset();

    /**
     * Returns time when this plan started executing.
     *
     * @return Start time in milliseconds.
     */
    long getStartTime();

    /**
     * Number of seconds elapsed.
     *
     * @return time in seconds
     */
    long getElapsedSeconds();

  }

  /**
   * Holds source and dest volumes UUIDs and their BasePaths
   * that disk balancer will be operating against.
   */
  public static class VolumePair {
    private final String sourceVolUuid;
    private final String destVolUuid;
    private final String sourceVolBasePath;
    private final String destVolBasePath;

    /**
     * Constructs a volume pair.
     * @param sourceVolUuid     - Source Volume
     * @param sourceVolBasePath - Source Volume Base Path
     * @param destVolUuid       - Destination Volume
     * @param destVolBasePath   - Destination Volume Base Path
     */
    public VolumePair(final String sourceVolUuid,
        final String sourceVolBasePath, final String destVolUuid,
        final String destVolBasePath) {
      this.sourceVolUuid = sourceVolUuid;
      this.sourceVolBasePath = sourceVolBasePath;
      this.destVolUuid = destVolUuid;
      this.destVolBasePath = destVolBasePath;
    }

    /**
     * Gets source volume UUID.
     *
     * @return UUID String
     */
    public String getSourceVolUuid() {
      return sourceVolUuid;
    }

    /**
     * Gets source volume base path.
     * @return String
     */
    public String getSourceVolBasePath() {
      return sourceVolBasePath;
    }
    /**
     * Gets destination volume UUID.
     *
     * @return UUID String
     */
    public String getDestVolUuid() {
      return destVolUuid;
    }

    /**
     * Gets desitnation volume base path.
     *
     * @return String
     */
    public String getDestVolBasePath() {
      return destVolBasePath;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      VolumePair that = (VolumePair) o;
      return sourceVolUuid.equals(that.sourceVolUuid)
          && sourceVolBasePath.equals(that.sourceVolBasePath)
          && destVolUuid.equals(that.destVolUuid)
          && destVolBasePath.equals(that.destVolBasePath);
    }

    @Override
    public int hashCode() {
      final int primeNum = 31;
      final List<String> volumeStrList = Arrays.asList(sourceVolUuid,
          sourceVolBasePath, destVolUuid, destVolBasePath);
      int result = 1;
      for (String str : volumeStrList) {
        result = (result * primeNum) + str.hashCode();
      }
      return result;
    }
  }

  /**
   * Actual DataMover class for DiskBalancer.
   * <p>
   */
  public static class DiskBalancerMover implements BlockMover {
    private final FsDatasetSpi dataset;
    private long diskBandwidth;
    private long blockTolerance;
    private long maxDiskErrors;
    private int poolIndex;
    private AtomicBoolean shouldRun;
    private long startTime;
    private long secondsElapsed;

    /**
     * Constructs diskBalancerMover.
     *
     * @param dataset Dataset
     * @param conf    Configuration
     */
    public DiskBalancerMover(FsDatasetSpi dataset, Configuration conf) {
      this.dataset = dataset;
      shouldRun = new AtomicBoolean(false);

      this.diskBandwidth = conf.getLong(
          DFSConfigKeys.DFS_DISK_BALANCER_MAX_DISK_THROUGHPUT,
          DFSConfigKeys.DFS_DISK_BALANCER_MAX_DISK_THROUGHPUT_DEFAULT);

      this.blockTolerance = conf.getLong(
          DFSConfigKeys.DFS_DISK_BALANCER_BLOCK_TOLERANCE,
          DFSConfigKeys.DFS_DISK_BALANCER_BLOCK_TOLERANCE_DEFAULT);

      this.maxDiskErrors = conf.getLong(
          DFSConfigKeys.DFS_DISK_BALANCER_MAX_DISK_ERRORS,
          DFSConfigKeys.DFS_DISK_BALANCER_MAX_DISK_ERRORS_DEFAULT);

      // Since these are user provided values make sure it is sane
      // or ignore faulty values.
      if (this.diskBandwidth <= 0) {
        LOG.debug("Found 0 or less as max disk throughput, ignoring config " +
            "value. value : " + diskBandwidth);
        diskBandwidth =
            DFSConfigKeys.DFS_DISK_BALANCER_MAX_DISK_THROUGHPUT_DEFAULT;
      }

      if (this.blockTolerance <= 0) {
        LOG.debug("Found 0 or less for block tolerance value, ignoring config" +
            "value. value : " + blockTolerance);
        blockTolerance =
            DFSConfigKeys.DFS_DISK_BALANCER_BLOCK_TOLERANCE_DEFAULT;

      }

      if (this.maxDiskErrors < 0) {
        LOG.debug("Found  less than 0 for maxDiskErrors value, ignoring " +
            "config value. value : " + maxDiskErrors);
        maxDiskErrors =
            DFSConfigKeys.DFS_DISK_BALANCER_MAX_DISK_ERRORS_DEFAULT;
      }
    }

    /**
     * Sets Diskmover copyblocks into runnable state.
     */
    @Override
    public void setRunnable() {
      this.shouldRun.set(true);
    }

    /**
     * Signals copy block to exit.
     */
    @Override
    public void setExitFlag() {
      this.shouldRun.set(false);
    }

    /**
     * Returns the shouldRun boolean flag.
     */
    public boolean shouldRun() {
      return this.shouldRun.get();
    }

    /**
     * Checks if a given block is less than needed size to meet our goal.
     *
     * @param blockSize - block len
     * @param item      - Work item
     * @return true if this block meets our criteria, false otherwise.
     */
    private boolean isLessThanNeeded(long blockSize,
                                     DiskBalancerWorkItem item) {
      long bytesToCopy = item.getBytesToCopy() - item.getBytesCopied();
      bytesToCopy = bytesToCopy +
          ((bytesToCopy * getBlockTolerancePercentage(item)) / 100);
      return (blockSize <= bytesToCopy) ? true : false;
    }

    /**
     * Returns the default block tolerance if the plan does not have value of
     * tolerance specified.
     *
     * @param item - DiskBalancerWorkItem
     * @return long
     */
    private long getBlockTolerancePercentage(DiskBalancerWorkItem item) {
      return item.getTolerancePercent() <= 0 ? this.blockTolerance :
          item.getTolerancePercent();
    }

    /**
     * Inflates bytesCopied and returns true or false. This allows us to stop
     * copying if we have reached close enough.
     *
     * @param item DiskBalancerWorkItem
     * @return -- false if we need to copy more, true if we are done
     */
    private boolean isCloseEnough(DiskBalancerWorkItem item) {
      long temp = item.getBytesCopied() +
          ((item.getBytesCopied() * getBlockTolerancePercentage(item)) / 100);
      return (item.getBytesToCopy() >= temp) ? false : true;
    }

    /**
     * Returns disk bandwidth associated with this plan, if none is specified
     * returns the global default.
     *
     * @param item DiskBalancerWorkItem.
     * @return MB/s - long
     */
    private long getDiskBandwidth(DiskBalancerWorkItem item) {
      return item.getBandwidth() <= 0 ? this.diskBandwidth : item
          .getBandwidth();
    }

    /**
     * Computes sleep delay needed based on the block that just got copied. we
     * copy using a burst mode, that is we let the copy proceed in full
     * throttle. Once a copy is done, we compute how many bytes have been
     * transferred and try to average it over the user specified bandwidth. In
     * other words, This code implements a poor man's token bucket algorithm for
     * traffic shaping.
     *
     * @param bytesCopied - byteCopied.
     * @param timeUsed    in milliseconds
     * @param item        DiskBalancerWorkItem
     * @return sleep delay in Milliseconds.
     */
    @VisibleForTesting
    public long computeDelay(long bytesCopied, long timeUsed,
                              DiskBalancerWorkItem item) {

      // we had an overflow, ignore this reading and continue.
      if (timeUsed == 0) {
        return 0;
      }
      final int megaByte = 1024 * 1024;
      if (bytesCopied < megaByte) {
        return 0;
      }
      long bytesInMB = bytesCopied / megaByte;

      // converting disk bandwidth in MB/millisec
      float bandwidth = getDiskBandwidth(item) / 1000f;
      float delay = ((long) (bytesInMB / bandwidth) - timeUsed);
      return (delay <= 0) ? 0 : (long) delay;
    }

    /**
     * Returns maximum errors to tolerate for the specific plan or the default.
     *
     * @param item - DiskBalancerWorkItem
     * @return maximum error counts to tolerate.
     */
    private long getMaxError(DiskBalancerWorkItem item) {
      return item.getMaxDiskErrors() <= 0 ? this.maxDiskErrors :
          item.getMaxDiskErrors();
    }

    /**
     * Gets the next block that we can copy, returns null if we cannot find a
     * block that fits our parameters or if have run out of blocks.
     *
     * @param iter Block Iter
     * @param item - Work item
     * @return Extended block or null if no copyable block is found.
     */
    private ExtendedBlock getBlockToCopy(FsVolumeSpi.BlockIterator iter,
                                         DiskBalancerWorkItem item) {
      while (!iter.atEnd() && item.getErrorCount() <= getMaxError(item)) {
        try {
          ExtendedBlock block = iter.nextBlock();
          if(null == block){
            LOG.info("NextBlock call returned null. No valid block to copy. {}",
                item.toJson());
            return null;
          }
          // A valid block is a finalized block, we iterate until we get
          // finalized blocks
          if (!this.dataset.isValidBlock(block)) {
            continue;
          }
          // We don't look for the best, we just do first fit
          if (isLessThanNeeded(block.getNumBytes(), item)) {
            return block;
          }
        } catch (IOException e) {
          item.incErrorCount();
        }
      }
      if (item.getErrorCount() > getMaxError(item)) {
        item.setErrMsg("Error count exceeded.");
        LOG.info("Maximum error count exceeded. Error count: {} Max error:{} ",
            item.getErrorCount(), item.getMaxDiskErrors());
      }
      return null;
    }

    /**
     * Opens all Block pools on a given volume.
     *
     * @param source    Source
     * @param poolIters List of PoolIters to maintain.
     */
    private void openPoolIters(FsVolumeSpi source, List<FsVolumeSpi
        .BlockIterator> poolIters) {
      Preconditions.checkNotNull(source);
      Preconditions.checkNotNull(poolIters);

      for (String blockPoolID : source.getBlockPoolList()) {
        poolIters.add(source.newBlockIterator(blockPoolID,
            "DiskBalancerSource"));
      }
    }

    /**
     * Returns the next block that we copy from all the block pools. This
     * function looks across all block pools to find the next block to copy.
     *
     * @param poolIters - List of BlockIterators
     * @return ExtendedBlock.
     */
    ExtendedBlock getNextBlock(List<FsVolumeSpi.BlockIterator> poolIters,
                               DiskBalancerWorkItem item) {
      Preconditions.checkNotNull(poolIters);
      int currentCount = 0;
      ExtendedBlock block = null;
      while (block == null && currentCount < poolIters.size()) {
        currentCount++;
        int index = poolIndex++ % poolIters.size();
        FsVolumeSpi.BlockIterator currentPoolIter = poolIters.get(index);
        block = getBlockToCopy(currentPoolIter, item);
      }

      if (block == null) {
        try {
          item.setErrMsg("No source blocks found to move.");
          LOG.error("No movable source blocks found. {}", item.toJson());
        } catch (IOException e) {
          LOG.error("Unable to get json from Item.");
        }
      }
      return block;
    }

    /**
     * Close all Pool Iters.
     *
     * @param poolIters List of BlockIters
     */
    private void closePoolIters(List<FsVolumeSpi.BlockIterator> poolIters) {
      Preconditions.checkNotNull(poolIters);
      for (FsVolumeSpi.BlockIterator iter : poolIters) {
        try {
          iter.close();
        } catch (IOException ex) {
          LOG.error("Error closing a block pool iter. ex: {}", ex);
        }
      }
    }

    /**
     * Copies blocks from a set of volumes.
     *
     * @param pair - Source and Destination Volumes.
     * @param item - Number of bytes to move from volumes.
     */
    @Override
    public void copyBlocks(VolumePair pair, DiskBalancerWorkItem item) {
      String sourceVolUuid = pair.getSourceVolUuid();
      String destVolUuuid = pair.getDestVolUuid();

      // When any of the DiskBalancerWorkItem volumes are not
      // available, return after setting error in item.
      FsVolumeSpi source = getFsVolume(this.dataset, sourceVolUuid);
      if (source == null) {
        final String errMsg = "Disk Balancer - Unable to find source volume: "
            + pair.getDestVolBasePath();
        LOG.error(errMsg);
        item.setErrMsg(errMsg);
        return;
      }
      FsVolumeSpi dest = getFsVolume(this.dataset, destVolUuuid);
      if (dest == null) {
        final String errMsg = "Disk Balancer - Unable to find dest volume: "
            + pair.getDestVolBasePath();
        LOG.error(errMsg);
        item.setErrMsg(errMsg);
        return;
      }

      if (source.isTransientStorage() || dest.isTransientStorage()) {
        final String errMsg = "Disk Balancer - Unable to support " +
                "transient storage type.";
        LOG.error(errMsg);
        item.setErrMsg(errMsg);
        return;
      }

      List<FsVolumeSpi.BlockIterator> poolIters = new LinkedList<>();
      startTime = Time.now();
      item.setStartTime(startTime);
      secondsElapsed = 0;

      try {
        openPoolIters(source, poolIters);
        if (poolIters.size() == 0) {
          LOG.error("No block pools found on volume. volume : {}. Exiting.",
              source.getBaseURI());
          return;
        }

        while (shouldRun()) {
          try {

            // Check for the max error count constraint.
            if (item.getErrorCount() > getMaxError(item)) {
              LOG.error("Exceeded the max error count. source {}, dest: {} " +
                      "error count: {}", source.getBaseURI(),
                  dest.getBaseURI(), item.getErrorCount());
              break;
            }

            // Check for the block tolerance constraint.
            if (isCloseEnough(item)) {
              LOG.info("Copy from {} to {} done. copied {} bytes and {} " +
                      "blocks.",
                  source.getBaseURI(), dest.getBaseURI(),
                  item.getBytesCopied(), item.getBlocksCopied());
              this.setExitFlag();
              continue;
            }

            ExtendedBlock block = getNextBlock(poolIters, item);
            // we are not able to find any blocks to copy.
            if (block == null) {
              LOG.error("No source blocks, exiting the copy. Source: {}, " +
                  "Dest:{}", source.getBaseURI(), dest.getBaseURI());
              this.setExitFlag();
              continue;
            }

            // check if someone told us exit, treat this as an interruption
            // point
            // for the thread, since both getNextBlock and moveBlocAcrossVolume
            // can take some time.
            if (!shouldRun()) {
              continue;
            }

            long timeUsed;
            // There is a race condition here, but we will get an IOException
            // if dest has no space, which we handle anyway.
            if (dest.getAvailable() > item.getBytesToCopy()) {
              long begin = System.nanoTime();
              this.dataset.moveBlockAcrossVolumes(block, dest);
              long now = System.nanoTime();
              timeUsed = (now - begin) > 0 ? now - begin : 0;
            } else {

              // Technically it is possible for us to find a smaller block and
              // make another copy, but opting for the safer choice of just
              // exiting here.
              LOG.error("Destination volume: {} does not have enough space to" +
                  " accommodate a block. Block Size: {} Exiting from" +
                  " copyBlocks.", dest.getBaseURI(), block.getNumBytes());
              break;
            }

            LOG.debug("Moved block with size {} from  {} to {}",
                block.getNumBytes(), source.getBaseURI(),
                dest.getBaseURI());

            // Check for the max throughput constraint.
            // We sleep here to keep the promise that we will not
            // copy more than Max MB/sec. we sleep enough time
            // to make sure that our promise is good on average.
            // Because we sleep, if a shutdown or cancel call comes in
            // we exit via Thread Interrupted exception.
            Thread.sleep(computeDelay(block.getNumBytes(), TimeUnit.NANOSECONDS
                .toMillis(timeUsed), item));

            // We delay updating the info to avoid confusing the user.
            // This way we report the copy only if it is under the
            // throughput threshold.
            item.incCopiedSoFar(block.getNumBytes());
            item.incBlocksCopied();
            secondsElapsed = TimeUnit.MILLISECONDS.toSeconds(Time.now() -
                startTime);
            item.setSecondsElapsed(secondsElapsed);
          } catch (IOException ex) {
            LOG.error("Exception while trying to copy blocks. error: {}", ex);
            item.incErrorCount();
          } catch (InterruptedException e) {
            LOG.error("Copy Block Thread interrupted, exiting the copy.");
            Thread.currentThread().interrupt();
            item.incErrorCount();
            this.setExitFlag();
          } catch (RuntimeException ex) {
            // Exiting if any run time exceptions.
            LOG.error("Got an unexpected Runtime Exception {}", ex);
            item.incErrorCount();
            this.setExitFlag();
          }
        }
      } finally {
        // Close all Iters.
        closePoolIters(poolIters);
      }
    }

    /**
     * Returns a pointer to the current dataset we are operating against.
     *
     * @return FsDatasetSpi
     */
    @Override
    public FsDatasetSpi getDataset() {
      return dataset;
    }

    /**
     * Returns time when this plan started executing.
     *
     * @return Start time in milliseconds.
     */
    @Override
    public long getStartTime() {
      return startTime;
    }

    /**
     * Number of seconds elapsed.
     *
     * @return time in seconds
     */
    @Override
    public long getElapsedSeconds() {
      return secondsElapsed;
    }
  }
}
