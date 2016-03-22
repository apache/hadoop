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

import com.google.common.base.Preconditions;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus.DiskBalancerWorkEntry;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus.Result;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerConstants;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerException;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.NodePlan;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.Step;
import org.apache.hadoop.util.Time;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Worker class for Disk Balancer.
 * <p/>
 * Here is the high level logic executed by this class. Users can submit disk
 * balancing plans using submitPlan calls. After a set of sanity checks the plan
 * is admitted and put into workMap.
 * <p/>
 * The executePlan launches a thread that picks up work from workMap and hands
 * it over to the BlockMover#copyBlocks function.
 * <p/>
 * Constraints :
 * <p/>
 * Only one plan can be executing in a datanode at any given time. This is
 * ensured by checking the future handle of the worker thread in submitPlan.
 */
@InterfaceAudience.Private
public class DiskBalancer {

  private static final Logger LOG = LoggerFactory.getLogger(DiskBalancer
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
  private DiskBalancerWorkStatus.Result currentResult;
  private long bandwidth;

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
    this.currentResult = Result.NO_PLAN;
    this.blockMover = blockMover;
    this.dataset = this.blockMover.getDataset();
    this.dataNodeUUID = dataNodeUUID;
    scheduler = Executors.newSingleThreadExecutor();
    lock = new ReentrantLock();
    workMap = new ConcurrentHashMap<>();
    this.isDiskBalancerEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_DISK_BALANCER_ENABLED,
        DFSConfigKeys.DFS_DISK_BALANCER_ENABLED_DEFAULT);
  }

  /**
   * Shutdown  disk balancer services.
   */
  public void shutdown() {
    lock.lock();
    try {
      this.isDiskBalancerEnabled = false;
      this.currentResult = Result.NO_PLAN;
      if ((this.future != null) && (!this.future.isDone())) {
        this.currentResult = Result.PLAN_CANCELLED;
        this.blockMover.setExitFlag();
        shutdownExecutor();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Shutdown the executor.
   */
  private void shutdownExecutor() {
    scheduler.shutdown();
    try {
      if(!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
        if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
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
   * @param planID      - A SHA512 of the plan string
   * @param planVersion - version of the plan string - for future use.
   * @param plan        - Actual Plan
   * @param bandwidth   - BytesPerSec to copy
   * @param force       - Skip some validations and execute the plan file.
   * @throws DiskBalancerException
   */
  public void submitPlan(String planID, long planVersion, String plan,
                         long bandwidth, boolean force)
      throws DiskBalancerException {

    lock.lock();
    try {
      checkDiskBalancerEnabled();
      if ((this.future != null) && (!this.future.isDone())) {
        LOG.error("Disk Balancer - Executing another plan, submitPlan failed.");
        throw new DiskBalancerException("Executing another plan",
            DiskBalancerException.Result.PLAN_ALREADY_IN_PROGRESS);
      }
      NodePlan nodePlan =
          verifyPlan(planID, planVersion, plan, bandwidth, force);
      createWorkPlan(nodePlan);
      this.planID = planID;
      this.currentResult = Result.PLAN_UNDER_PROGRESS;
      this.bandwidth = bandwidth;
      executePlan();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns the Current Work Status of a submitted Plan.
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
          new DiskBalancerWorkStatus(this.currentResult, this.planID);
      for (Map.Entry<VolumePair, DiskBalancerWorkItem> entry :
          workMap.entrySet()) {
        DiskBalancerWorkEntry workEntry = new DiskBalancerWorkEntry(
            entry.getKey().getSource().getBasePath(),
            entry.getKey().getDest().getBasePath(),
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
   * @param planID - Hash of the plan to cancel.
   * @throws DiskBalancerException
   */
  public void cancelPlan(String planID) throws DiskBalancerException {
    lock.lock();
    try {
      checkDiskBalancerEnabled();
      if ((this.planID == null) || (!this.planID.equals(planID))) {
        LOG.error("Disk Balancer - No such plan. Cancel plan failed. PlanID: " +
            planID);
        throw new DiskBalancerException("No such plan.",
            DiskBalancerException.Result.NO_SUCH_PLAN);
      }
      if (!this.future.isDone()) {
        this.blockMover.setExitFlag();
        shutdownExecutor();
        this.currentResult = Result.PLAN_CANCELLED;
      }
    } finally {
      lock.unlock();
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
      Map<String, String> pathMap = new HashMap<>();
      Map<String, FsVolumeSpi> volMap = getStorageIDToVolumeMap();
      for (Map.Entry<String, FsVolumeSpi> entry : volMap.entrySet()) {
        pathMap.put(entry.getKey(), entry.getValue().getBasePath());
      }
      ObjectMapper mapper = new ObjectMapper();
      return mapper.writeValueAsString(pathMap);
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
      LOG.error("Disk Balancer is not enabled.");
      throw new DiskBalancerException("Disk Balancer is not enabled.",
          DiskBalancerException.Result.DISK_BALANCER_NOT_ENABLED);
    }
  }

  /**
   * Verifies that user provided plan is valid.
   *
   * @param planID      - SHA 512 of the plan.
   * @param planVersion - Version of the plan, for future use.
   * @param plan        - Plan String in Json.
   * @param bandwidth   - Max disk bandwidth to use per second.
   * @param force       - Skip verifying when the plan was generated.
   * @return a NodePlan Object.
   * @throws DiskBalancerException
   */
  private NodePlan verifyPlan(String planID, long planVersion, String plan,
                              long bandwidth, boolean force)
      throws DiskBalancerException {

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
   * Verifies that plan matches the SHA512 provided by the client.
   *
   * @param planID - Sha512 Hex Bytes
   * @param plan   - Plan String
   * @throws DiskBalancerException
   */
  private NodePlan verifyPlanHash(String planID, String plan)
      throws DiskBalancerException {
    final long sha512Length = 128;
    if (plan == null || plan.length() == 0) {
      LOG.error("Disk Balancer -  Invalid plan.");
      throw new DiskBalancerException("Invalid plan.",
          DiskBalancerException.Result.INVALID_PLAN);
    }

    if ((planID == null) ||
        (planID.length() != sha512Length) ||
        !DigestUtils.sha512Hex(plan.getBytes(Charset.forName("UTF-8")))
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

    // TODO : Support Valid Plan hours as a user configurable option.
    if ((planTime +
        (TimeUnit.HOURS.toMillis(
            DiskBalancerConstants.DISKBALANCER_VALID_PLAN_HOURS))) < now) {
      String hourString = "Plan was generated more than " +
           Integer.toString(DiskBalancerConstants.DISKBALANCER_VALID_PLAN_HOURS)
          +  " hours ago.";
      LOG.error("Disk Balancer - " + hourString);
      throw new DiskBalancerException(hourString,
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
    Map<String, FsVolumeSpi> pathMap = getStorageIDToVolumeMap();

    for (Step step : plan.getVolumeSetPlans()) {
      String sourceuuid = step.getSourceVolume().getUuid();
      String destinationuuid = step.getDestinationVolume().getUuid();

      FsVolumeSpi sourceVol = pathMap.get(sourceuuid);
      if (sourceVol == null) {
        LOG.error("Disk Balancer - Unable to find source volume. submitPlan " +
            "failed.");
        throw new DiskBalancerException("Unable to find source volume.",
            DiskBalancerException.Result.INVALID_VOLUME);
      }

      FsVolumeSpi destVol = pathMap.get(destinationuuid);
      if (destVol == null) {
        LOG.error("Disk Balancer - Unable to find destination volume. " +
            "submitPlan failed.");
        throw new DiskBalancerException("Unable to find destination volume.",
            DiskBalancerException.Result.INVALID_VOLUME);
      }
      createWorkPlan(sourceVol, destVol, step.getBytesToMove());
    }
  }

  /**
   * Returns a path to Volume Map.
   *
   * @return Map
   * @throws DiskBalancerException
   */
  private Map<String, FsVolumeSpi> getStorageIDToVolumeMap()
      throws DiskBalancerException {
    Map<String, FsVolumeSpi> pathMap = new HashMap<>();
    FsDatasetSpi.FsVolumeReferences references;
    try {
      synchronized (this.dataset) {
        references = this.dataset.getFsVolumeReferences();
        for (int ndx = 0; ndx < references.size(); ndx++) {
          FsVolumeSpi vol = references.get(ndx);
          pathMap.put(vol.getStorageID(), vol);
        }
        references.close();
      }
    } catch (IOException ex) {
      LOG.error("Disk Balancer - Internal Error.", ex);
      throw new DiskBalancerException("Internal error", ex,
          DiskBalancerException.Result.INTERNAL_ERROR);
    }
    return pathMap;
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
        LOG.info("Executing Disk balancer plan. Plan ID -  " + planID);

        for (Map.Entry<VolumePair, DiskBalancerWorkItem> entry :
            workMap.entrySet()) {
          blockMover.copyBlocks(entry.getKey(), entry.getValue());
        }
      }
    });
  }

  /**
   * Insert work items to work map.
   *
   * @param source      - Source vol
   * @param dest        - destination volume
   * @param bytesToMove - number of bytes to move
   */
  private void createWorkPlan(FsVolumeSpi source, FsVolumeSpi dest,
                              long bytesToMove) throws DiskBalancerException {

    if(source.getStorageID().equals(dest.getStorageID())) {
      throw new DiskBalancerException("Same source and destination",
          DiskBalancerException.Result.INVALID_MOVE);
    }
    VolumePair pair = new VolumePair(source, dest);

    // In case we have a plan with more than
    // one line of same <source, dest>
    // we compress that into one work order.
    if (workMap.containsKey(pair)) {
      bytesToMove += workMap.get(pair).getBytesToCopy();
    }

    DiskBalancerWorkItem work = new DiskBalancerWorkItem(bytesToMove, 0);
    workMap.put(pair, work);
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
  }

  /**
   * Holds references to actual volumes that we will be operating against.
   */
  public static class VolumePair {
    private final FsVolumeSpi source;
    private final FsVolumeSpi dest;

    /**
     * Constructs a volume pair.
     *
     * @param source - Source Volume
     * @param dest   - Destination Volume
     */
    public VolumePair(FsVolumeSpi source, FsVolumeSpi dest) {
      this.source = source;
      this.dest = dest;
    }

    /**
     * gets source volume.
     *
     * @return volume
     */
    public FsVolumeSpi getSource() {
      return source;
    }

    /**
     * Gets Destination volume.
     *
     * @return volume.
     */
    public FsVolumeSpi getDest() {
      return dest;
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
      return source.equals(that.source) && dest.equals(that.dest);
    }

    @Override
    public int hashCode() {
      int result = source.getBasePath().hashCode();
      result = 31 * result + dest.getBasePath().hashCode();
      return result;
    }
  }

  /**
   * Actual DataMover class for DiskBalancer.
   * <p/>
   * TODO : Add implementation for this class. This is here as a place holder so
   * that Datanode can make calls into this class.
   */
  public static class DiskBalancerMover implements BlockMover {
    private final FsDatasetSpi dataset;

    /**
     * Constructs diskBalancerMover.
     *
     * @param dataset Dataset
     * @param conf    Configuration
     */
    public DiskBalancerMover(FsDatasetSpi dataset, Configuration conf) {
      this.dataset = dataset;
      // TODO : Read Config values.
    }

    /**
     * Copies blocks from a set of volumes.
     *
     * @param pair - Source and Destination Volumes.
     * @param item - Number of bytes to move from volumes.
     */
    @Override
    public void copyBlocks(VolumePair pair, DiskBalancerWorkItem item) {

    }

    /**
     * Begin the actual copy operations. This is useful in testing.
     */
    @Override
    public void setRunnable() {

    }

    /**
     * Tells copyBlocks to exit from the copy routine.
     */
    @Override
    public void setExitFlag() {

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
  }
}
