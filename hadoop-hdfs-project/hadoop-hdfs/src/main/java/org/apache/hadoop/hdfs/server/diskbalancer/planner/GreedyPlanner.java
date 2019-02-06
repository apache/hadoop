/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer.planner;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel
    .DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel
    .DiskBalancerVolumeSet;
import org.apache.hadoop.util.Time;

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * Greedy Planner is a simple planner that computes the largest possible move at
 * any point of time given a volumeSet.
 * <p>
 * This is done by choosing the disks with largest  amount of data above and
 * below the idealStorage and then a move is scheduled between them.
 */
public class GreedyPlanner implements Planner {
  public static final long MB = 1024L * 1024L;
  public static final long GB = MB * 1024L;
  public static final long TB = GB * 1024L;
  private static final Logger LOG =
      LoggerFactory.getLogger(GreedyPlanner.class);
  private final double threshold;

  /**
   * Constructs a greedy planner.
   *
   * @param threshold - Disk tolerance that we are ok with
   * @param node      - node on which this planner is operating upon
   */
  public GreedyPlanner(double threshold, DiskBalancerDataNode node) {
    this.threshold = threshold;
  }

  /**
   * Computes a node plan for the given node.
   *
   * @return NodePlan
   * @throws Exception
   */
  @Override
  public NodePlan plan(DiskBalancerDataNode node) throws Exception {
    final long startTime = Time.monotonicNow();
    NodePlan plan = new NodePlan(node.getDataNodeName(),
        node.getDataNodePort());
    LOG.info("Starting plan for Node : {}:{}",
        node.getDataNodeName(), node.getDataNodePort());
    while (node.isBalancingNeeded(this.threshold)) {
      for (DiskBalancerVolumeSet vSet : node.getVolumeSets().values()) {
        balanceVolumeSet(node, vSet, plan);
      }
    }

    final long endTime = Time.monotonicNow();
    LOG.info("Compute Plan for Node : {}:{} took {} ms",
        node.getDataNodeName(), node.getDataNodePort(), endTime - startTime);

    return plan;
  }

  /**
   * Computes Steps to make a DiskBalancerVolumeSet Balanced.
   *
   * @param node
   * @param vSet - DiskBalancerVolumeSet
   * @param plan - NodePlan
   */
  public void balanceVolumeSet(DiskBalancerDataNode node,
                               DiskBalancerVolumeSet vSet, NodePlan plan)
      throws Exception {
    Preconditions.checkNotNull(vSet);
    Preconditions.checkNotNull(plan);
    Preconditions.checkNotNull(node);
    DiskBalancerVolumeSet currentSet = new DiskBalancerVolumeSet(vSet);

    while (currentSet.isBalancingNeeded(this.threshold)) {
      removeSkipVolumes(currentSet);

      DiskBalancerVolume lowVolume = currentSet.getSortedQueue().first();
      DiskBalancerVolume highVolume = currentSet.getSortedQueue().last();

      Step nextStep = null;
      // ok both volumes bytes used are in the range that we expect
      // Then we create a move request.
      if (!lowVolume.isSkip() && !highVolume.isSkip()) {
        nextStep = computeMove(currentSet, lowVolume, highVolume);
      } else {
        LOG.debug("Skipping compute move. lowVolume: {} highVolume: {}",
            lowVolume.getPath(), highVolume.getPath());
      }

      applyStep(nextStep, currentSet, lowVolume, highVolume);
      if (nextStep != null) {
        LOG.debug("Step : {} ", nextStep);
        plan.addStep(nextStep);
      }
    }

    LOG.info("Disk Volume set {} - Type : {} plan completed.",
        currentSet.getSetID(),
        currentSet.getVolumes().get(0).getStorageType());

    plan.setNodeName(node.getDataNodeName());
    plan.setNodeUUID(node.getDataNodeUUID());
    plan.setTimeStamp(Time.now());
    plan.setPort(node.getDataNodePort());
  }

  /**
   * Apply steps applies the current step on to a volumeSet so that we can
   * compute next steps until we reach the desired goals.
   *
   * @param nextStep   - nextStep or Null
   * @param currentSet - Current Disk BalancerVolume Set we are operating upon
   * @param lowVolume  - volume
   * @param highVolume - volume
   */
  private void applyStep(Step nextStep, DiskBalancerVolumeSet currentSet,
                         DiskBalancerVolume lowVolume,
                         DiskBalancerVolume highVolume) throws Exception {

    long used;
    if (nextStep != null) {
      used = lowVolume.getUsed() + nextStep.getBytesToMove();
      lowVolume.setUsed(used);

      used = highVolume.getUsed() - nextStep.getBytesToMove();
      highVolume.setUsed(used);
    }

    // since the volume data changed , we need to recompute the DataDensity.
    currentSet.computeVolumeDataDensity();
    printQueue(currentSet.getSortedQueue());
  }

  /**
   * Computes a data move from the largest disk we have to smallest disk.
   *
   * @param currentSet - Current Disk Set we are working with
   * @param lowVolume  - Low Data Capacity Volume
   * @param highVolume - High Data Capacity Volume
   * @return Step
   */
  private Step computeMove(DiskBalancerVolumeSet currentSet,
                           DiskBalancerVolume lowVolume,
                           DiskBalancerVolume highVolume) {
    // Compute how many bytes we can move. First Compute the maximum that
    // low Volume Can receive, then compute maximum high volume can give
    // Then take the minimum of those two numbers that is the bytesToMove.

    long maxLowVolumeCanReceive = (long) (
        (currentSet.getIdealUsed() * lowVolume.computeEffectiveCapacity()) -
            lowVolume.getUsed());

    // This disk cannot take any more data from any disk.
    // Remove it from our computation matrix.
    if (maxLowVolumeCanReceive <= 0) {
      LOG.debug("{} Skipping disk from computation. Maximum data size " +
          "achieved.", lowVolume.getPath());
      skipVolume(currentSet, lowVolume);
    }

    long maxHighVolumeCanGive = highVolume.getUsed() -
        (long) (currentSet.getIdealUsed() *
            highVolume.computeEffectiveCapacity());
    // This volume cannot give any more data, remove it from the
    // computation matrix
    if (maxHighVolumeCanGive <= 0) {
      LOG.debug(" {} Skipping disk from computation. Minimum data size " +
          "achieved.", highVolume.getPath());
      skipVolume(currentSet, highVolume);
    }


    long bytesToMove = Math.min(maxLowVolumeCanReceive, maxHighVolumeCanGive);
    Step nextStep = null;

    if (bytesToMove > 0) {
      // Create a new step
      nextStep = new MoveStep(highVolume, currentSet.getIdealUsed(), lowVolume,
          bytesToMove, currentSet.getSetID());
      LOG.debug("Next Step: {}", nextStep);
    }
    return nextStep;
  }

  /**
   * Skips this volume if needed.
   *
   * @param currentSet - Current Disk set
   * @param volume     - Volume
   */
  private void skipVolume(DiskBalancerVolumeSet currentSet,
                          DiskBalancerVolume volume) {
    if (LOG.isDebugEnabled()) {
      String message =
          String.format(
              "Skipping volume. Volume : %s " +
              "Type : %s Target " +
              "Number of bytes : %f lowVolume dfsUsed : %d. Skipping this " +
              "volume from all future balancing calls.", volume.getPath(),
              volume.getStorageType(),
              currentSet.getIdealUsed() * volume.getCapacity(),
              volume.getUsed());
      LOG.debug(message);
    }
    volume.setSkip(true);
  }

  // Removes all volumes which are part of the volumeSet but skip flag is set.
  private void removeSkipVolumes(DiskBalancerVolumeSet currentSet) {
    List<DiskBalancerVolume> volumeList = currentSet.getVolumes();
    Iterator<DiskBalancerVolume> volumeIterator = volumeList.iterator();
    while (volumeIterator.hasNext()) {
      DiskBalancerVolume vol = volumeIterator.next();
      if (vol.isSkip() || vol.isFailed()) {
        currentSet.removeVolume(vol);
      }
    }
    currentSet.computeVolumeDataDensity();
    printQueue(currentSet.getSortedQueue());
  }

  /**
   * This function is used only for debugging purposes to ensure queue looks
   * correct.
   *
   * @param queue - Queue
   */
  private void printQueue(TreeSet<DiskBalancerVolume> queue) {
    if (LOG.isDebugEnabled()) {
      String format =
          String.format(
              "First Volume : %s, DataDensity : %f, " +
              "Last Volume : %s, DataDensity : %f",
              queue.first().getPath(), queue.first().getVolumeDataDensity(),
              queue.last().getPath(), queue.last().getVolumeDataDensity());
      LOG.debug(format);
    }
  }
}
