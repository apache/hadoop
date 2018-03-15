/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.scm.container.replication;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdsl.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;
import org.apache.hadoop.ozone.scm.exceptions.SCMException;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.ozone.scm.node.NodePoolManager;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.util.concurrent.Uninterruptibles
    .sleepUninterruptibly;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_CONTAINER_REPORTS_WAIT_TIMEOUT_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_CONTAINER_REPORTS_WAIT_TIMEOUT;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_CONTAINER_REPORT_PROCESSING_INTERVAL_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_CONTAINER_REPORT_PROCESSING_INTERVAL;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_MAX_CONTAINER_REPORT_THREADS;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_MAX_CONTAINER_REPORT_THREADS_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_MAX_NODEPOOL_PROCESSING_THREADS;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_MAX_NODEPOOL_PROCESSING_THREADS_DEFAULT;

/**
 * This class takes a set of container reports that belong to a pool and then
 * computes the replication levels for each container.
 */
public class ContainerSupervisor implements Closeable {
  public static final Logger LOG =
      LoggerFactory.getLogger(ContainerSupervisor.class);

  private final NodePoolManager poolManager;
  private final HashSet<String> poolNames;
  private final PriorityQueue<PeriodicPool> poolQueue;
  private final NodeManager nodeManager;
  private final long containerProcessingLag;
  private final AtomicBoolean runnable;
  private final ExecutorService executorService;
  private final long maxPoolWait;
  private long poolProcessCount;
  private final List<InProgressPool> inProgressPoolList;
  private final AtomicInteger threadFaultCount;
  private final int inProgressPoolMaxCount;

  private final ReadWriteLock inProgressPoolListLock;

  /**
   * Returns the number of times we have processed pools.
   * @return long
   */
  public long getPoolProcessCount() {
    return poolProcessCount;
  }


  /**
   * Constructs a class that computes Replication Levels.
   *
   * @param conf - OzoneConfiguration
   * @param nodeManager - Node Manager
   * @param poolManager - Pool Manager
   */
  public ContainerSupervisor(Configuration conf, NodeManager nodeManager,
                             NodePoolManager poolManager) {
    Preconditions.checkNotNull(poolManager);
    Preconditions.checkNotNull(nodeManager);
    this.containerProcessingLag =
        conf.getTimeDuration(OZONE_SCM_CONTAINER_REPORT_PROCESSING_INTERVAL,
            OZONE_SCM_CONTAINER_REPORT_PROCESSING_INTERVAL_DEFAULT,
            TimeUnit.SECONDS
        ) * 1000;
    int maxContainerReportThreads =
        conf.getInt(OZONE_SCM_MAX_CONTAINER_REPORT_THREADS,
            OZONE_SCM_MAX_CONTAINER_REPORT_THREADS_DEFAULT
        );
    this.maxPoolWait =
        conf.getTimeDuration(OZONE_SCM_CONTAINER_REPORTS_WAIT_TIMEOUT,
            OZONE_SCM_CONTAINER_REPORTS_WAIT_TIMEOUT_DEFAULT,
            TimeUnit.MILLISECONDS);
    this.inProgressPoolMaxCount = conf.getInt(
        OZONE_SCM_MAX_NODEPOOL_PROCESSING_THREADS,
        OZONE_SCM_MAX_NODEPOOL_PROCESSING_THREADS_DEFAULT);
    this.poolManager = poolManager;
    this.nodeManager = nodeManager;
    this.poolNames = new HashSet<>();
    this.poolQueue = new PriorityQueue<>();
    this.runnable = new AtomicBoolean(true);
    this.threadFaultCount = new AtomicInteger(0);
    this.executorService = HadoopExecutors.newCachedThreadPool(
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("Container Reports Processing Thread - %d")
            .build(), maxContainerReportThreads);
    this.inProgressPoolList = new LinkedList<>();
    this.inProgressPoolListLock = new ReentrantReadWriteLock();

    initPoolProcessThread();
  }

  /**
   * Returns the number of pools that are under process right now.
   * @return  int - Number of pools that are in process.
   */
  public int getInProgressPoolCount() {
    return inProgressPoolList.size();
  }

  /**
   * Exits the background thread.
   */
  public void setExit() {
    this.runnable.set(false);
  }

  /**
   * Adds or removes pools from names that we need to process.
   *
   * There are two different cases that we need to process.
   * The case where some pools are being added and some times we have to
   * handle cases where pools are removed.
   */
  private void refreshPools() {
    List<String> pools = this.poolManager.getNodePools();
    if (pools != null) {

      HashSet<String> removedPools =
          computePoolDifference(this.poolNames, new HashSet<>(pools));

      HashSet<String> addedPools =
          computePoolDifference(new HashSet<>(pools), this.poolNames);
      // TODO: Support remove pool API in pool manager so that this code
      // path can be tested. This never happens in the current code base.
      for (String poolName : removedPools) {
        for (PeriodicPool periodicPool : poolQueue) {
          if (periodicPool.getPoolName().compareTo(poolName) == 0) {
            poolQueue.remove(periodicPool);
          }
        }
      }
      // Remove the pool names that we have in the list.
      this.poolNames.removeAll(removedPools);

      for (String poolName : addedPools) {
        poolQueue.add(new PeriodicPool(poolName));
      }

      // Add to the pool names we are tracking.
      poolNames.addAll(addedPools);
    }

  }

  /**
   * Handle the case where pools are added.
   *
   * @param newPools - New Pools list
   * @param oldPool - oldPool List.
   */
  private HashSet<String> computePoolDifference(HashSet<String> newPools,
      Set<String> oldPool) {
    Preconditions.checkNotNull(newPools);
    Preconditions.checkNotNull(oldPool);
    HashSet<String> newSet = new HashSet<>(newPools);
    newSet.removeAll(oldPool);
    return newSet;
  }

  private void initPoolProcessThread() {

    /*
     * Task that runs to check if we need to start a pool processing job.
     * if so we create a pool reconciliation job and find out of all the
     * expected containers are on the nodes.
     */
    Runnable processPools = () -> {
      while (runnable.get()) {
        // Make sure that we don't have any new pools.
        refreshPools();
        while (inProgressPoolList.size() < inProgressPoolMaxCount) {
          PeriodicPool pool = poolQueue.poll();
          if (pool != null) {
            if (pool.getLastProcessedTime() + this.containerProcessingLag >
                Time.monotonicNow()) {
              LOG.debug("Not within the time window for processing: {}",
                  pool.getPoolName());
              // we might over sleep here, not a big deal.
              sleepUninterruptibly(this.containerProcessingLag,
                  TimeUnit.MILLISECONDS);
            }
            LOG.debug("Adding pool {} to container processing queue",
                pool.getPoolName());
            InProgressPool inProgressPool = new InProgressPool(maxPoolWait,
                pool, this.nodeManager, this.poolManager, this.executorService);
            inProgressPool.startReconciliation();
            inProgressPoolListLock.writeLock().lock();
            try {
              inProgressPoolList.add(inProgressPool);
            } finally {
              inProgressPoolListLock.writeLock().unlock();
            }
            poolProcessCount++;
          } else {
            break;
          }
        }
        sleepUninterruptibly(this.maxPoolWait, TimeUnit.MILLISECONDS);
        inProgressPoolListLock.readLock().lock();
        try {
          for (InProgressPool inProgressPool : inProgressPoolList) {
            inProgressPool.finalizeReconciliation();
            poolQueue.add(inProgressPool.getPool());
          }
        } finally {
          inProgressPoolListLock.readLock().unlock();
        }
        inProgressPoolListLock.writeLock().lock();
        try {
          inProgressPoolList.clear();
        } finally {
          inProgressPoolListLock.writeLock().unlock();
        }
      }
    };

    // We will have only one thread for pool processing.
    Thread poolProcessThread = new Thread(processPools);
    poolProcessThread.setDaemon(true);
    poolProcessThread.setName("Pool replica thread");
    poolProcessThread.setUncaughtExceptionHandler((Thread t, Throwable e) -> {
      // Let us just restart this thread after logging a critical error.
      // if this thread is not running we cannot handle commands from SCM.
      LOG.error("Critical Error : Pool replica thread encountered an " +
          "error. Thread: {} Error Count : {}", t.toString(), e,
          threadFaultCount.incrementAndGet());
      poolProcessThread.start();
      // TODO : Add a config to restrict how many times we will restart this
      // thread in a single session.
    });
    poolProcessThread.start();
  }

  /**
   * Adds a container report to appropriate inProgress Pool.
   * @param containerReport  -- Container report for a specific container from
   * a datanode.
   */
  public void handleContainerReport(
      ContainerReportsRequestProto containerReport) {
    DatanodeID datanodeID = DatanodeID.getFromProtoBuf(
        containerReport.getDatanodeID());
    inProgressPoolListLock.readLock().lock();
    try {
      String poolName = poolManager.getNodePool(datanodeID);
      for (InProgressPool ppool : inProgressPoolList) {
        if (ppool.getPoolName().equalsIgnoreCase(poolName)) {
          ppool.handleContainerReport(containerReport);
          return;
        }
      }
      // TODO: Decide if we can do anything else with this report.
      LOG.debug("Discarding the container report for pool {}. " +
              "That pool is not currently in the pool reconciliation process." +
              " Container Name: {}", poolName, containerReport.getDatanodeID());
    } catch (SCMException e) {
      LOG.warn("Skipping processing container report from datanode {}, "
              + "cause: failed to get the corresponding node pool",
          datanodeID.toString(), e);
    } finally {
      inProgressPoolListLock.readLock().unlock();
    }
  }

  /**
   * Get in process pool list, used for testing.
   * @return List of InProgressPool
   */
  @VisibleForTesting
  public List<InProgressPool> getInProcessPoolList() {
    return inProgressPoolList;
  }

  /**
   * Shutdown the Container Replication Manager.
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    setExit();
    HadoopExecutors.shutdown(executorService, LOG, 5, TimeUnit.SECONDS);
  }
}
