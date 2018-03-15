/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.ozone.scm.container.closer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_CONTAINER_REPORT_INTERVAL_DEFAULT;

/**
 * A class that manages closing of containers. This allows transition from a
 * open but full container to a closed container, to which no data is written.
 */
public class ContainerCloser {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerCloser.class);
  private static final long MULTIPLIER = 3L;
  private static final int CLEANUP_WATER_MARK = 1000;
  private final NodeManager nodeManager;
  private final Map<String, Long> commandIssued;
  private final Configuration configuration;
  private final AtomicInteger mapCount;
  private final long reportInterval;
  private final AtomicInteger threadRunCount;
  private final AtomicBoolean isRunning;

  /**
   * Constructs the ContainerCloser class.
   *
   * @param nodeManager - NodeManager
   * @param conf -   Configuration
   */
  public ContainerCloser(NodeManager nodeManager, Configuration conf) {
    Preconditions.checkNotNull(nodeManager);
    Preconditions.checkNotNull(conf);
    this.nodeManager = nodeManager;
    this.configuration = conf;
    this.commandIssued = new ConcurrentHashMap<>();
    this.mapCount = new AtomicInteger(0);
    this.threadRunCount = new AtomicInteger(0);
    this.isRunning = new AtomicBoolean(false);
    this.reportInterval = this.configuration.getTimeDuration(
        OZONE_CONTAINER_REPORT_INTERVAL,
        OZONE_CONTAINER_REPORT_INTERVAL_DEFAULT, TimeUnit.SECONDS);
    Preconditions.checkState(this.reportInterval > 0,
        "report interval has to be greater than 0");
  }

  @VisibleForTesting
  public static int getCleanupWaterMark() {
    return CLEANUP_WATER_MARK;
  }

  /**
   * Sends a Container Close command to the data nodes where this container
   * lives.
   *
   * @param info - ContainerInfo.
   */
  public void close(HdslProtos.SCMContainerInfo info) {

    if (commandIssued.containsKey(info.getContainerName())) {
      // We check if we issued a close command in last 3 * reportInterval secs.
      long commandQueueTime = commandIssued.get(info.getContainerName());
      long currentTime = TimeUnit.MILLISECONDS.toSeconds(Time.monotonicNow());
      if (currentTime > commandQueueTime + (MULTIPLIER * reportInterval)) {
        commandIssued.remove(info.getContainerName());
        mapCount.decrementAndGet();
      } else {
        // Ignore this request, since we just issued a close command. We
        // should wait instead of sending a command to datanode again.
        return;
      }
    }

    // if we reached here, it means that we have not issued a command to the
    // data node in last (3 times report interval). We are presuming that is
    // enough time to close the container. Let us go ahead and queue a close
    // to all the datanodes that participate in the container.
    //
    // Three important things to note here:
    //
    // 1. It is ok to send this command multiple times to a datanode. Close
    // container is an idempotent command, if the container is already closed
    // then we have no issues.
    //
    // 2. The container close command is issued to all datanodes. But
    // depending on the pipeline type, some of the datanodes might ignore it.
    //
    // 3. SCM will see that datanode is closed from container reports, but it
    // is possible that datanodes might get close commands since
    // this queue can be emptied by a datanode after a close report is send
    // to SCM. In that case also, data node will ignore this command.

    HdslProtos.Pipeline pipeline = info.getPipeline();
    for (HdfsProtos.DatanodeIDProto datanodeID :
        pipeline.getPipelineChannel().getMembersList()) {
      nodeManager.addDatanodeCommand(DatanodeID.getFromProtoBuf(datanodeID),
          new CloseContainerCommand(info.getContainerName()));
    }
    if (!commandIssued.containsKey(info.getContainerName())) {
      commandIssued.put(info.getContainerName(),
          TimeUnit.MILLISECONDS.toSeconds(Time.monotonicNow()));
      mapCount.incrementAndGet();
    }
    // run the hash map cleaner thread if needed, non-blocking call.
    runCleanerThreadIfNeeded();
  }

  private void runCleanerThreadIfNeeded() {
    // Let us check if we should run a cleaner thread, not using map.size
    // since it runs a loop in the case of the concurrentMap.
    if (mapCount.get() > CLEANUP_WATER_MARK &&
        isRunning.compareAndSet(false, true)) {
      Runnable entryCleaner = () -> {
        LOG.debug("Starting close container Hash map cleaner.");
        try {
          for (Map.Entry<String, Long> entry : commandIssued.entrySet()) {
            long commandQueueTime = entry.getValue();
            if (commandQueueTime + (MULTIPLIER * reportInterval) >
                TimeUnit.MILLISECONDS.toSeconds(Time.monotonicNow())) {

              // It is possible for this remove to fail due to race conditions.
              // No big deal we will cleanup next time.
              commandIssued.remove(entry.getKey());
              mapCount.decrementAndGet();
            }
          }
          isRunning.compareAndSet(true, false);
          LOG.debug("Finished running, close container Hash map cleaner.");
        } catch (Exception ex) {
          LOG.error("Unable to finish cleaning the closed containers map.", ex);
        }
      };

      // Launch the cleaner thread when we need instead of having a daemon
      // thread that is sleeping all the time. We need to set the Daemon to
      // true to avoid blocking clean exits.
      Thread cleanerThread = new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("Closed Container Cleaner Thread - %d")
          .build().newThread(entryCleaner);
      threadRunCount.incrementAndGet();
      cleanerThread.start();
    }
  }

  @VisibleForTesting
  public int getThreadRunCount() {
    return threadRunCount.get();
  }

  @VisibleForTesting
  public int getCloseCount() {
    return mapCount.get();
  }
}
