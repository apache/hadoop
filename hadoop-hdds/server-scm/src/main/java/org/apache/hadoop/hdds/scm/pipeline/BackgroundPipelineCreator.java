/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.utils.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implements api for running background pipeline creation jobs.
 */
class BackgroundPipelineCreator {

  private static final Logger LOG =
      LoggerFactory.getLogger(BackgroundPipelineCreator.class);

  private final Scheduler scheduler;
  private final AtomicBoolean isPipelineCreatorRunning;
  private final PipelineManager pipelineManager;
  private final Configuration conf;

  BackgroundPipelineCreator(PipelineManager pipelineManager,
      Scheduler scheduler, Configuration conf) {
    this.pipelineManager = pipelineManager;
    this.conf = conf;
    this.scheduler = scheduler;
    isPipelineCreatorRunning = new AtomicBoolean(false);
  }

  private boolean shouldSchedulePipelineCreator() {
    return isPipelineCreatorRunning.compareAndSet(false, true);
  }

  /**
   * Schedules a fixed interval job to create pipelines.
   */
  void startFixedIntervalPipelineCreator() {
    long intervalInMillis = conf
        .getTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL,
            ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
    // TODO: #CLUTIL We can start the job asap
    scheduler.scheduleWithFixedDelay(() -> {
      if (!shouldSchedulePipelineCreator()) {
        return;
      }
      createPipelines();
    }, 0, intervalInMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * Triggers pipeline creation via background thread.
   */
  void triggerPipelineCreation() {
    // TODO: #CLUTIL introduce a better mechanism to not have more than one
    // job of a particular type running, probably via ratis.
    if (!shouldSchedulePipelineCreator()) {
      return;
    }
    scheduler.schedule(this::createPipelines, 0, TimeUnit.MILLISECONDS);
  }

  private void createPipelines() {
    // TODO: #CLUTIL Different replication factor may need to be supported
    HddsProtos.ReplicationType type = HddsProtos.ReplicationType.valueOf(
        conf.get(OzoneConfigKeys.OZONE_REPLICATION_TYPE,
            OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT));

    for (HddsProtos.ReplicationFactor factor : HddsProtos.ReplicationFactor
        .values()) {
      while (true) {
        try {
          if (scheduler.isClosed()) {
            break;
          }
          pipelineManager.createPipeline(type, factor);
        } catch (IOException ioe) {
          break;
        } catch (Throwable t) {
          LOG.error("Error while creating pipelines {}", t);
          break;
        }
      }
    }
    isPipelineCreatorRunning.set(false);
  }
}
