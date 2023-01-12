/*
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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.audit.CommonAuditContext;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConfig;

import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_JOB_ID;
import static org.apache.hadoop.fs.audit.CommonAuditContext.currentAuditContext;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.CONTEXT_ATTR_STAGE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.CONTEXT_ATTR_TASK_ATTEMPT_ID;

/**
 * Helper class to support integration with Hadoop 3.3.2+ Auditing.
 * This MUST BE the sole place where fs.audit methods are used, so can be replaced
 * by a stub class on any backport.
 */
@InterfaceAudience.Private
public final class AuditingIntegration {
  private AuditingIntegration() {
  }

  /**
   * Add jobID to current context; also
   * task attempt ID if set.
   */
  public static void updateCommonContextOnCommitterEntry(
      ManifestCommitterConfig committerConfig) {
    CommonAuditContext context = currentAuditContext();
    context.put(PARAM_JOB_ID,
        committerConfig.getJobUniqueId());
    // maybe the task attempt ID.
    if (!committerConfig.getTaskAttemptId().isEmpty()) {
      context.put(CONTEXT_ATTR_TASK_ATTEMPT_ID,
          committerConfig.getTaskAttemptId());
    }
  }

  /**
   * Callback on stage entry.
   * Sets the activeStage and updates the
   * common context.
   * @param stage new stage
   */
  public static void enterStage(String stage) {
    currentAuditContext().put(CONTEXT_ATTR_STAGE, stage);
  }

  /**
   * Remove stage from common audit context.
   */
  public static void exitStage() {
    currentAuditContext().remove(CONTEXT_ATTR_STAGE);
  }

  /**
   * Remove commit info at the end of the task or job.
   */
  public static void updateCommonContextOnCommitterExit() {
    currentAuditContext().remove(PARAM_JOB_ID);
    currentAuditContext().remove(CONTEXT_ATTR_TASK_ATTEMPT_ID);
  }

  /**
   * Update the thread context with the stage name and
   * job ID.
   * This MUST be invoked at the start of methods invoked in helper threads,
   * to ensure that they are all annotated with job and stage.
   * @param jobId job ID.
   * @param stage stage name.
   */
  public static void enterStageWorker(String jobId, String stage) {
    CommonAuditContext context = currentAuditContext();
    context.put(PARAM_JOB_ID, jobId);
    context.put(CONTEXT_ATTR_STAGE, stage);
  }
}
