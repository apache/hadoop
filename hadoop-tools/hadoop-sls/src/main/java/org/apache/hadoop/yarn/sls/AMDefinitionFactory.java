/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.sls;

import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.synthetic.SynthJob;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class AMDefinitionFactory {
  private static final Logger LOG = LoggerFactory.getLogger(
      AMDefinitionFactory.class);
  public final static String DEFAULT_JOB_TYPE = "mapreduce";

  private AMDefinitionFactory() {}

  public static AMDefinitionSLS createFromSlsTrace(Map<String, String> jsonJob,
      SLSRunner slsRunner) throws YarnException {
    AMDefinitionSLS amDefinition = AMDefinitionSLS.Builder.create(jsonJob)
        .withAmType(SLSConfiguration.AM_TYPE)
        .withAmResource(getAMContainerResourceSLS(jsonJob, slsRunner))
        .withTaskContainers(
            AMDefinitionSLS.getTaskContainers(jsonJob, slsRunner))
        .withJobStartTime(SLSConfiguration.JOB_START_MS)
        .withJobFinishTime(SLSConfiguration.JOB_END_MS)
        .withLabelExpression(SLSConfiguration.JOB_LABEL_EXPR)
        .withUser(SLSConfiguration.JOB_USER)
        .withQueue(SLSConfiguration.JOB_QUEUE_NAME)
        .withJobId(SLSConfiguration.JOB_ID)
        .withJobCount(SLSConfiguration.JOB_COUNT)
        .build();
    slsRunner.increaseQueueAppNum(amDefinition.getQueue());
    return amDefinition;
  }

  public static AMDefinitionRumen createFromRumenTrace(LoggedJob job,
      long baselineTimeMs, SLSRunner slsRunner) throws YarnException {
    AMDefinitionRumen amDefinition = AMDefinitionRumen.Builder.create()
        .withAmType(DEFAULT_JOB_TYPE)
        .withAmResource(getAMContainerResourceSynthAndRumen(slsRunner))
        .withTaskContainers(
            AMDefinitionRumen.getTaskContainers(job, slsRunner))
        .withJobStartTime(job.getSubmitTime())
        .withJobFinishTime(job.getFinishTime())
        .withBaseLineTimeMs(baselineTimeMs)
        .withUser(job.getUser())
        .withQueue(job.getQueue().getValue())
        .withJobId(job.getJobID().toString())
        .build();
    slsRunner.increaseQueueAppNum(amDefinition.getQueue());
    return amDefinition;
  }

  public static AMDefinitionSynth createFromSynth(SynthJob job,
      SLSRunner slsRunner) throws YarnException {
    AMDefinitionSynth amDefinition =
        AMDefinitionSynth.Builder.create()
            .withAmType(job.getType())
            .withAmResource(getAMContainerResourceSynthAndRumen(slsRunner))
            .withTaskContainers(
                AMDefinitionSynth.getTaskContainers(job, slsRunner))
            .withUser(job.getUser())
            .withQueue(job.getQueueName())
            .withJobId(job.getJobID().toString())
            .withJobStartTime(job.getSubmissionTime())
            .withJobFinishTime(job.getSubmissionTime() + job.getDuration())
            .withBaseLineTimeMs(0)
            .build();

    slsRunner.increaseQueueAppNum(amDefinition.getQueue());
    return amDefinition;
  }

  private static Resource getAMContainerResourceSLS(Map<String, String> jsonJob,
      Configured configured) {
    Resource amContainerResource =
        SLSConfiguration.getAMContainerResource(configured.getConf());
    if (jsonJob == null) {
      return amContainerResource;
    }

    ResourceInformation[] infors = ResourceUtils.getResourceTypesArray();
    for (ResourceInformation info : infors) {
      String key = SLSConfiguration.JOB_AM_PREFIX + info.getName();
      if (jsonJob.containsKey(key)) {
        long value = Long.parseLong(jsonJob.get(key));
        amContainerResource.setResourceValue(info.getName(), value);
      }
    }

    return amContainerResource;
  }

  private static Resource getAMContainerResourceSynthAndRumen(
      Configured configured) {
    return SLSConfiguration.getAMContainerResource(configured.getConf());
  }

  static void adjustTimeValuesToBaselineTime(AMDefinition amDef,
      AMDefinition.AmDefinitionBuilder builder, long baselineTimeMs) {
    builder.jobStartTime -= baselineTimeMs;
    builder.jobFinishTime -= baselineTimeMs;
    if (builder.jobStartTime < 0) {
      LOG.warn("Warning: reset job {} start time to 0.", amDef.getOldAppId());
      builder.jobFinishTime = builder.jobFinishTime - builder.jobStartTime;
      builder.jobStartTime = 0;
    }
    amDef.jobStartTime = builder.jobStartTime;
  }
}
