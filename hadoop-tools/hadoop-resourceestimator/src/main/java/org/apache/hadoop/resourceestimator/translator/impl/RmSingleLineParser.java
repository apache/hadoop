/*
 *
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
 *
 */

package org.apache.hadoop.resourceestimator.translator.impl;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.resourceestimator.translator.api.JobMetaData;
import org.apache.hadoop.resourceestimator.translator.api.SingleLineParser;
import org.apache.hadoop.resourceestimator.translator.exceptions.DataFieldNotFoundException;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * {@link SingleLineParser} for Hadoop Resource Manager logs.
 */
public class RmSingleLineParser implements SingleLineParser {
  private static final LogParserUtil PARSERUTIL = new LogParserUtil();
  private static final Pattern FILTER_PATTERN = Pattern.compile(
      "(Submit Application Request|AM Allocated Container|"
          + "AM Released Container|finalState=FAILED|"
          + "ApplicationSummary|, Resource:)");
  private static final Pattern SUBMISSION_PATTERN =
      Pattern.compile("APPID=(\\w+)");
  private static final Pattern FAIL_PATTERN =
      Pattern.compile("appattempt_(\\d+_\\d+)_\\d+");
  private static final Pattern FINISH_PATTERN =
      Pattern.compile("appId=(\\w+).*?name=(\\w+)\\-(\\w+)");
  private static final Pattern CONTAINER_EVENT_PATTERN =
      Pattern.compile("APPID=(\\w+).*?CONTAINERID=(\\w+)");
  private static final Pattern CONTAINER_SPEC_PATTERN = Pattern.compile(
      "(container_[^_]+|appattempt)_(\\d+_\\d+).*?memory:(\\d+),"
          + "\\svCores:(\\d+)");

  /**
   * Aggregates different jobs' {@link ResourceSkyline}s within the same
   * pipeline together.
   *
   * @param resourceSkyline newly extracted {@link ResourceSkyline}.
   * @param recurrenceId    the {@link RecurrenceId} which the resourceSkyline
   *                        belongs to.
   * @param skylineRecords  a {@link Map} which stores the
   *     {@link ResourceSkyline}s for all pipelines during this parsing.
   */
  private void aggregateSkyline(final ResourceSkyline resourceSkyline,
      final RecurrenceId recurrenceId,
      final Map<RecurrenceId, List<ResourceSkyline>> skylineRecords) {
    List<ResourceSkyline> resourceSkylines = skylineRecords.get(recurrenceId);
    if (resourceSkylines == null) {
      resourceSkylines = new ArrayList<ResourceSkyline>();
      skylineRecords.put(recurrenceId, resourceSkylines);
    }
    resourceSkylines.add(resourceSkyline);
  }

  @Override public final void parseLine(final String logLine,
      final Map<String, JobMetaData> jobMetas,
      final Map<RecurrenceId, List<ResourceSkyline>> skylineRecords)
      throws DataFieldNotFoundException, ParseException {
    final String[] splits = logLine.split(",", 5); // Limit the max number of 5
    // splits
    if (splits.length < 5) {
      return;
    }
    final Matcher jobEventMatcher = FILTER_PATTERN.matcher(splits[4]); // search
    // only
    // the
    // tail
    if (!jobEventMatcher.find()) { // this line of log does not contain targeted
      // events
      return;
    }

    // now we have the match, let's do some parsing
    final long date = PARSERUTIL.stringToUnixTimestamp(splits[1]);
    final String tail = splits[4].split("\\s+", 4)[3]; // use the tail of the
    // tail only
    switch (jobEventMatcher.group(1)) {
    case "Submit Application Request": {
      /** Submit job. */
      final Matcher appIdMatch = SUBMISSION_PATTERN.matcher(tail);
      if (appIdMatch.find()) {
        final String appId = appIdMatch.group(1);
        jobMetas.put(appId, new JobMetaData(date));
      } else {
        throw new DataFieldNotFoundException(tail);
      }
      break;
    }
    case "AM Allocated Container": {
      /** Allocate container. */
      final Matcher containerEventMatcher =
          CONTAINER_EVENT_PATTERN.matcher(tail);
      if (containerEventMatcher.find()) {
        final String appId = containerEventMatcher.group(1);
        final String containerId = containerEventMatcher.group(2);
        final JobMetaData appMeta = jobMetas.get(appId);
        if (appMeta != null) {
          appMeta.setContainerStart(containerId, date);
        }
      } else {
        throw new DataFieldNotFoundException(tail);
      }
      break;
    }
    case ", Resource:": {
      final Matcher containerSpecMatcher = CONTAINER_SPEC_PATTERN.matcher(tail);
      if (containerSpecMatcher.find()) {
        final String appId = "application_" + containerSpecMatcher.group(2);
        final JobMetaData appMeta = jobMetas.get(appId);
        if (appMeta != null) {
          final long memAlloc = Long.parseLong(containerSpecMatcher.group(3));
          final int cpuAlloc = Integer.parseInt(containerSpecMatcher.group(4));
          final Resource containerAlloc =
              Resource.newInstance(memAlloc, cpuAlloc);
          appMeta.getResourceSkyline().setContainerSpec(containerAlloc);
        }
      } else {
        throw new DataFieldNotFoundException(tail);
      }
      break;
    }
    case "AM Released Container": {
      final Matcher containerEventMatcher =
          CONTAINER_EVENT_PATTERN.matcher(tail);
      if (containerEventMatcher.find()) {
        final String appId = containerEventMatcher.group(1);
        final JobMetaData appMeta = jobMetas.get(appId);
        if (appMeta != null) {
          final String containerId = containerEventMatcher.group(2);
          appMeta.setContainerEnd(containerId, date);
        }
      } else {
        throw new DataFieldNotFoundException(tail);
      }
      break;
    }
    case "finalState=FAILED": {
      /** In case of appAttempt failed: discard previous records. */
      final Matcher failMatcher = FAIL_PATTERN.matcher(tail);
      if (failMatcher.find()) {
        final String appId = "application_" + failMatcher.group(1);
        if (jobMetas.containsKey(appId)) {
          jobMetas.put(appId, new JobMetaData(date));
        }
      } else {
        throw new DataFieldNotFoundException(tail);
      }
      break;
    }
    case "ApplicationSummary": {
      /** Finish a job. */
      final Matcher finishMatcher = FINISH_PATTERN.matcher(tail);
      if (finishMatcher.find()) {
        final String appId = finishMatcher.group(1);
        final String pipelineId = finishMatcher.group(2);
        final String runId = finishMatcher.group(3);
        final RecurrenceId recurrenceId = new RecurrenceId(pipelineId, runId);
        final JobMetaData appMeta = jobMetas.remove(appId);
        if (appMeta != null) {
          appMeta.setRecurrenceId(recurrenceId).setJobFinishTime(date)
              .getResourceSkyline().setJobInputDataSize(0); // TODO: need to
          // read job input
          // data size from
          // logs
          appMeta.createSkyline();
          final ResourceSkyline resourceSkyline = appMeta.getResourceSkyline();
          resourceSkyline.setJobId(appId);
          aggregateSkyline(resourceSkyline, recurrenceId, skylineRecords);
        }
      } else {
        throw new DataFieldNotFoundException(tail);
      }
      break;
    }
    default:
      break;
    }
  }
}
