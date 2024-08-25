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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.task.TaskDescriptions;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.hs.ConfigureAware;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.app.SimpleAppInfo;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

public class TaskDescriptionsFetcher {

  private static final Log LOG = LogFactory.getLog(TaskDescriptionsFetcher.class);

  private static final String RM_APPS_PATH = "/ws/v1/cluster/apps/";
  private static final String AM_TASK_DESCRIPTIONS_PATH = "/ws/v1/mapreduce/jobs/";

  private final Configuration config;

  private final RestClient restClient;

  public TaskDescriptionsFetcher(HistoryContext ctx, RestClient restClient) {
    config = ((ConfigureAware) ctx).getConfig();
    this.restClient = restClient;
  }

  public TaskDescriptions fetch(JobId jobId) {
    TaskDescriptions taskDescriptions = null;
    // Query AM tracking URL from RM
    String rmWebAppUrl = WebAppUtils.getRMWebAppURLWithScheme(config);
    LOG.debug("RMWebAppURL: " + rmWebAppUrl);
    ApplicationId appId = jobId.getAppId();
    SimpleAppInfo app;
    try {
      app = restClient.fetchAs(rmWebAppUrl + RM_APPS_PATH + appId.toString() + "/info",
          SimpleAppInfo.class);

      if (app != null && app.getTrackingUrl() != null) {
        // Get TaskDescriptions from AM
        String taskUrl = buildMRAppMasterWebServiceUrl(app.getTrackingUrl(), jobId.toString());
        LOG.debug("AMTaskDescriptionURL: " + taskUrl);
        taskDescriptions = restClient.fetchAs(taskUrl, TaskDescriptions.class);
      } else {
        LOG.debug("App for " + jobId + " is not ready");
      }
    } catch (NotFoundException e) {
      taskDescriptions = new TaskDescriptions();
      taskDescriptions.setSuccessful(true);
      taskDescriptions.setFound(false);
      LOG.info("App for " + jobId + " is not found");
    }

    return taskDescriptions;
  }

  protected String buildMRAppMasterWebServiceUrl(String trackUrl, String jobId) {
    if (trackUrl.endsWith("/")) {
      return trackUrl.substring(0, trackUrl.length() - 1) + AM_TASK_DESCRIPTIONS_PATH + jobId
          + "/taskDescriptions";
    }
    return trackUrl + AM_TASK_DESCRIPTIONS_PATH + jobId + "/taskDescriptions";
  }
}
