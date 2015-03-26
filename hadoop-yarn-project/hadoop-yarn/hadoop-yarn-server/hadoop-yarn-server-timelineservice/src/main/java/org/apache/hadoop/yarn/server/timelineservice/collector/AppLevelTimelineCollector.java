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

package org.apache.hadoop.yarn.server.timelineservice.collector;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

/**
 * Service that handles writes to the timeline service and writes them to the
 * backing storage for a given YARN application.
 *
 * App-related lifecycle management is handled by this service.
 */
@Private
@Unstable
public class AppLevelTimelineCollector extends TimelineCollector {
  private final ApplicationId appId;
  private final TimelineCollectorContext context;

  public AppLevelTimelineCollector(ApplicationId appId) {
    super(AppLevelTimelineCollector.class.getName() + " - " + appId.toString());
    Preconditions.checkNotNull(appId, "AppId shouldn't be null");
    this.appId = appId;
    context = new TimelineCollectorContext();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    context.setClusterId(conf.get(YarnConfiguration.RM_CLUSTER_ID,
        YarnConfiguration.DEFAULT_RM_CLUSTER_ID));
    // Set the default values, which will be updated with an RPC call to get the
    // context info from NM.
    // Current user usually is not the app user, but keep this field non-null
    context.setUserId(UserGroupInformation.getCurrentUser().getShortUserName());
    // Use app ID to generate a default flow ID for orphan app
    context.setFlowId(TimelineUtils.generateDefaultFlowIdBasedOnAppId(appId));
    // Set the flow run ID to 0 if it's an orphan app
    context.setFlowRunId("0");
    context.setAppId(appId.toString());
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
  }

  @Override
  protected TimelineCollectorContext getTimelineEntityContext() {
    return context;
  }

}
