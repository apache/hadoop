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
package org.apache.hadoop.yarn.sls.scheduler;

import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.codahale.metrics.MetricRegistry;

@Private
@Unstable
public interface SchedulerWrapper {

	public MetricRegistry getMetrics();
	public SchedulerMetrics getSchedulerMetrics();
	public Set<String> getQueueSet();
	public void setQueueSet(Set<String> queues);
	public Set<String> getTrackedAppSet();
	public void setTrackedAppSet(Set<String> apps);
	public void addTrackedApp(ApplicationAttemptId appAttemptId,
              String oldAppId);
	public void removeTrackedApp(ApplicationAttemptId appAttemptId,
                 String oldAppId);
	public void addAMRuntime(ApplicationId appId,
              long traceStartTimeMS, long traceEndTimeMS,
              long simulateStartTimeMS, long simulateEndTimeMS);

}
