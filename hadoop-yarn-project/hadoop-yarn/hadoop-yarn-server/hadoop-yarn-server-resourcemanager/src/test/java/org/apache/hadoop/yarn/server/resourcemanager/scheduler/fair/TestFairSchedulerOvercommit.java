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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestSchedulerOvercommit;

/**
 * Test changing resources and overcommit in the Fair Scheduler
 * {@link FairScheduler}.
 */
public class TestFairSchedulerOvercommit extends TestSchedulerOvercommit {

  @Override
  protected Configuration getConfiguration() {
    Configuration conf = super.getConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
        FairScheduler.class, ResourceScheduler.class);

    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 0);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 10 * GB);
    conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, false);
    conf.setLong(FairSchedulerConfiguration.UPDATE_INTERVAL_MS, 10);
    conf.setFloat(FairSchedulerConfiguration.PREEMPTION_THRESHOLD, 0f);

    return conf;
  }
}
