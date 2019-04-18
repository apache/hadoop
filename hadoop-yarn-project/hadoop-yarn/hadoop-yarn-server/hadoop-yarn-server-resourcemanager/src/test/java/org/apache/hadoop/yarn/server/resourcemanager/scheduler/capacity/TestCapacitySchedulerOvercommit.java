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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestSchedulerOvercommit;

/**
 * Test changing resources and overcommit in the Capacity Scheduler
 * {@link CapacityScheduler}.
 */
public class TestCapacitySchedulerOvercommit extends TestSchedulerOvercommit {

  @Override
  protected Configuration getConfiguration() {
    Configuration conf = super.getConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class, ResourceScheduler.class);

    // Remove limits on AMs to allow multiple applications running
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    csConf.setMaximumApplicationMasterResourcePerQueuePercent(
        CapacitySchedulerConfiguration.ROOT, 100.0f);
    csConf.setMaximumAMResourcePercentPerPartition(
        CapacitySchedulerConfiguration.ROOT, "", 100.0f);
    csConf.setMaximumApplicationMasterResourcePerQueuePercent(
        CapacitySchedulerConfiguration.ROOT + ".default", 100.0f);
    csConf.setMaximumAMResourcePercentPerPartition(
        CapacitySchedulerConfiguration.ROOT + ".default", "", 100.0f);

    return csConf;
  }
}