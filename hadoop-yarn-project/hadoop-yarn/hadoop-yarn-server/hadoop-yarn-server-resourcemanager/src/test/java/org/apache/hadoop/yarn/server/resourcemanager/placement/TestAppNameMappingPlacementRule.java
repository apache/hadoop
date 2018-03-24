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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SimpleGroupsMapping;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class TestAppNameMappingPlacementRule {

  private static final long CLUSTER_TIMESTAMP = System.currentTimeMillis();
  public static final String APPIDSTRPREFIX = "application";
  private static final String APPLICATION_ID_PREFIX = APPIDSTRPREFIX + '_';
  private static final String APPLICATION_ID_SUFFIX = '_' + "0001";
  private static final String CLUSTER_APP_ID = APPLICATION_ID_PREFIX +
      CLUSTER_TIMESTAMP + APPLICATION_ID_SUFFIX;

  private YarnConfiguration conf = new YarnConfiguration();

  @Before
  public void setup() {
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
  }

  private void verifyQueueMapping(QueueMappingEntity queueMapping,
      String inputAppId, String expectedQueue) throws YarnException {
    verifyQueueMapping(queueMapping, inputAppId,
        YarnConfiguration.DEFAULT_QUEUE_NAME, expectedQueue, false);
  }

  private void verifyQueueMapping(QueueMappingEntity queueMapping,
      String inputAppId, String inputQueue, String expectedQueue,
      boolean overwrite) throws YarnException {
    AppNameMappingPlacementRule rule = new AppNameMappingPlacementRule(
        overwrite, Arrays.asList(queueMapping));
    ApplicationSubmissionContext asc = Records.newRecord(
        ApplicationSubmissionContext.class);
    asc.setQueue(inputQueue);
    ApplicationId appId = ApplicationId.newInstance(CLUSTER_TIMESTAMP,
        Integer.parseInt(inputAppId));
    asc.setApplicationId(appId);
    ApplicationPlacementContext ctx = rule.getPlacementForApp(asc,
        queueMapping.getSource());
    Assert.assertEquals(expectedQueue,
        ctx != null ? ctx.getQueue() : inputQueue);
  }

  @Test
  public void testMapping() throws YarnException {
    // simple base case for mapping user to queue
    verifyQueueMapping(new QueueMappingEntity(CLUSTER_APP_ID,
        "q1"), "1", "q1");
    verifyQueueMapping(new QueueMappingEntity("%application", "q2"), "1", "q2");
    verifyQueueMapping(new QueueMappingEntity("%application", "%application"),
        "1", CLUSTER_APP_ID);

    // specify overwritten, and see if user specified a queue, and it will be
    // overridden
    verifyQueueMapping(new QueueMappingEntity(CLUSTER_APP_ID,
        "q1"), "1", "q2", "q1", true);

    // if overwritten not specified, it should be which user specified
    verifyQueueMapping(new QueueMappingEntity(CLUSTER_APP_ID,
            "q1"), "1", "q2", "q2", false);
  }
}