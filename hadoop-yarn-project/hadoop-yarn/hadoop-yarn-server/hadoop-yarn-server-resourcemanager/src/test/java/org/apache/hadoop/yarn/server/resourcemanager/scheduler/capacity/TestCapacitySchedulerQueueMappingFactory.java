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

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMappingEntity;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserGroupMappingPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerAutoCreatedQueueBase.getQueueMapping;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerAutoCreatedQueueBase.setupQueueConfiguration;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

public class TestCapacitySchedulerQueueMappingFactory {

  private static final String QUEUE_MAPPING_NAME = "app-name";
  private static final String QUEUE_MAPPING_RULE_APP_NAME =
      "org.apache.hadoop.yarn.server.resourcemanager.placement.AppNameMappingPlacementRule";
  private static final String QUEUE_MAPPING_RULE_USER_GROUP =
      "org.apache.hadoop.yarn.server.resourcemanager.placement.UserGroupMappingPlacementRule";
  public static final String USER = "user_";
  public static final String PARENT_QUEUE = "c";

  private MockRM mockRM = null;

  public static CapacitySchedulerConfiguration setupQueueMappingsForRules(
      CapacitySchedulerConfiguration conf, String parentQueue,
      boolean overrideWithQueueMappings, int[] sourceIds) {

    List<String> queuePlacementRules = new ArrayList<>();

    queuePlacementRules.add(QUEUE_MAPPING_RULE_USER_GROUP);
    queuePlacementRules.add(QUEUE_MAPPING_RULE_APP_NAME);

    conf.setQueuePlacementRules(queuePlacementRules);

    List<UserGroupMappingPlacementRule.QueueMapping> existingMappingsForUG =
        conf.getQueueMappings();

    //set queue mapping
    List<UserGroupMappingPlacementRule.QueueMapping> queueMappingsForUG =
        new ArrayList<>();
    for (int i = 0; i < sourceIds.length; i++) {
      //Set C as parent queue name for auto queue creation
      UserGroupMappingPlacementRule.QueueMapping userQueueMapping =
          new UserGroupMappingPlacementRule.QueueMapping(
              UserGroupMappingPlacementRule.QueueMapping.MappingType.USER,
              USER + sourceIds[i],
              getQueueMapping(parentQueue, USER + sourceIds[i]));
      queueMappingsForUG.add(userQueueMapping);
    }

    existingMappingsForUG.addAll(queueMappingsForUG);
    conf.setQueueMappings(existingMappingsForUG);

    List<QueueMappingEntity> existingMappingsForAN =
        conf.getQueueMappingEntity(QUEUE_MAPPING_NAME);

    //set queue mapping
    List<QueueMappingEntity> queueMappingsForAN =
        new ArrayList<>();
    for (int i = 0; i < sourceIds.length; i++) {
      //Set C as parent queue name for auto queue creation
      QueueMappingEntity queueMapping =
          new QueueMappingEntity(USER + sourceIds[i],
              getQueueMapping(parentQueue, USER + sourceIds[i]));
      queueMappingsForAN.add(queueMapping);
    }

    existingMappingsForAN.addAll(queueMappingsForAN);
    conf.setQueueMappingEntities(existingMappingsForAN, QUEUE_MAPPING_NAME);
    //override with queue mappings
    conf.setOverrideWithQueueMappings(overrideWithQueueMappings);
    return conf;
  }

  @Test
  public void testUpdatePlacementRulesFactory() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    // init queue mapping for UserGroupMappingRule and AppNameMappingRule
    setupQueueMappingsForRules(conf, PARENT_QUEUE, true, new int[] {1, 2, 3});

    mockRM = new MockRM(conf);
    CapacityScheduler cs = (CapacityScheduler) mockRM.getResourceScheduler();
    cs.updatePlacementRules();
    mockRM.start();
    cs.start();

    List<PlacementRule> rules = cs.getRMContext()
        .getQueuePlacementManager().getPlacementRules();

    List<String> placementRuleNames = new ArrayList<>();
    for (PlacementRule pr : rules) {
      placementRuleNames.add(pr.getName());
    }

    // verify both placement rules were added successfully
    assertThat(placementRuleNames, hasItems(QUEUE_MAPPING_RULE_USER_GROUP));
    assertThat(placementRuleNames, hasItems(QUEUE_MAPPING_RULE_APP_NAME));
  }
}
