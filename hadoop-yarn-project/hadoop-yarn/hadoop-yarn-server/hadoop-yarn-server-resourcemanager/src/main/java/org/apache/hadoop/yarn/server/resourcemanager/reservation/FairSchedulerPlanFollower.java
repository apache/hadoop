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

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FairSchedulerPlanFollower extends AbstractSchedulerPlanFollower {
  private static final Logger LOG = LoggerFactory
      .getLogger(FairSchedulerPlanFollower.class);

  private FairScheduler fs;

  @Override
  public void init(Clock clock, ResourceScheduler sched,
      Collection<Plan> plans) {
    super.init(clock, sched, plans);
    fs = (FairScheduler)sched;
    LOG.info("Initializing Plan Follower Policy:"
        + this.getClass().getCanonicalName());
  }

  @Override
  protected Queue getPlanQueue(String planQueueName) {
    Queue planQueue = fs.getQueueManager().getParentQueue(planQueueName, false);
    if (planQueue == null) {
      LOG.error("The queue " + planQueueName + " cannot be found or is not a " +
          "ParentQueue");
    }
    return planQueue;
  }

  @Override
  protected List<? extends Queue> getChildReservationQueues(Queue queue) {
    FSQueue planQueue = (FSQueue)queue;
    List<FSQueue> childQueues = planQueue.getChildQueues();
    return childQueues;
  }


  @Override
  protected void addReservationQueue(String planQueueName, Queue queue,
      String currResId) {
    String leafQueueName = getReservationQueueName(planQueueName, currResId);
    fs.getQueueManager().getLeafQueue(leafQueueName, true);
  }

  @Override
  protected void createDefaultReservationQueue(String planQueueName,
      Queue queue, String defReservationId) {
    String defReservationQueueName = getReservationQueueName(planQueueName,
        defReservationId);
    if (!fs.getQueueManager().exists(defReservationQueueName)) {
      fs.getQueueManager().getLeafQueue(defReservationQueueName, true);
    }
  }

  @Override
  protected Resource getPlanResources(Plan plan, Queue queue,
      Resource clusterResources) {
    FSParentQueue planQueue = (FSParentQueue)queue;
    Resource planResources = planQueue.getSteadyFairShare();
    return planResources;
  }

  @Override
  protected Resource getReservationQueueResourceIfExists(Plan plan,
      ReservationId reservationId) {
    String reservationQueueName = getReservationQueueName(plan.getQueueName(),
        reservationId.toString());
    FSLeafQueue reservationQueue =
        fs.getQueueManager().getLeafQueue(reservationQueueName, false);
    Resource reservationResource = null;
    if (reservationQueue != null) {
      reservationResource = reservationQueue.getSteadyFairShare();
    }
    return reservationResource;
  }

  @Override
  protected String getReservationQueueName(String planQueueName,
      String reservationQueueName) {
    String planQueueNameFullPath = fs.getQueueManager().getQueue
        (planQueueName).getName();

    if (!reservationQueueName.startsWith(planQueueNameFullPath)) {
      // If name is not a path we need full path for FairScheduler. See
      // YARN-2773 for the root cause
      return planQueueNameFullPath + "." + reservationQueueName;
    }
    return reservationQueueName;
  }

  @Override
  protected String getReservationIdFromQueueName(String resQueueName) {
    return resQueueName.substring(resQueueName.lastIndexOf(".") + 1);
  }
}
