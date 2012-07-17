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

import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * A queue containing several applications.
 */
@Private
@Unstable
public class FSQueue {
  /** Queue name. */
  private String name;

  /** Applications in this specific queue; does not include children queues' jobs. */
  private Collection<FSSchedulerApp> applications = 
      new ArrayList<FSSchedulerApp>();

  /** Scheduling mode for jobs inside the queue (fair or FIFO) */
  private SchedulingMode schedulingMode;

  private FairScheduler scheduler;

  private FSQueueSchedulable queueSchedulable;

  public FSQueue(FairScheduler scheduler, String name) {
    this.name = name;
    this.queueSchedulable = new FSQueueSchedulable(scheduler, this);
    this.scheduler = scheduler;
  }

  public Collection<FSSchedulerApp> getApplications() {
    return applications;
  }

  public void addApp(FSSchedulerApp app) {
    applications.add(app);
    queueSchedulable.addApp(new AppSchedulable(scheduler, app, this));
  }

  public void removeJob(FSSchedulerApp app) {
    applications.remove(app);
    queueSchedulable.removeApp(app);
  }

  public String getName() {
    return name;
  }

  public SchedulingMode getSchedulingMode() {
    return schedulingMode;
  }

  public void setSchedulingMode(SchedulingMode schedulingMode) {
    this.schedulingMode = schedulingMode;
  }

  public FSQueueSchedulable getQueueSchedulable() {
    return queueSchedulable;
  }
}
