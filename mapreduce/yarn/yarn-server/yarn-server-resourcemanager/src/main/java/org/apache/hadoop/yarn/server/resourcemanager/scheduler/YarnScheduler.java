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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.io.IOException;
import java.util.List;


import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;

/**
 * This interface is used by the components to talk to the
 * scheduler for allocating of resources, cleaning up resources.
 *
 */
public interface YarnScheduler {
  /**
   * Allocates and returns resources.
   * @param applicationId
   * @param ask
   * @param release
   * @return the scheduler's {@link Allocation} response 
   * @throws IOException
   */
  Allocation allocate(ApplicationId applicationId,
      List<ResourceRequest> ask, List<Container> release)
  throws IOException;
  
  /**
   * A new application has been submitted to the ResourceManager
   * @param applicationId application which has been submitted
   * @param master the application master
   * @param user application user
   * @param queue queue to which the applications is being submitte
   * @param priority application priority
   * @param appStore the storage for the application.
   */
  public void addApplication(ApplicationId applicationId, ApplicationMaster master,
      String user, String queue, Priority priority, ApplicationStore appStore) 
  throws IOException;
  
  /**
   * A submitted application has completed.
   * @param applicationId completed application
   * @param finishApplication true if the application is completed and the
   * scheduler needs to notify other components of application completion.
   */
  public void doneApplication(ApplicationId applicationId, boolean finishApplication)
  throws IOException;


  /**
   * Get queue information
   * @param queueName queue name
   * @param includeApplications include applications?
   * @param includeChildQueues include child queues?
   * @param recursive get children queues?
   * @return queue information
   * @throws IOException
   */
  public QueueInfo getQueueInfo(String queueName, boolean includeApplications, 
      boolean includeChildQueues, boolean recursive) 
  throws IOException;

  /**
   * Get acls for queues for current user.
   * @return acls for queues for current user
   * @throws IOException
   */
  public List<QueueUserACLInfo> getQueueUserAclInfo();
  
  /**
   * Get minimum allocatable {@link Resource}.
   * @return minimum allocatable resource
   */
  public Resource getMinimumResourceCapability();
  
  /**
   * Get maximum allocatable {@link Resource}.
   * @return maximum allocatable resource
   */
  public Resource getMaximumResourceCapability();

}
