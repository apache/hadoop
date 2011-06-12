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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;

/** 
 * The context of an application. 
 *
 */
public interface AppContext {
  
  /**
   * the application submission context for this application.
   * @return the {@link XApplicationSubmissionContext} for the submitted
   * application.
   */
  public ApplicationSubmissionContext getSubmissionContext();
  
  /**
   *  get the resource required for the application master.
   * @return the resource requirements of the application master
   */
  public Resource getResource();
  
  /**
   * get the application ID for this application
   * @return the application id for this application
   */
  public ApplicationId getApplicationID(); 
  
  /**
   * get the status of the application
   * @return the {@link XApplicationStatus} of this application
   */
  public ApplicationStatus getStatus();
  
  /**
   * the application master for this application.
   * @return the {@link XApplicationMaster} for this application
   */
  public ApplicationMaster getMaster();
  
  /**
   * the container on which the application master is running.
   * @return the container for running the application master.
   */
  public Container getMasterContainer();
  
  /**
   * the user for this application
   * @return the user for this application
   */
  public String getUser();
  
  /**
   * The last time the RM heard from this application
   * @return the last time RM heard from this application.
   */
  public long getLastSeen();  
  
  /**
   * the name for this application
   * @return the application name.
   */
  public String getName();
  
  /**
   * The queue of this application.
   * @return the queue for this application
   */
  public String getQueue();
  
  /**
   * the count of number of times the AM has expired/failed.
   * @return the count of number of times the AM has expired/failed.
   */
  public int getFailedCount();
  
  /**
   * The store for this application
   * @return the application store for this application
   */
  public ApplicationStore getStore();
}