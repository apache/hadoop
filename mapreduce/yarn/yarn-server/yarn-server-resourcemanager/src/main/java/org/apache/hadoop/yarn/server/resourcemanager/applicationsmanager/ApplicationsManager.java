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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Application;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;

/**
 * This interface defines the interface for ApplicationsManager. This interface
 * is used by the application submission clients to call into the applications manager.
 */
@Private
@Evolving
public interface ApplicationsManager extends Recoverable {
  /**
   * Create and return a new application Id.
   * @return a new application id
   */
   ApplicationId getNewApplicationID();
   
   /**
    * Return the {@link ApplicationMaster} information for this application.
    * @param applicationId the application id of the application
    * @return the {@link ApplicationMaster} for this application
    */
   ApplicationMaster getApplicationMaster(ApplicationId applicationId);
   
   /**
    * Get the information for this application.
    * @param applicationID the applicaiton id for the application
    * @return {@link Application} information about the application.
    */
   Application getApplication(ApplicationId applicationID);
   
   /**
    * Submit the application to run on the cluster.
    * @param context the {@link ApplicationSubmissionContext} for this application.
    * @throws IOException
    */
   void submitApplication(ApplicationSubmissionContext context) throws IOException;
   
   /**
    * Api to kill the application. 
    * @param applicationId the {@link ApplicationId} to be killed.
    * @param callerUGI the {@link UserGroupInformation} of the user calling it.
    * @throws IOException
    */
   void finishApplication(ApplicationId applicationId, 
       UserGroupInformation callerUGI) throws IOException;
   
   /**
    * Get all the applications in the cluster.
    * This is used by the webUI.
    * @return the applications in the cluster.
    */
   List<AppContext> getAllApplications();
   
   /**
    * Get all the applications in the cluster. 
    * @return the list of applications in the cluster.
    */
   List<Application> getApplications();
}