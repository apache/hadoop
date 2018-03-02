/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;

/**
 * Interface for storing and retrieving placement constraints (see
 * {@link PlacementConstraint}).
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface PlacementConstraintManager {

  /**
   * Register all placement constraints of an application.
   *
   * @param appId the application ID
   * @param constraintMap the map of allocation tags to constraints for this
   *          application
   */
  void registerApplication(ApplicationId appId,
      Map<Set<String>, PlacementConstraint> constraintMap);

  /**
   * Add a placement constraint for a given application and a given set of
   * (source) allocation tags. The constraint will be used on Scheduling
   * Requests that carry this set of allocation tags.
   * TODO: Support merge and not only replace when adding a constraint.
   *
   * @param appId the application ID
   * @param sourceTags the set of allocation tags that will enable this
   *          constraint
   * @param placementConstraint the constraint
   * @param replace if true, an existing constraint for these tags will be
   *          replaced by the given one
   */
  void addConstraint(ApplicationId appId, Set<String> sourceTags,
      PlacementConstraint placementConstraint, boolean replace);

  /**
   * Add a placement constraint that will be used globally. These constraints
   * are added by the cluster administrator.
   * TODO: Support merge and not only replace when adding a constraint.
   *
   * @param sourceTags the allocation tags that will enable this constraint
   * @param placementConstraint the constraint
   * @param replace if true, an existing constraint for these tags will be
   *          replaced by the given one
   */
  void addGlobalConstraint(Set<String> sourceTags,
      PlacementConstraint placementConstraint, boolean replace);

  /**
   * Retrieve all constraints for a given application, along with the allocation
   * tags that enable each constraint.
   *
   * @param appId the application ID
   * @return the constraints for this application with the associated tags
   */
  Map<Set<String>, PlacementConstraint> getConstraints(ApplicationId appId);

  /**
   * Retrieve the placement constraint that is associated with a set of
   * allocation tags for a given application.
   *
   * @param appId the application ID
   * @param sourceTags the allocation tags that enable this constraint
   * @return the constraint
   */
  PlacementConstraint getConstraint(ApplicationId appId,
      Set<String> sourceTags);

  /**
   * Retrieve a global constraint that is associated with a given set of
   * allocation tags.
   *
   * @param sourceTags the allocation tags that enable this constraint
   * @return the constraint
   */
  PlacementConstraint getGlobalConstraint(Set<String> sourceTags);

  /**
   * Consider all levels of constraints (scheduling request, app, cluster) and
   * return a merged constraint.
   *
   * @param applicationId application ID
   * @param sourceTags a set of source allocation tags
   * @param schedulingRequestConstraint placement constraint at scheduling
   *          request level
   * @return a merged placement constraint
   */
  PlacementConstraint getMultilevelConstraint(ApplicationId applicationId,
      Set<String> sourceTags, PlacementConstraint schedulingRequestConstraint);

  /**
   * Remove the constraints that correspond to a given application.
   *
   * @param appId the application that will be removed.
   */
  void unregisterApplication(ApplicationId appId);

  /**
   * Remove a global constraint that is associated with the given allocation
   * tags.
   *
   * @param sourceTags the allocation tags
   */
  void removeGlobalConstraint(Set<String> sourceTags);

  /**
   * Returns the number of currently registered applications in the Placement
   * Constraint Manager.
   *
   * @return number of registered applications.
   */
  int getNumRegisteredApplications();

  /**
   * Returns the number of global constraints registered in the Placement
   * Constraint Manager.
   *
   * @return number of global constraints.
   */
  int getNumGlobalConstraints();

  /**
   * Validate a placement constraint and the set of allocation tags that will
   * enable it.
   *
   * @param sourceTags the associated allocation tags
   * @param placementConstraint the constraint
   * @return true if constraint and tags are valid
   */
  default boolean validateConstraint(Set<String> sourceTags,
      PlacementConstraint placementConstraint) {
    return true;
  }

}
