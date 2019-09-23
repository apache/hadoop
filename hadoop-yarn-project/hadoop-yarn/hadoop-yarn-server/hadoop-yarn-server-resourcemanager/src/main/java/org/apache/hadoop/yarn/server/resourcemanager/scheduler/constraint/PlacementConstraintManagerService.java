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

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;

/**
 * The service that implements the {@link PlacementConstraintManager} interface.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class PlacementConstraintManagerService extends AbstractService
    implements PlacementConstraintManager {

  protected static final Logger LOG =
      LoggerFactory.getLogger(PlacementConstraintManagerService.class);

  private PlacementConstraintManager placementConstraintManager = null;

  public PlacementConstraintManagerService() {
    super(PlacementConstraintManagerService.class.getName());
  }

  @Override
  public boolean validateConstraint(Set<String> sourceTags,
      PlacementConstraint placementConstraint) {
    if (!validateSourceTags(sourceTags)) {
      return false;
    }
    // TODO: Perform actual validation of the constraint (in YARN-6621).
    // TODO: Perform satisfiability check for constraint.
    return true;
  }

  /**
   * Validates whether the allocation tags that will enable a constraint have
   * the expected format. At the moment we support a single allocation tag per
   * constraint.
   *
   * @param sourceTags the source allocation tags
   * @return true if the tags have the expected format
   */
  protected boolean validateSourceTags(Set<String> sourceTags) {
    if (sourceTags.isEmpty()) {
      LOG.warn("A placement constraint cannot be associated with an empty "
          + "set of tags.");
      return false;
    }
    if (sourceTags.size() > 1) {
      LOG.warn("Only a single tag can be associated with a placement "
          + "constraint currently.");
      return false;
    }
    return true;
  }

  /**
   * This method will return a single allocation tag. It should be called after
   * validating the tags by calling {@link #validateSourceTags}.
   *
   * @param sourceTags the source allocation tags
   * @return the single source tag
   */
  protected String getValidSourceTag(Set<String> sourceTags) {
    return sourceTags.iterator().next();
  }

}
