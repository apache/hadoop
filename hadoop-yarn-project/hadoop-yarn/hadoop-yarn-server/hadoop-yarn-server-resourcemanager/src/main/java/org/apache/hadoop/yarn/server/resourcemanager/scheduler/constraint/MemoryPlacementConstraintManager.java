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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In memory implementation of the {@link PlacementConstraintManagerService}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MemoryPlacementConstraintManager
    extends PlacementConstraintManagerService {

  private static final Logger LOG =
      LoggerFactory.getLogger(MemoryPlacementConstraintManager.class);

  private ReentrantReadWriteLock.ReadLock readLock;
  private ReentrantReadWriteLock.WriteLock writeLock;

  /**
   * Stores the global constraints that will be manipulated by the cluster
   * admin. The key of each entry is the tag that will enable the corresponding
   * constraint.
   */
  private Map<String, PlacementConstraint> globalConstraints;
  /**
   * Stores the constraints for each application, along with the allocation tags
   * that will enable each of the constraints for a given application.
   */
  private Map<ApplicationId, Map<String, PlacementConstraint>> appConstraints;

  public MemoryPlacementConstraintManager() {
    this.globalConstraints = new HashMap<>();
    this.appConstraints = new HashMap<>();
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  public void registerApplication(ApplicationId appId,
      Map<Set<String>, PlacementConstraint> constraintMap) {
    // Check if app already exists. If not, prepare its constraint map.
    Map<String, PlacementConstraint> constraintsForApp = new HashMap<>();
    readLock.lock();
    try {
      if (appConstraints.get(appId) != null) {
        LOG.warn("Application {} has already been registered.", appId);
        return;
      }
      // Go over each sourceTag-constraint pair, validate it, and add it to the
      // constraint map for this app.
      for (Map.Entry<Set<String>, PlacementConstraint> entry : constraintMap
          .entrySet()) {
        Set<String> sourceTags = entry.getKey();
        PlacementConstraint constraint = entry.getValue();
        if (validateConstraint(sourceTags, constraint)) {
          String sourceTag = getValidSourceTag(sourceTags);
          constraintsForApp.put(sourceTag, constraint);
        }
      }
    } finally {
      readLock.unlock();
    }

    if (constraintsForApp.isEmpty()) {
      LOG.info("Application {} was registered, but no constraints were added.",
          appId);
    }
    // Update appConstraints.
    writeLock.lock();
    try {
      appConstraints.put(appId, constraintsForApp);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void addConstraint(ApplicationId appId, Set<String> sourceTags,
      PlacementConstraint placementConstraint, boolean replace) {
    writeLock.lock();
    try {
      Map<String, PlacementConstraint> constraintsForApp =
          appConstraints.get(appId);
      if (constraintsForApp == null) {
        LOG.info("Cannot add constraint to application {}, as it has not "
            + "been registered yet.", appId);
        return;
      }

      addConstraintToMap(constraintsForApp, sourceTags, placementConstraint,
          replace);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void addGlobalConstraint(Set<String> sourceTags,
      PlacementConstraint placementConstraint, boolean replace) {
    writeLock.lock();
    try {
      addConstraintToMap(globalConstraints, sourceTags, placementConstraint,
          replace);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Helper method that adds a constraint to a map for a given source tag.
   * Assumes there is already a lock on the constraint map.
   *
   * @param constraintMap constraint map to which the constraint will be added
   * @param sourceTags the source tags that will enable this constraint
   * @param placementConstraint the new constraint to be added
   * @param replace if true, an existing constraint for these sourceTags will be
   *          replaced with the new one
   */
  private void addConstraintToMap(
      Map<String, PlacementConstraint> constraintMap, Set<String> sourceTags,
      PlacementConstraint placementConstraint, boolean replace) {
    if (validateConstraint(sourceTags, placementConstraint)) {
      String sourceTag = getValidSourceTag(sourceTags);
      if (constraintMap.get(sourceTag) == null || replace) {
        if (replace) {
          LOG.info("Replacing the constraint associated with tag {} with {}.",
              sourceTag, placementConstraint);
        }
        constraintMap.put(sourceTag, placementConstraint);
      } else {
        LOG.info("Constraint {} will not be added. There is already a "
                + "constraint associated with tag {}.",
            placementConstraint, sourceTag);
      }
    }
  }

  @Override
  public Map<Set<String>, PlacementConstraint> getConstraints(
      ApplicationId appId) {
    readLock.lock();
    try {
      if (appConstraints.get(appId) == null) {
        LOG.debug("Application {} is not registered in the Placement "
            + "Constraint Manager.", appId);
        return null;
      }

      // Copy to a new map and return an unmodifiable version of it.
      // Each key of the map is a set with a single source tag.
      Map<Set<String>, PlacementConstraint> constraintMap =
          appConstraints.get(appId).entrySet().stream()
              .collect(Collectors.toMap(
                  e -> Stream.of(e.getKey()).collect(Collectors.toSet()),
                  e -> e.getValue()));

      return Collections.unmodifiableMap(constraintMap);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public PlacementConstraint getConstraint(ApplicationId appId,
      Set<String> sourceTags) {
    if (!validateSourceTags(sourceTags)) {
      return null;
    }
    String sourceTag = getValidSourceTag(sourceTags);
    readLock.lock();
    try {
      if (appConstraints.get(appId) == null) {
        LOG.debug("Application {} is not registered in the Placement "
            + "Constraint Manager.", appId);
        return null;
      }
      // TODO: Merge this constraint with the global one for this tag, if one
      // exists.
      return appConstraints.get(appId).get(sourceTag);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public PlacementConstraint getGlobalConstraint(Set<String> sourceTags) {
    if (!validateSourceTags(sourceTags)) {
      return null;
    }
    String sourceTag = getValidSourceTag(sourceTags);
    readLock.lock();
    try {
      return globalConstraints.get(sourceTag);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public PlacementConstraint getMultilevelConstraint(ApplicationId appId,
      Set<String> sourceTags, PlacementConstraint schedulingRequestConstraint) {
    List<PlacementConstraint> constraints = new ArrayList<>();
    // Add scheduling request-level constraint.
    if (schedulingRequestConstraint != null) {
      constraints.add(schedulingRequestConstraint);
    }
    // Add app-level constraint if appId is given.
    if (appId != null && sourceTags != null
        && !sourceTags.isEmpty()) {
      constraints.add(getConstraint(appId, sourceTags));
    }
    // Add global constraint.
    if (sourceTags != null && !sourceTags.isEmpty()) {
      constraints.add(getGlobalConstraint(sourceTags));
    }

    // Remove all null or duplicate constraints.
    List<PlacementConstraint.AbstractConstraint> allConstraints =
        constraints.stream()
            .filter(placementConstraint -> placementConstraint != null
            && placementConstraint.getConstraintExpr() != null)
            .map(PlacementConstraint::getConstraintExpr)
            .distinct()
            .collect(Collectors.toList());

    // Compose an AND constraint
    // When merge request(RC), app(AC) and global constraint(GC),
    // we do a merge on them with CC=AND(GC, AC, RC) and returns a
    // composite AND constraint. Subsequently we check if CC could
    // be satisfied. This ensures that every level of constraint
    // is satisfied.
    PlacementConstraint.And andConstraint = PlacementConstraints.and(
        allConstraints.toArray(new PlacementConstraint
            .AbstractConstraint[allConstraints.size()]));
    return andConstraint.build();
  }

  @Override
  public void unregisterApplication(ApplicationId appId) {
    writeLock.lock();
    try {
      appConstraints.remove(appId);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void removeGlobalConstraint(Set<String> sourceTags) {
    if (!validateSourceTags(sourceTags)) {
      return;
    }
    String sourceTag = getValidSourceTag(sourceTags);
    writeLock.lock();
    try {
      globalConstraints.remove(sourceTag);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public int getNumRegisteredApplications() {
    readLock.lock();
    try {
      return appConstraints.size();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public int getNumGlobalConstraints() {
    readLock.lock();
    try {
      return globalConstraints.size();
    } finally {
      readLock.unlock();
    }
  }
}
