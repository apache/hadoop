/*
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
package org.apache.hadoop.yarn.api.records.timelineservice;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * This class extends timeline entity and defines parent-child relationships
 * with other entities.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class HierarchicalTimelineEntity extends TimelineEntity {
  public static final String PARENT_INFO_KEY =
      TimelineEntity.SYSTEM_INFO_KEY_PREFIX + "PARENT_ENTITY";
  public static final String CHILDREN_INFO_KEY =
      TimelineEntity.SYSTEM_INFO_KEY_PREFIX + "CHILDREN_ENTITY";

  HierarchicalTimelineEntity(TimelineEntity entity) {
    super(entity);
  }

  HierarchicalTimelineEntity(String type) {
    super(type);
  }

  public Identifier getParent() {
    Object obj = getInfo().get(PARENT_INFO_KEY);
    if (obj != null) {
      if (obj instanceof Identifier) {
        return (Identifier) obj;
      } else {
        throw new YarnRuntimeException(
            "Parent info is invalid identifier object");
      }
    }
    return null;
  }

  public void setParent(Identifier parent) {
    validateParent(parent.getType());
    addInfo(PARENT_INFO_KEY, parent);
  }

  public void setParent(String type, String id) {
    setParent(new Identifier(type, id));
  }

  @SuppressWarnings("unchecked")
  public Set<Identifier> getChildren() {
    Object identifiers = getInfo().get(CHILDREN_INFO_KEY);
    if (identifiers == null) {
      return new HashSet<>();
    }
    TimelineEntityType thisType = TimelineEntityType.valueOf(getType());
    if (identifiers instanceof Set<?>) {
      for (Object identifier : (Set<?>) identifiers) {
        if (!(identifier instanceof Identifier)) {
          throw new YarnRuntimeException(
              "Children info contains invalid identifier object");
        } else {
          validateChild((Identifier) identifier, thisType);
        }
      }
    } else {
      throw new YarnRuntimeException(
          "Children info is invalid identifier set");
    }
    Set<Identifier> children = (Set<Identifier>) identifiers;
    return children;
  }

  public void setChildren(Set<Identifier> children) {
    addInfo(CHILDREN_INFO_KEY, children);
  }

  public void addChildren(Set<Identifier> children) {
    TimelineEntityType thisType = TimelineEntityType.valueOf(getType());
    for (Identifier child : children) {
      validateChild(child, thisType);
    }
    Set<Identifier> existingChildren = getChildren();
    existingChildren.addAll(children);
    setChildren(existingChildren);
  }

  public void addChild(Identifier child) {
    addChildren(Collections.singleton(child));
  }

  public void addChild(String type, String id) {
    addChild(new Identifier(type, id));
  }

  private void validateParent(String type) {
    TimelineEntityType parentType = TimelineEntityType.valueOf(type);
    TimelineEntityType thisType = TimelineEntityType.valueOf(getType());
    if (!thisType.isParent(parentType)) {
      throw new IllegalArgumentException(
          type + " is not the acceptable parent of " + this.getType());
    }
  }

  private void validateChild(Identifier child, TimelineEntityType thisType) {
    TimelineEntityType childType = TimelineEntityType.valueOf(child.getType());
    if (!thisType.isChild(childType)) {
      throw new IllegalArgumentException(
          child.getType() + " is not the acceptable child of " +
              this.getType());
    }
  }
}
