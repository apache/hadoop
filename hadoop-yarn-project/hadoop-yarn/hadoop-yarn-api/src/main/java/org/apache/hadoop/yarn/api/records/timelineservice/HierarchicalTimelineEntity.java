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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@XmlAccessorType(XmlAccessType.NONE)
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class HierarchicalTimelineEntity extends TimelineEntity {
  private Identifier parent;
  private HashMap<String, Set<String>> children = new HashMap<>();

  HierarchicalTimelineEntity(String type) {
    super(type);
  }

  @XmlElement(name = "parent")
  public Identifier getParent() {
    return parent;
  }

  public void setParent(Identifier parent) {
    validateParent(parent.getType());
    this.parent = parent;
  }

  public void setParent(String type, String id) {
    validateParent(type);
    parent = new Identifier();
    parent.setType(type);
    parent.setId(id);
  }

  // required by JAXB
  @InterfaceAudience.Private
  @XmlElement(name = "children")
  public HashMap<String, Set<String>> getChildrenJAXB() {
    return children;
  }

  public Map<String, Set<String>> getChildren() {
    return children;
  }

  public void setChildren(Map<String, Set<String>> children) {
    validateChildren(children);
    if (children != null && !(children instanceof HashMap)) {
      this.children = new HashMap<String, Set<String>>(children);
    } else {
      this.children = (HashMap) children;
    }
  }

  public void addChildren(Map<String, Set<String>> children) {
    validateChildren(children);
    for (Map.Entry<String, Set<String>> entry : children.entrySet()) {
      Set<String> ids = this.children.get(entry.getKey());
      if (ids == null) {
        ids = new HashSet<>();
        this.children.put(entry.getKey(), ids);
      }
      ids.addAll(entry.getValue());
    }
  }

  public void addChild(String type, String id) {
    TimelineEntityType thisType = TimelineEntityType.valueOf(getType());
    TimelineEntityType childType = TimelineEntityType.valueOf(type);
    if (thisType.isChild(childType)) {
      Set<String> ids = children.get(type);
      if (ids == null) {
        ids = new HashSet<>();
        children.put(type, ids);
      }
      ids.add(id);
    } else {
      throw new IllegalArgumentException(
          type + " is not the acceptable child of " + this.getType());
    }
  }

  private void validateParent(String type) {
    TimelineEntityType parentType = TimelineEntityType.valueOf(type);
    TimelineEntityType thisType = TimelineEntityType.valueOf(getType());
    if (!thisType.isParent(parentType)) {
      throw new IllegalArgumentException(
          type + " is not the acceptable parent of " + this.getType());
    }
  }

  private void validateChildren(Map<String, Set<String>> children) {
    TimelineEntityType thisType = TimelineEntityType.valueOf(getType());
    for (Map.Entry<String, Set<String>> entry : children.entrySet()) {
      TimelineEntityType childType = TimelineEntityType.valueOf(entry.getKey());
      if (!thisType.isChild(childType)) {
        throw new IllegalArgumentException(
            entry.getKey() + " is not the acceptable child of " +
                this.getType());
      }
    }
  }
}
