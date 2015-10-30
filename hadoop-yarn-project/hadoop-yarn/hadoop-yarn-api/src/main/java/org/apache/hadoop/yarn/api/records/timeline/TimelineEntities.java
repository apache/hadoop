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

package org.apache.hadoop.yarn.api.records.timeline;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * The class that hosts a list of timeline entities.
 */
@XmlRootElement(name = "entities")
@XmlAccessorType(XmlAccessType.NONE)
@Public
@Evolving
public class TimelineEntities {

  private List<TimelineEntity> entities =
      new ArrayList<TimelineEntity>();

  public TimelineEntities() {

  }

  /**
   * Get a list of entities
   * 
   * @return a list of entities
   */
  @XmlElement(name = "entities")
  public List<TimelineEntity> getEntities() {
    return entities;
  }

  /**
   * Add a single entity into the existing entity list
   * 
   * @param entity
   *          a single entity
   */
  public void addEntity(TimelineEntity entity) {
    entities.add(entity);
  }

  /**
   * All a list of entities into the existing entity list
   * 
   * @param entities
   *          a list of entities
   */
  public void addEntities(List<TimelineEntity> entities) {
    this.entities.addAll(entities);
  }

  /**
   * Set the entity list to the given list of entities
   * 
   * @param entities
   *          a list of entities
   */
  public void setEntities(List<TimelineEntity> entities) {
    this.entities = entities;
  }

}
