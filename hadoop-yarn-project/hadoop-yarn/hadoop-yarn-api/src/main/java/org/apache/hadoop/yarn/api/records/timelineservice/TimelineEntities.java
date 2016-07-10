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

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This class hosts a set of timeline entities.
 */
@XmlRootElement(name = "entities")
@XmlAccessorType(XmlAccessType.NONE)
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class TimelineEntities {

  private List<TimelineEntity> entities = new ArrayList<>();

  public TimelineEntities() {

  }

  @XmlElement(name = "entities")
  public List<TimelineEntity> getEntities() {
    return entities;
  }

  public void setEntities(List<TimelineEntity> timelineEntities) {
    this.entities = timelineEntities;
  }

  public void addEntities(List<TimelineEntity> timelineEntities) {
    this.entities.addAll(timelineEntities);
  }

  public void addEntity(TimelineEntity entity) {
    entities.add(entity);
  }
}
