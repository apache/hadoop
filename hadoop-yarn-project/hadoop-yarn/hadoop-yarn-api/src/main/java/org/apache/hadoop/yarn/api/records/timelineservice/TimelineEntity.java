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
import javax.xml.bind.annotation.XmlRootElement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@XmlRootElement(name = "entity")
@XmlAccessorType(XmlAccessType.NONE)
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class TimelineEntity {

  @XmlRootElement(name = "identifier")
  @XmlAccessorType(XmlAccessType.NONE)
  public static class Identifier {
    private String type;
    private String id;

    public Identifier() {

    }

    @XmlElement(name = "type")
    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    @XmlElement(name = "id")
    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }
  }

  private Identifier identifier;
  private HashMap<String, Object> info = new HashMap<>();
  private HashMap<String, Object> configs = new HashMap<>();
  private Set<TimelineMetric> metrics = new HashSet<>();
  private Set<TimelineEvent> events = new HashSet<>();
  private HashMap<String, Set<String>> isRelatedToEntities = new HashMap<>();
  private HashMap<String, Set<String>> relatesToEntities = new HashMap<>();
  private long createdTime;
  private long modifiedTime;

  public TimelineEntity() {
    identifier = new Identifier();
  }

  protected TimelineEntity(String type) {
    this();
    identifier.type = type;
  }

  @XmlElement(name = "type")
  public String getType() {
    return identifier.type;
  }

  public void setType(String type) {
    identifier.type = type;
  }

  @XmlElement(name = "id")
  public String getId() {
    return identifier.id;
  }

  public void setId(String id) {
    identifier.id = id;
  }

  public Identifier getIdentifier() {
    return identifier;
  }

  public void setIdentifier(Identifier identifier) {
    this.identifier = identifier;
  }

  // required by JAXB
  @InterfaceAudience.Private
  @XmlElement(name = "info")
  public HashMap<String, Object> getInfoJAXB() {
    return info;
  }

  public Map<String, Object> getInfo() {
    return info;
  }

  public void setInfo(Map<String, Object> info) {
    if (info != null && !(info instanceof HashMap)) {
      this.info = new HashMap<String, Object>(info);
    } else {
      this.info = (HashMap<String, Object>) info;
    }
  }

  public void addInfo(Map<String, Object> info) {
    this.info.putAll(info);
  }

  public void addInfo(String key, Object value) {
    info.put(key, value);
  }

  // required by JAXB
  @InterfaceAudience.Private
  @XmlElement(name = "configs")
  public HashMap<String, Object> getConfigsJAXB() {
    return configs;
  }

  public Map<String, Object> getConfigs() {
    return configs;
  }

  public void setConfigs(Map<String, Object> configs) {
    if (configs != null && !(configs instanceof HashMap)) {
      this.configs = new HashMap<String, Object>(configs);
    } else {
      this.configs = (HashMap<String, Object>) configs;
    }
  }

  public void addConfigs(Map<String, Object> configs) {
    this.configs.putAll(configs);
  }

  public void addConfig(String key, Object value) {
    configs.put(key, value);
  }

  @XmlElement(name = "metrics")
  public Set<TimelineMetric> getMetrics() {
    return metrics;
  }

  public void setMetrics(Set<TimelineMetric> metrics) {
    this.metrics = metrics;
  }

  public void addMetrics(Set<TimelineMetric> metrics) {
    this.metrics.addAll(metrics);
  }

  public void addMetric(TimelineMetric metric) {
    metrics.add(metric);
  }

  @XmlElement(name = "events")
  public Set<TimelineEvent> getEvents() {
    return events;
  }

  public void setEvents(Set<TimelineEvent> events) {
    this.events = events;
  }

  public void addEvents(Set<TimelineEvent> events) {
    this.events.addAll(events);
  }

  public void addEvent(TimelineEvent event) {
    events.add(event);
  }

  public Map<String, Set<String>> getIsRelatedToEntities() {
    return isRelatedToEntities;
  }

  // required by JAXB
  @InterfaceAudience.Private
  @XmlElement(name = "isrelatedto")
  public HashMap<String, Set<String>> getIsRelatedToEntitiesJAXB() {
    return isRelatedToEntities;
  }

  public void setIsRelatedToEntities(
      Map<String, Set<String>> isRelatedToEntities) {
    if (isRelatedToEntities != null && !(isRelatedToEntities instanceof HashMap)) {
      this.isRelatedToEntities = new HashMap<String, Set<String>>(isRelatedToEntities);
    } else {
      this.isRelatedToEntities = (HashMap<String, Set<String>>) isRelatedToEntities;
    }
  }

  public void addIsRelatedToEntities(
      Map<String, Set<String>> isRelatedToEntities) {
    for (Map.Entry<String, Set<String>> entry : isRelatedToEntities
        .entrySet()) {
      Set<String> ids = this.isRelatedToEntities.get(entry.getKey());
      if (ids == null) {
        ids = new HashSet<>();
        this.isRelatedToEntities.put(entry.getKey(), ids);
      }
      ids.addAll(entry.getValue());
    }
  }

  public void addIsRelatedToEntity(String type, String id) {
    Set<String> ids = isRelatedToEntities.get(type);
    if (ids == null) {
      ids = new HashSet<>();
      isRelatedToEntities.put(type, ids);
    }
    ids.add(id);
  }

  // required by JAXB
  @InterfaceAudience.Private
  @XmlElement(name = "relatesto")
  public HashMap<String, Set<String>> getRelatesToEntitiesJAXB() {
    return relatesToEntities;
  }

  public Map<String, Set<String>> getRelatesToEntities() {
    return relatesToEntities;
  }

  public void addRelatesToEntities(Map<String, Set<String>> relatesToEntities) {
    for (Map.Entry<String, Set<String>> entry : relatesToEntities.entrySet()) {
      Set<String> ids = this.relatesToEntities.get(entry.getKey());
      if (ids == null) {
        ids = new HashSet<>();
        this.relatesToEntities.put(entry.getKey(), ids);
      }
      ids.addAll(entry.getValue());
    }
  }

  public void addRelatesToEntity(String type, String id) {
    Set<String> ids = relatesToEntities.get(type);
    if (ids == null) {
      ids = new HashSet<>();
      relatesToEntities.put(type, ids);
    }
    ids.add(id);
  }

  public void setRelatesToEntities(Map<String, Set<String>> relatesToEntities) {
    if (relatesToEntities != null && !(relatesToEntities instanceof HashMap)) {
      this.relatesToEntities = new HashMap<String, Set<String>>(relatesToEntities);
    } else {
      this.relatesToEntities = (HashMap<String, Set<String>>) relatesToEntities;
    }
  }

  @XmlElement(name = "createdtime")
  public long getCreatedTime() {
    return createdTime;
  }

  public void setCreatedTime(long createdTime) {
    this.createdTime = createdTime;
  }

  @XmlElement(name = "modifiedtime")
  public long getModifiedTime() {
    return modifiedTime;
  }

  public void setModifiedTime(long modifiedTime) {
    this.modifiedTime = modifiedTime;
  }


}