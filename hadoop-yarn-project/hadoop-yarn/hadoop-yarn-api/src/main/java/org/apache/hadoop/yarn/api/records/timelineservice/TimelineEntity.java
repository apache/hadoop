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
import org.apache.hadoop.yarn.util.TimelineServiceHelper;
import org.codehaus.jackson.annotate.JsonSetter;

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
  protected final static String SYSTEM_INFO_KEY_PREFIX = "SYSTEM_INFO_";

  @XmlRootElement(name = "identifier")
  @XmlAccessorType(XmlAccessType.NONE)
  public static class Identifier {
    private String type;
    private String id;

    public Identifier(String type, String id) {
      this.type = type;
      this.id = id;
    }

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

    @Override
    public String toString() {
      return "TimelineEntity[" +
          "type='" + type + '\'' +
          ", id='" + id + '\'' + "]";
    }
  }

  private TimelineEntity real;
  private Identifier identifier;
  private HashMap<String, Object> info = new HashMap<>();
  private HashMap<String, String> configs = new HashMap<>();
  private Set<TimelineMetric> metrics = new HashSet<>();
  private Set<TimelineEvent> events = new HashSet<>();
  private HashMap<String, Set<String>> isRelatedToEntities = new HashMap<>();
  private HashMap<String, Set<String>> relatesToEntities = new HashMap<>();
  private long createdTime;
  private long modifiedTime;

  public TimelineEntity() {
    identifier = new Identifier();
  }

  /**
   * <p>
   * The constuctor is used to construct a proxy {@link TimelineEntity} or its
   * subclass object from the real entity object that carries information.
   * </p>
   *
   * <p>
   * It is usually used in the case where we want to recover class polymorphism
   * after deserializing the entity from its JSON form.
   * </p>
   * @param entity the real entity that carries information
   */
  public TimelineEntity(TimelineEntity entity) {
    real = entity.getReal();
  }

  protected TimelineEntity(String type) {
    this();
    identifier.type = type;
  }

  @XmlElement(name = "type")
  public String getType() {
    if (real == null) {
      return identifier.type;
    } else {
      return real.getType();
    }
  }

  public void setType(String type) {
    if (real == null) {
      identifier.type = type;
    } else {
      real.setType(type);
    }
  }

  @XmlElement(name = "id")
  public String getId() {
    if (real == null) {
      return identifier.id;
    } else {
      return real.getId();
    }
  }

  public void setId(String id) {
    if (real == null) {
      identifier.id = id;
    } else {
      real.setId(id);
    }
  }

  public Identifier getIdentifier() {
    if (real == null) {
      return identifier;
    } else {
      return real.getIdentifier();
    }
  }

  public void setIdentifier(Identifier identifier) {
    if (real == null) {
      this.identifier = identifier;
    } else {
      real.setIdentifier(identifier);
    }
  }

  // required by JAXB
  @InterfaceAudience.Private
  @XmlElement(name = "info")
  public HashMap<String, Object> getInfoJAXB() {
    if (real == null) {
      return info;
    } else {
      return real.getInfoJAXB();
    }
  }

  public Map<String, Object> getInfo() {
    if (real == null) {
      return info;
    } else {
      return real.getInfo();
    }
  }

  public void setInfo(Map<String, Object> info) {
    if (real == null) {
      this.info = TimelineServiceHelper.mapCastToHashMap(info);
    } else {
      real.setInfo(info);
    }
  }

  public void addInfo(Map<String, Object> info) {
    if (real == null) {
      this.info.putAll(info);
    } else {
      real.addInfo(info);
    }
  }

  public void addInfo(String key, Object value) {
    if (real == null) {
      info.put(key, value);
    } else {
      real.addInfo(key, value);
    }
  }

  // required by JAXB
  @InterfaceAudience.Private
  @XmlElement(name = "configs")
  public HashMap<String, String> getConfigsJAXB() {
    if (real == null) {
      return configs;
    } else {
      return real.getConfigsJAXB();
    }
  }

  public Map<String, String> getConfigs() {
    if (real == null) {
      return configs;
    } else {
      return real.getConfigs();
    }
  }

  public void setConfigs(Map<String, String> configs) {
    if (real == null) {
      this.configs = TimelineServiceHelper.mapCastToHashMap(configs);
    } else {
      real.setConfigs(configs);
    }
  }

  public void addConfigs(Map<String, String> configs) {
    if (real == null) {
      this.configs.putAll(configs);
    } else {
      real.addConfigs(configs);
    }
  }

  public void addConfig(String key, String value) {
    if (real == null) {
      configs.put(key, value);
    } else {
      real.addConfig(key, value);
    }
  }

  @XmlElement(name = "metrics")
  public Set<TimelineMetric> getMetrics() {
    if (real == null) {
      return metrics;
    } else {
      return real.getMetrics();
    }
  }

  public void setMetrics(Set<TimelineMetric> metrics) {
    if (real == null) {
      this.metrics = metrics;
    } else {
      real.setMetrics(metrics);
    }
  }

  public void addMetrics(Set<TimelineMetric> metrics) {
    if (real == null) {
      this.metrics.addAll(metrics);
    } else {
      real.addMetrics(metrics);
    }
  }

  public void addMetric(TimelineMetric metric) {
    if (real == null) {
      metrics.add(metric);
    } else {
      real.addMetric(metric);
    }
  }

  @XmlElement(name = "events")
  public Set<TimelineEvent> getEvents() {
    if (real == null) {
      return events;
    } else {
      return real.getEvents();
    }
  }

  public void setEvents(Set<TimelineEvent> events) {
    if (real == null) {
      this.events = events;
    } else {
      real.setEvents(events);
    }
  }

  public void addEvents(Set<TimelineEvent> events) {
    if (real == null) {
      this.events.addAll(events);
    } else {
      real.addEvents(events);
    }
  }

  public void addEvent(TimelineEvent event) {
    if (real == null) {
      events.add(event);
    } else {
      real.addEvent(event);
    }
  }

  public Map<String, Set<String>> getIsRelatedToEntities() {
    if (real == null) {
      return isRelatedToEntities;
    } else {
      return real.getIsRelatedToEntities();
    }
  }

  // required by JAXB
  @InterfaceAudience.Private
  @XmlElement(name = "isrelatedto")
  public HashMap<String, Set<String>> getIsRelatedToEntitiesJAXB() {
    if (real == null) {
      return isRelatedToEntities;
    } else {
      return real.getIsRelatedToEntitiesJAXB();
    }
  }

  @JsonSetter("isrelatedto")
  public void setIsRelatedToEntities(
      Map<String, Set<String>> isRelatedToEntities) {
    if (real == null) {
      this.isRelatedToEntities =
          TimelineServiceHelper.mapCastToHashMap(isRelatedToEntities);
    } else {
      real.setIsRelatedToEntities(isRelatedToEntities);
    }
  }

  public void addIsRelatedToEntities(
      Map<String, Set<String>> isRelatedToEntities) {
    if (real == null) {
      for (Map.Entry<String, Set<String>> entry : isRelatedToEntities
          .entrySet()) {
        Set<String> ids = this.isRelatedToEntities.get(entry.getKey());
        if (ids == null) {
          ids = new HashSet<>();
          this.isRelatedToEntities.put(entry.getKey(), ids);
        }
        ids.addAll(entry.getValue());
      }
    } else {
      real.addIsRelatedToEntities(isRelatedToEntities);
    }
  }

  public void addIsRelatedToEntity(String type, String id) {
    if (real == null) {
      Set<String> ids = isRelatedToEntities.get(type);
      if (ids == null) {
        ids = new HashSet<>();
        isRelatedToEntities.put(type, ids);
      }
      ids.add(id);
    } else {
      real.addIsRelatedToEntity(type, id);
    }
  }

  // required by JAXB
  @InterfaceAudience.Private
  @XmlElement(name = "relatesto")
  public HashMap<String, Set<String>> getRelatesToEntitiesJAXB() {
    if (real == null) {
      return relatesToEntities;
    } else {
      return real.getRelatesToEntitiesJAXB();
    }
  }

  public Map<String, Set<String>> getRelatesToEntities() {
    if (real == null) {
      return relatesToEntities;
    } else {
      return real.getRelatesToEntities();
    }
  }

  public void addRelatesToEntities(Map<String, Set<String>> relatesToEntities) {
    if (real == null) {
      for (Map.Entry<String, Set<String>> entry : relatesToEntities
          .entrySet()) {
        Set<String> ids = this.relatesToEntities.get(entry.getKey());
        if (ids == null) {
          ids = new HashSet<>();
          this.relatesToEntities.put(entry.getKey(), ids);
        }
        ids.addAll(entry.getValue());
      }
    } else {
      real.addRelatesToEntities(relatesToEntities);
    }
  }

  public void addRelatesToEntity(String type, String id) {
    if (real == null) {
      Set<String> ids = relatesToEntities.get(type);
      if (ids == null) {
        ids = new HashSet<>();
        relatesToEntities.put(type, ids);
      }
      ids.add(id);
    } else {
      real.addRelatesToEntity(type, id);
    }
  }

  @JsonSetter("relatesto")
  public void setRelatesToEntities(Map<String, Set<String>> relatesToEntities) {
    if (real == null) {
      this.relatesToEntities =
          TimelineServiceHelper.mapCastToHashMap(relatesToEntities);
    } else {
      real.setRelatesToEntities(relatesToEntities);
    }
  }

  @XmlElement(name = "createdtime")
  public long getCreatedTime() {
    if (real == null) {
      return createdTime;
    } else {
      return real.getCreatedTime();
    }
  }

  @JsonSetter("createdtime")
  public void setCreatedTime(long createdTime) {
    if (real == null) {
      this.createdTime = createdTime;
    } else {
      real.setCreatedTime(createdTime);
    }
  }

  @XmlElement(name = "modifiedtime")
  public long getModifiedTime() {
    if (real == null) {
      return modifiedTime;
    } else {
      return real.getModifiedTime();
    }
  }

  @JsonSetter("modifiedtime")
  public void setModifiedTime(long modifiedTime) {
    if (real == null) {
      this.modifiedTime = modifiedTime;
    } else {
      real.setModifiedTime(modifiedTime);
    }
  }

  protected TimelineEntity getReal() {
    return real == null ? this : real;
  }

  public String toString() {
    return identifier.toString();
  }
}