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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.util.TimelineServiceHelper;

import com.fasterxml.jackson.annotation.JsonSetter;

/**
 * The basic timeline entity data structure for timeline service v2. Timeline
 * entity objects are not thread safe and should not be accessed concurrently.
 * All collection members will be initialized into empty collections. Two
 * timeline entities are equal iff. their type and id are identical.
 *
 * All non-primitive type, non-collection members will be initialized into null.
 * User should set the type and id of a timeline entity to make it valid (can be
 * checked by using the {@link #isValid()} method). Callers to the getters
 * should perform null checks for non-primitive type, non-collection members.
 *
 * Callers are recommended not to alter the returned collection objects from the
 * getters.
 */
@XmlRootElement(name = "entity")
@XmlAccessorType(XmlAccessType.NONE)
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class TimelineEntity implements Comparable<TimelineEntity> {
  protected final static String SYSTEM_INFO_KEY_PREFIX = "SYSTEM_INFO_";
  public final static long DEFAULT_ENTITY_PREFIX = 0L;

  /**
   * Identifier of timeline entity(entity id + entity type).
   */
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

    public void setType(String entityType) {
      this.type = entityType;
    }

    @XmlElement(name = "id")
    public String getId() {
      return id;
    }

    public void setId(String entityId) {
      this.id = entityId;
    }

    @Override
    public String toString() {
      return "TimelineEntity[" +
          "type='" + type + '\'' +
          ", id='" + id + '\'' + "]";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((id == null) ? 0 : id.hashCode());
      result =
        prime * result + ((type == null) ? 0 : type.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof Identifier)) {
        return false;
      }
      Identifier other = (Identifier) obj;
      if (id == null) {
        if (other.getId() != null) {
          return false;
        }
      } else if (!id.equals(other.getId())) {
        return false;
      }
      if (type == null) {
        if (other.getType() != null) {
          return false;
        }
      } else if (!type.equals(other.getType())) {
        return false;
      }
      return true;
    }
  }

  private TimelineEntity real;
  private Identifier identifier;
  private HashMap<String, Object> info = new HashMap<>();
  private HashMap<String, String> configs = new HashMap<>();
  private Set<TimelineMetric> metrics = new HashSet<>();
  // events should be sorted by timestamp in descending order
  private NavigableSet<TimelineEvent> events = new TreeSet<>();
  private HashMap<String, Set<String>> isRelatedToEntities = new HashMap<>();
  private HashMap<String, Set<String>> relatesToEntities = new HashMap<>();
  private Long createdTime;
  private long idPrefix;

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

  public void setIdentifier(Identifier entityIdentifier) {
    if (real == null) {
      this.identifier = entityIdentifier;
    } else {
      real.setIdentifier(entityIdentifier);
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

  public void setInfo(Map<String, Object> entityInfos) {
    if (real == null) {
      this.info = TimelineServiceHelper.mapCastToHashMap(entityInfos);
    } else {
      real.setInfo(entityInfos);
    }
  }

  public void addInfo(Map<String, Object> entityInfos) {
    if (real == null) {
      this.info.putAll(entityInfos);
    } else {
      real.addInfo(entityInfos);
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

  public void setConfigs(Map<String, String> entityConfigs) {
    if (real == null) {
      this.configs = TimelineServiceHelper.mapCastToHashMap(entityConfigs);
    } else {
      real.setConfigs(entityConfigs);
    }
  }

  public void addConfigs(Map<String, String> entityConfigs) {
    if (real == null) {
      this.configs.putAll(entityConfigs);
    } else {
      real.addConfigs(entityConfigs);
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

  public void setMetrics(Set<TimelineMetric> entityMetrics) {
    if (real == null) {
      this.metrics = entityMetrics;
    } else {
      real.setMetrics(entityMetrics);
    }
  }

  public void addMetrics(Set<TimelineMetric> entityMetrics) {
    if (real == null) {
      this.metrics.addAll(entityMetrics);
    } else {
      real.addMetrics(entityMetrics);
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
  public NavigableSet<TimelineEvent> getEvents() {
    if (real == null) {
      return events;
    } else {
      return real.getEvents();
    }
  }

  public void setEvents(NavigableSet<TimelineEvent> entityEvents) {
    if (real == null) {
      this.events = entityEvents;
    } else {
      real.setEvents(entityEvents);
    }
  }

  public void addEvents(Set<TimelineEvent> entityEvents) {
    if (real == null) {
      this.events.addAll(entityEvents);
    } else {
      real.addEvents(entityEvents);
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
      Map<String, Set<String>> isRelatedTo) {
    if (real == null) {
      this.isRelatedToEntities =
          TimelineServiceHelper.mapCastToHashMap(isRelatedTo);
    } else {
      real.setIsRelatedToEntities(isRelatedTo);
    }
  }

  public void addIsRelatedToEntities(
      Map<String, Set<String>> isRelatedTo) {
    if (real == null) {
      for (Map.Entry<String, Set<String>> entry : isRelatedTo.entrySet()) {
        Set<String> ids = this.isRelatedToEntities.get(entry.getKey());
        if (ids == null) {
          ids = new HashSet<>();
          this.isRelatedToEntities.put(entry.getKey(), ids);
        }
        ids.addAll(entry.getValue());
      }
    } else {
      real.addIsRelatedToEntities(isRelatedTo);
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

  public void addRelatesToEntities(Map<String, Set<String>> relatesTo) {
    if (real == null) {
      for (Map.Entry<String, Set<String>> entry : relatesTo.entrySet()) {
        Set<String> ids = this.relatesToEntities.get(entry.getKey());
        if (ids == null) {
          ids = new HashSet<>();
          this.relatesToEntities.put(entry.getKey(), ids);
        }
        ids.addAll(entry.getValue());
      }
    } else {
      real.addRelatesToEntities(relatesTo);
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
  public void setRelatesToEntities(Map<String, Set<String>> relatesTo) {
    if (real == null) {
      this.relatesToEntities =
          TimelineServiceHelper.mapCastToHashMap(relatesTo);
    } else {
      real.setRelatesToEntities(relatesTo);
    }
  }

  @XmlElement(name = "createdtime")
  public Long getCreatedTime() {
    if (real == null) {
      return createdTime;
    } else {
      return real.getCreatedTime();
    }
  }

  @JsonSetter("createdtime")
  public void setCreatedTime(Long createdTs) {
    if (real == null) {
      this.createdTime = createdTs;
    } else {
      real.setCreatedTime(createdTs);
    }
  }

  /**
   * Set UID in info which will be then used for query by UI.
   * @param uidKey key for UID in info.
   * @param uId UID to be set for the key.
   */
  public void setUID(String uidKey, String uId) {
    if (real == null) {
      info.put(uidKey, uId);
    } else {
      real.addInfo(uidKey, uId);
    }
  }

  public boolean isValid() {
    return (getId() != null && getType() != null);
  }

  // When get hashCode for a timeline entity, or check if two timeline entities
  // are equal, we only compare their identifiers (id and type)
  @Override
  public int hashCode() {
    return getIdentifier().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TimelineEntity)) {
      return false;
    }
    TimelineEntity other = (TimelineEntity) obj;
    return getIdentifier().equals(other.getIdentifier());
  }

  @Override
  public int compareTo(TimelineEntity other) {
    int comparison = getType().compareTo(other.getType());
    if (comparison == 0) {
      if (getIdPrefix() > other.getIdPrefix()) {
        // Descending order by entity id prefix
        return -1;
      } else if (getIdPrefix() < other.getIdPrefix()) {
        return 1;
      } else {
        return getId().compareTo(other.getId());
      }
    } else {
      return comparison;
    }
  }

  protected TimelineEntity getReal() {
    return real == null ? this : real;
  }

  public String toString() {
    if (real == null) {
      return identifier.toString();
    } else {
      return real.toString();
    }
  }

  @XmlElement(name = "idprefix")
  public long getIdPrefix() {
    if (real == null) {
      return idPrefix;
    } else {
      return real.getIdPrefix();
    }
  }

  /**
   * Sets idPrefix for an entity.
   * <p>
   * <b>Note</b>: Entities will be stored in the order of idPrefix specified.
   * If users decide to set idPrefix for an entity, they <b>MUST</b> provide
   * the same prefix for every update of this entity.
   * </p>
   * Example: <blockquote><pre>
   * TimelineEntity entity = new TimelineEntity();
   * entity.setIdPrefix(value);
   * </pre></blockquote>
   * Users can use {@link TimelineServiceHelper#invertLong(long)} to invert
   * the prefix if necessary.
   *
   * @param entityIdPrefix prefix for an entity.
   */
  @JsonSetter("idprefix")
  public void setIdPrefix(long entityIdPrefix) {
    if (real == null) {
      this.idPrefix = entityIdPrefix;
    } else {
      real.setIdPrefix(entityIdPrefix);
    }
  }
}