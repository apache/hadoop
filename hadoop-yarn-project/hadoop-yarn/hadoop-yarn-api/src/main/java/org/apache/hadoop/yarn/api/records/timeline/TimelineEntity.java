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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.util.TimelineServiceHelper;

/**
 * <p>
 * The class that contains the the meta information of some conceptual entity
 * and its related events. The entity can be an application, an application
 * attempt, a container or whatever the user-defined object.
 * </p>
 * 
 * <p>
 * Primary filters will be used to index the entities in
 * <code>TimelineStore</code>, such that users should carefully choose the
 * information they want to store as the primary filters. The remaining can be
 * stored as other information.
 * </p>
 */
@XmlRootElement(name = "entity")
@XmlAccessorType(XmlAccessType.NONE)
@Public
@Evolving
public class TimelineEntity implements Comparable<TimelineEntity> {

  private String entityType;
  private String entityId;
  private Long startTime;
  private List<TimelineEvent> events = new ArrayList<TimelineEvent>();
  private HashMap<String, Set<String>> relatedEntities =
      new HashMap<String, Set<String>>();
  private HashMap<String, Set<Object>> primaryFilters =
      new HashMap<String, Set<Object>>();
  private HashMap<String, Object> otherInfo =
      new HashMap<String, Object>();
  private String domainId;

  public TimelineEntity() {

  }

  /**
   * Get the entity type
   * 
   * @return the entity type
   */
  @XmlElement(name = "entitytype")
  public String getEntityType() {
    return entityType;
  }

  /**
   * Set the entity type
   * 
   * @param entityType
   *          the entity type
   */
  public void setEntityType(String entityType) {
    this.entityType = entityType;
  }

  /**
   * Get the entity Id
   * 
   * @return the entity Id
   */
  @XmlElement(name = "entity")
  public String getEntityId() {
    return entityId;
  }

  /**
   * Set the entity Id
   * 
   * @param entityId
   *          the entity Id
   */
  public void setEntityId(String entityId) {
    this.entityId = entityId;
  }

  /**
   * Get the start time of the entity
   * 
   * @return the start time of the entity
   */
  @XmlElement(name = "starttime")
  public Long getStartTime() {
    return startTime;
  }

  /**
   * Set the start time of the entity
   * 
   * @param startTime
   *          the start time of the entity
   */
  public void setStartTime(Long startTime) {
    this.startTime = startTime;
  }

  /**
   * Get a list of events related to the entity
   * 
   * @return a list of events related to the entity
   */
  @XmlElement(name = "events")
  public List<TimelineEvent> getEvents() {
    return events;
  }

  /**
   * Add a single event related to the entity to the existing event list
   * 
   * @param event
   *          a single event related to the entity
   */
  public void addEvent(TimelineEvent event) {
    events.add(event);
  }

  /**
   * Add a list of events related to the entity to the existing event list
   * 
   * @param events
   *          a list of events related to the entity
   */
  public void addEvents(List<TimelineEvent> events) {
    this.events.addAll(events);
  }

  /**
   * Set the event list to the given list of events related to the entity
   * 
   * @param events
   *          events a list of events related to the entity
   */
  public void setEvents(List<TimelineEvent> events) {
    this.events = events;
  }

  /**
   * Get the related entities
   * 
   * @return the related entities
   */
  public Map<String, Set<String>> getRelatedEntities() {
    return relatedEntities;
  }

  // Required by JAXB
  @Private
  @XmlElement(name = "relatedentities")
  public HashMap<String, Set<String>> getRelatedEntitiesJAXB() {
    return relatedEntities;
  }

  /**
   * Add an entity to the existing related entity map
   * 
   * @param entityType
   *          the entity type
   * @param entityId
   *          the entity Id
   */
  public void addRelatedEntity(String entityType, String entityId) {
    Set<String> thisRelatedEntity = relatedEntities.get(entityType);
    if (thisRelatedEntity == null) {
      thisRelatedEntity = new HashSet<String>();
      relatedEntities.put(entityType, thisRelatedEntity);
    }
    thisRelatedEntity.add(entityId);
  }

  /**
   * Add a map of related entities to the existing related entity map
   * 
   * @param relatedEntities
   *          a map of related entities
   */
  public void addRelatedEntities(Map<String, Set<String>> relatedEntities) {
    for (Entry<String, Set<String>> relatedEntity : relatedEntities.entrySet()) {
      Set<String> thisRelatedEntity =
          this.relatedEntities.get(relatedEntity.getKey());
      if (thisRelatedEntity == null) {
        this.relatedEntities.put(
            relatedEntity.getKey(), relatedEntity.getValue());
      } else {
        thisRelatedEntity.addAll(relatedEntity.getValue());
      }
    }
  }

  /**
   * Set the related entity map to the given map of related entities
   * 
   * @param relatedEntities
   *          a map of related entities
   */
  public void setRelatedEntities(
      Map<String, Set<String>> relatedEntities) {
    this.relatedEntities = TimelineServiceHelper.mapCastToHashMap(
        relatedEntities);
  }

  /**
   * Get the primary filters
   * 
   * @return the primary filters
   */
  public Map<String, Set<Object>> getPrimaryFilters() {
    return primaryFilters;
  }

  // Required by JAXB
  @Private
  @XmlElement(name = "primaryfilters")
  public HashMap<String, Set<Object>> getPrimaryFiltersJAXB() {
    return primaryFilters;
  }

  /**
   * Add a single piece of primary filter to the existing primary filter map
   * 
   * @param key
   *          the primary filter key
   * @param value
   *          the primary filter value
   */
  public void addPrimaryFilter(String key, Object value) {
    Set<Object> thisPrimaryFilter = primaryFilters.get(key);
    if (thisPrimaryFilter == null) {
      thisPrimaryFilter = new HashSet<Object>();
      primaryFilters.put(key, thisPrimaryFilter);
    }
    thisPrimaryFilter.add(value);
  }

  /**
   * Add a map of primary filters to the existing primary filter map
   * 
   * @param primaryFilters
   *          a map of primary filters
   */
  public void addPrimaryFilters(Map<String, Set<Object>> primaryFilters) {
    for (Entry<String, Set<Object>> primaryFilter : primaryFilters.entrySet()) {
      Set<Object> thisPrimaryFilter =
          this.primaryFilters.get(primaryFilter.getKey());
      if (thisPrimaryFilter == null) {
        this.primaryFilters.put(
            primaryFilter.getKey(), primaryFilter.getValue());
      } else {
        thisPrimaryFilter.addAll(primaryFilter.getValue());
      }
    }
  }

  /**
   * Set the primary filter map to the given map of primary filters
   * 
   * @param primaryFilters
   *          a map of primary filters
   */
  public void setPrimaryFilters(Map<String, Set<Object>> primaryFilters) {
    this.primaryFilters =
        TimelineServiceHelper.mapCastToHashMap(primaryFilters);
  }

  /**
   * Get the other information of the entity
   * 
   * @return the other information of the entity
   */
  public Map<String, Object> getOtherInfo() {
    return otherInfo;
  }

  // Required by JAXB
  @Private
  @XmlElement(name = "otherinfo")
  public HashMap<String, Object> getOtherInfoJAXB() {
    return otherInfo;
  }

  /**
   * Add one piece of other information of the entity to the existing other info
   * map
   * 
   * @param key
   *          the other information key
   * @param value
   *          the other information value
   */
  public void addOtherInfo(String key, Object value) {
    this.otherInfo.put(key, value);
  }

  /**
   * Add a map of other information of the entity to the existing other info map
   * 
   * @param otherInfo
   *          a map of other information
   */
  public void addOtherInfo(Map<String, Object> otherInfo) {
    this.otherInfo.putAll(otherInfo);
  }

  /**
   * Set the other info map to the given map of other information
   * 
   * @param otherInfo
   *          a map of other information
   */
  public void setOtherInfo(Map<String, Object> otherInfo) {
    this.otherInfo = TimelineServiceHelper.mapCastToHashMap(otherInfo);
  }

  /**
   * Get the ID of the domain that the entity is to be put
   * 
   * @return the domain ID
   */
  @XmlElement(name = "domain")
  public String getDomainId() {
    return domainId;
  }

  /**
   * Set the ID of the domain that the entity is to be put
   * 
   * @param domainId
   *          the name space ID
   */
  public void setDomainId(String domainId) {
    this.domainId = domainId;
  }

  @Override
  public int hashCode() {
    // generated by eclipse
    final int prime = 31;
    int result = 1;
    result = prime * result + ((entityId == null) ? 0 : entityId.hashCode());
    result =
        prime * result + ((entityType == null) ? 0 : entityType.hashCode());
    result = prime * result + ((events == null) ? 0 : events.hashCode());
    result = prime * result + ((otherInfo == null) ? 0 : otherInfo.hashCode());
    result =
        prime * result
            + ((primaryFilters == null) ? 0 : primaryFilters.hashCode());
    result =
        prime * result
            + ((relatedEntities == null) ? 0 : relatedEntities.hashCode());
    result = prime * result + ((startTime == null) ? 0 : startTime.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    // generated by eclipse
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TimelineEntity other = (TimelineEntity) obj;
    if (entityId == null) {
      if (other.entityId != null)
        return false;
    } else if (!entityId.equals(other.entityId))
      return false;
    if (entityType == null) {
      if (other.entityType != null)
        return false;
    } else if (!entityType.equals(other.entityType))
      return false;
    if (events == null) {
      if (other.events != null)
        return false;
    } else if (!events.equals(other.events))
      return false;
    if (otherInfo == null) {
      if (other.otherInfo != null)
        return false;
    } else if (!otherInfo.equals(other.otherInfo))
      return false;
    if (primaryFilters == null) {
      if (other.primaryFilters != null)
        return false;
    } else if (!primaryFilters.equals(other.primaryFilters))
      return false;
    if (relatedEntities == null) {
      if (other.relatedEntities != null)
        return false;
    } else if (!relatedEntities.equals(other.relatedEntities))
      return false;
    if (startTime == null) {
      if (other.startTime != null)
        return false;
    } else if (!startTime.equals(other.startTime))
      return false;
    return true;
  }

  @Override
  public int compareTo(TimelineEntity other) {
    int comparison = entityType.compareTo(other.entityType);
    if (comparison == 0) {
      long thisStartTime =
          startTime == null ? Long.MIN_VALUE : startTime;
      long otherStartTime =
          other.startTime == null ? Long.MIN_VALUE : other.startTime;
      if (thisStartTime > otherStartTime) {
        return -1;
      } else if (thisStartTime < otherStartTime) {
        return 1;
      } else {
        return entityId.compareTo(other.entityId);
      }
    } else {
      return comparison;
    }
  }

}
