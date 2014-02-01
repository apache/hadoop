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

package org.apache.hadoop.yarn.api.records.apptimeline;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * The class that hosts a list of events, which are categorized according to
 * their related entities.
 */
@XmlRootElement(name = "events")
@XmlAccessorType(XmlAccessType.NONE)
@Public
@Unstable
public class ATSEvents {

  private List<ATSEventsOfOneEntity> allEvents =
      new ArrayList<ATSEventsOfOneEntity>();

  public ATSEvents() {

  }

  /**
   * Get a list of {@link ATSEventsOfOneEntity} instances
   * 
   * @return a list of {@link ATSEventsOfOneEntity} instances
   */
  @XmlElement(name = "events")
  public List<ATSEventsOfOneEntity> getAllEvents() {
    return allEvents;
  }

  /**
   * Add a single {@link ATSEventsOfOneEntity} instance into the existing list
   * 
   * @param eventsOfOneEntity
   *          a single {@link ATSEventsOfOneEntity} instance
   */
  public void addEvent(ATSEventsOfOneEntity eventsOfOneEntity) {
    allEvents.add(eventsOfOneEntity);
  }

  /**
   * Add a list of {@link ATSEventsOfOneEntity} instances into the existing list
   * 
   * @param allEvents
   *          a list of {@link ATSEventsOfOneEntity} instances
   */
  public void addEvents(List<ATSEventsOfOneEntity> allEvents) {
    this.allEvents.addAll(allEvents);
  }

  /**
   * Set the list to the given list of {@link ATSEventsOfOneEntity} instances
   * 
   * @param allEvents
   *          a list of {@link ATSEventsOfOneEntity} instances
   */
  public void setEvents(List<ATSEventsOfOneEntity> allEvents) {
    this.allEvents.clear();
    this.allEvents.addAll(allEvents);
  }

  /**
   * The class that hosts a list of events that are only related to one entity.
   */
  @XmlRootElement(name = "events")
  @XmlAccessorType(XmlAccessType.NONE)
  @Public
  @Unstable
  public static class ATSEventsOfOneEntity {

    private String entityId;
    private String entityType;
    private List<ATSEvent> events = new ArrayList<ATSEvent>();

    public ATSEventsOfOneEntity() {

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
     * Get a list of events
     * 
     * @return a list of events
     */
    @XmlElement(name = "events")
    public List<ATSEvent> getEvents() {
      return events;
    }

    /**
     * Add a single event to the existing event list
     * 
     * @param event
     *          a single event
     */
    public void addEntity(ATSEvent event) {
      events.add(event);
    }

    /**
     * Add a list of event to the existing event list
     * 
     * @param events
     *          a list of events
     */
    public void addEvents(List<ATSEvent> events) {
      this.events.addAll(events);
    }

    /**
     * Set the event list to the given list of events
     * 
     * @param events
     *          a list of events
     */
    public void setEvents(List<ATSEvent> events) {
      this.events = events;
    }

  }

}
