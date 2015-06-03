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

import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.util.TimelineServiceHelper;

/**
 * The class that contains the information of an event that is related to some
 * conceptual entity of an application. Users are free to define what the event
 * means, such as starting an application, getting allocated a container and
 * etc.
 */
@XmlRootElement(name = "event")
@XmlAccessorType(XmlAccessType.NONE)
@Public
@Evolving
public class TimelineEvent implements Comparable<TimelineEvent> {

  private long timestamp;
  private String eventType;
  private HashMap<String, Object> eventInfo = new HashMap<String, Object>();

  public TimelineEvent() {
  }

  /**
   * Get the timestamp of the event
   * 
   * @return the timestamp of the event
   */
  @XmlElement(name = "timestamp")
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Set the timestamp of the event
   * 
   * @param timestamp
   *          the timestamp of the event
   */
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * Get the event type
   * 
   * @return the event type
   */
  @XmlElement(name = "eventtype")
  public String getEventType() {
    return eventType;
  }

  /**
   * Set the event type
   * 
   * @param eventType
   *          the event type
   */
  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  /**
   * Set the information of the event
   * 
   * @return the information of the event
   */
  public Map<String, Object> getEventInfo() {
    return eventInfo;
  }

  // Required by JAXB
  @Private
  @XmlElement(name = "eventinfo")
  public HashMap<String, Object> getEventInfoJAXB() {
    return eventInfo;
  }

  /**
   * Add one piece of the information of the event to the existing information
   * map
   * 
   * @param key
   *          the information key
   * @param value
   *          the information value
   */
  public void addEventInfo(String key, Object value) {
    this.eventInfo.put(key, value);
  }

  /**
   * Add a map of the information of the event to the existing information map
   * 
   * @param eventInfo
   *          a map of of the information of the event
   */
  public void addEventInfo(Map<String, Object> eventInfo) {
    this.eventInfo.putAll(eventInfo);
  }

  /**
   * Set the information map to the given map of the information of the event
   * 
   * @param eventInfo
   *          a map of of the information of the event
   */
  public void setEventInfo(Map<String, Object> eventInfo) {
    this.eventInfo = TimelineServiceHelper.mapCastToHashMap(
        eventInfo);
  }

  @Override
  public int compareTo(TimelineEvent other) {
    if (timestamp > other.timestamp) {
      return -1;
    } else if (timestamp < other.timestamp) {
      return 1;
    } else {
      return eventType.compareTo(other.eventType);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    TimelineEvent event = (TimelineEvent) o;

    if (timestamp != event.timestamp)
      return false;
    if (!eventType.equals(event.eventType))
      return false;
    if (eventInfo != null ? !eventInfo.equals(event.eventInfo) :
        event.eventInfo != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (timestamp ^ (timestamp >>> 32));
    result = 31 * result + eventType.hashCode();
    result = 31 * result + (eventInfo != null ? eventInfo.hashCode() : 0);
    return result;
  }
}
