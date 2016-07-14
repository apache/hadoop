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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.HashMap;
import java.util.Map;

/**
 * This class contains the information of an event that belongs to an entity.
 * Users are free to define what the event means, such as starting an
 * application, container being allocated, etc.
 */
@XmlRootElement(name = "event")
@XmlAccessorType(XmlAccessType.NONE)
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class TimelineEvent implements Comparable<TimelineEvent> {
  public static final long INVALID_TIMESTAMP = 0L;

  private String id;
  private HashMap<String, Object> info = new HashMap<>();
  private long timestamp;

  public TimelineEvent() {

  }

  @XmlElement(name = "id")
  public String getId() {
    return id;
  }

  public void setId(String eventId) {
    this.id = eventId;
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

  public void setInfo(Map<String, Object> infos) {
    this.info = TimelineServiceHelper.mapCastToHashMap(infos);
  }

  public void addInfo(Map<String, Object> infos) {
    this.info.putAll(infos);
  }

  public void addInfo(String key, Object value) {
    info.put(key, value);
  }

  @XmlElement(name = "timestamp")
  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long ts) {
    this.timestamp = ts;
  }

  public boolean isValid() {
    return (id != null && timestamp != INVALID_TIMESTAMP);
  }

  @Override
  public int hashCode() {
    int result = (int) (timestamp ^ (timestamp >>> 32));
    result = 31 * result + id.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimelineEvent)) {
      return false;
    }

    TimelineEvent event = (TimelineEvent) o;

    if (timestamp != event.timestamp) {
      return false;
    }
    if (!id.equals(event.id)) {
      return false;
    }
    return true;
  }

  @Override
  public int compareTo(TimelineEvent other) {
    if (timestamp > other.timestamp) {
      return -1;
    } else if (timestamp < other.timestamp) {
      return 1;
    } else {
      return id.compareTo(other.id);
    }
  }
}
