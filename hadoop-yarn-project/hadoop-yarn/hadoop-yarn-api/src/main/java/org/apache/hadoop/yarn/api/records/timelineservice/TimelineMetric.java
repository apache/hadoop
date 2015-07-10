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
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

@XmlRootElement(name = "metric")
@XmlAccessorType(XmlAccessType.NONE)
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class TimelineMetric {

  public static enum Type {
    SINGLE_VALUE,
    TIME_SERIES
  }

  private Type type;
  private String id;
  private Comparator<Long> reverseComparator = new Comparator<Long>() {
    @Override
    public int compare(Long l1, Long l2) {
      return l2.compareTo(l1);
    }
  };
  private TreeMap<Long, Number> values = new TreeMap<>(reverseComparator);

  public TimelineMetric() {
    this(Type.SINGLE_VALUE);
  }

  public TimelineMetric(Type type) {
    this.type = type;
  }


  @XmlElement(name = "type")
  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  @XmlElement(name = "id")
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  // required by JAXB
  @InterfaceAudience.Private
  @XmlElement(name = "values")
  public TreeMap<Long, Number> getValuesJAXB() {
    return values;
  }

  public Map<Long, Number> getValues() {
    return values;
  }

  public void setValues(Map<Long, Number> values) {
    if (type == Type.SINGLE_VALUE) {
      overwrite(values);
    } else {
      if (values != null) {
        this.values = new TreeMap<Long, Number>(reverseComparator);
        this.values.putAll(values);
      } else {
        this.values = null;
      }
    }
  }

  public void addValues(Map<Long, Number> values) {
    if (type == Type.SINGLE_VALUE) {
      overwrite(values);
    } else {
      this.values.putAll(values);
    }
  }

  public void addValue(long timestamp, Number value) {
    if (type == Type.SINGLE_VALUE) {
      values.clear();
    }
    values.put(timestamp, value);
  }

  private void overwrite(Map<Long, Number> values) {
    if (values.size() > 1) {
      throw new IllegalArgumentException(
          "Values cannot contain more than one point in " +
              Type.SINGLE_VALUE + " mode");
    }
    this.values.clear();
    this.values.putAll(values);
  }

  public boolean isValid() {
    return (id != null);
  }

  @Override
  public int hashCode() {
    int result = id.hashCode();
    result = 31 * result + type.hashCode();
    return result;
  }

  // Only check if type and id are equal
  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof TimelineMetric))
      return false;

    TimelineMetric m = (TimelineMetric) o;

    if (!id.equals(m.id)) {
      return false;
    }
    if (type != m.type) {
      return false;
    }
    return true;
  }
}
