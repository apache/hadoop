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

import java.util.Collection;
import java.util.Date;
import java.util.NavigableSet;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * Entity that represents a record for flow activity. It's essentially a
 * container entity for flow runs with limited information.
 */
@Public
@Unstable
public class FlowActivityEntity extends TimelineEntity {
  public static final String CLUSTER_INFO_KEY =
      TimelineEntity.SYSTEM_INFO_KEY_PREFIX +  "CLUSTER";
  public static final String DATE_INFO_KEY =
      TimelineEntity.SYSTEM_INFO_KEY_PREFIX +  "DATE";
  public static final String USER_INFO_KEY =
      TimelineEntity.SYSTEM_INFO_KEY_PREFIX + "USER";
  public static final String FLOW_NAME_INFO_KEY =
      TimelineEntity.SYSTEM_INFO_KEY_PREFIX + "FLOW_NAME";

  private final NavigableSet<FlowRunEntity> flowRuns = new TreeSet<>();

  public FlowActivityEntity() {
    super(TimelineEntityType.YARN_FLOW_ACTIVITY.toString());
    // set config to null
    setConfigs(null);
  }

  public FlowActivityEntity(String cluster, long time, String user,
      String flowName) {
    this();
    setCluster(cluster);
    setDate(time);
    setUser(user);
    setFlowName(flowName);
  }

  public FlowActivityEntity(TimelineEntity entity) {
    super(entity);
    if (!TimelineEntityType.YARN_FLOW_ACTIVITY.matches(entity.getType())) {
      throw new IllegalArgumentException("Incompatible entity type: " +
          getId());
    }
    // set config to null
    setConfigs(null);
  }

  @XmlElement(name = "id")
  @Override
  public String getId() {
    // flow activity: cluster/day/user@flow_name
    String id = super.getId();
    if (id == null) {
      StringBuilder sb = new StringBuilder();
      sb.append(getCluster());
      sb.append('/');
      sb.append(getDate().getTime());
      sb.append('/');
      sb.append(getUser());
      sb.append('@');
      sb.append(getFlowName());
      id = sb.toString();
      setId(id);
    }
    return id;
  }

  @Override
  public int compareTo(TimelineEntity entity) {
    int comparison = getType().compareTo(entity.getType());
    if (comparison == 0) {
      // order by cluster, date (descending), user, and flow name
      FlowActivityEntity other = (FlowActivityEntity)entity;
      int clusterComparison = getCluster().compareTo(other.getCluster());
      if (clusterComparison != 0) {
        return clusterComparison;
      }
      int dateComparisonDescending =
          (int)(other.getDate().getTime() - getDate().getTime()); // descending
      if (dateComparisonDescending != 0) {
        return dateComparisonDescending; // descending
      }
      int userComparison = getUser().compareTo(other.getUser());
      if (userComparison != 0) {
        return userComparison;
      }
      return getFlowName().compareTo(other.getFlowName());
    } else {
      return comparison;
    }
  }

  /**
   * Reuse the base class equals method.
   */
  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

  /**
   * Reuse the base class hashCode method.
   */
  @Override
  public int hashCode() {
    return super.hashCode();
  }

  public String getCluster() {
    return (String)getInfo().get(CLUSTER_INFO_KEY);
  }

  public void setCluster(String cluster) {
    addInfo(CLUSTER_INFO_KEY, cluster);
  }

  public Date getDate() {
    Object date = getInfo().get(DATE_INFO_KEY);
    if (date != null) {
      if (date instanceof Long) {
        return new Date((Long)date);
      } else if (date instanceof Date) {
        return (Date)date;
      }
    }
    return null;
  }

  public void setDate(long time) {
    Date date = new Date(time);
    addInfo(DATE_INFO_KEY, date);
  }

  public String getUser() {
    return (String)getInfo().get(USER_INFO_KEY);
  }

  public void setUser(String user) {
    addInfo(USER_INFO_KEY, user);
  }

  public String getFlowName() {
    return (String)getInfo().get(FLOW_NAME_INFO_KEY);
  }

  public void setFlowName(String flowName) {
    addInfo(FLOW_NAME_INFO_KEY, flowName);
  }

  public void addFlowRun(FlowRunEntity run) {
    flowRuns.add(run);
  }

  public void addFlowRuns(Collection<FlowRunEntity> runs) {
    flowRuns.addAll(runs);
  }

  @XmlElement(name = "flowruns")
  public NavigableSet<FlowRunEntity> getFlowRuns() {
    return flowRuns;
  }

  public int getNumberOfRuns() {
    return flowRuns.size();
  }
}
