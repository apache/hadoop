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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowactivity;


import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This doc represents the {@link FlowActivityEntity} which is used for
 * showing all the flow runs with limited information.
 */
public class FlowActivityDocument implements
    TimelineDocument<FlowActivityDocument> {

  private String id;
  private final String type = TimelineEntityType.YARN_FLOW_ACTIVITY.toString();
  private Set<FlowActivitySubDoc> flowActivities = new HashSet<>();
  private long dayTimestamp;
  private String user;
  private String flowName;

  public FlowActivityDocument() {
  }

  public FlowActivityDocument(String flowName, String flowVersion,
      long flowRunId) {
    flowActivities.add(new FlowActivitySubDoc(flowName,
        flowVersion, flowRunId));
  }

  /**
   * Merge the {@link FlowActivityDocument} that is passed with the current
   * document for upsert.
   *
   * @param flowActivityDocument
   *          that has to be merged
   */
  @Override
  public void merge(FlowActivityDocument flowActivityDocument) {
    if (flowActivityDocument.getDayTimestamp() > 0) {
      this.dayTimestamp = flowActivityDocument.getDayTimestamp();
    }
    this.flowName = flowActivityDocument.getFlowName();
    this.user = flowActivityDocument.getUser();
    this.id = flowActivityDocument.getId();
    this.flowActivities.addAll(flowActivityDocument.getFlowActivities());
  }

  @Override
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  @Override
  public String getType() {
    return type;
  }

  public void addFlowActivity(String flowActivityName, String flowVersion,
      long flowRunId) {
    flowActivities.add(new FlowActivitySubDoc(flowActivityName,
        flowVersion, flowRunId));
  }

  public Set<FlowActivitySubDoc> getFlowActivities() {
    return flowActivities;
  }

  public void setFlowActivities(Set<FlowActivitySubDoc> flowActivities) {
    this.flowActivities = flowActivities;
  }

  @Override
  public long getCreatedTime() {
    return TimeUnit.SECONDS.toMillis(dayTimestamp);
  }

  @Override
  public void setCreatedTime(long time) {
  }

  public long getDayTimestamp() {
    return dayTimestamp;
  }

  public void setDayTimestamp(long dayTimestamp) {
    this.dayTimestamp = dayTimestamp;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getFlowName() {
    return flowName;
  }

  public void setFlowName(String flowName) {
    this.flowName = flowName;
  }
}