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

import javax.xml.bind.annotation.XmlElement;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class FlowEntity extends HierarchicalTimelineEntity {
  public static final String USER_INFO_KEY =
      TimelineEntity.SYSTEM_INFO_KEY_PREFIX + "USER";
  public static final String FLOW_NAME_INFO_KEY =
      TimelineEntity.SYSTEM_INFO_KEY_PREFIX + "FLOW_NAME";
  public static final String FLOW_VERSION_INFO_KEY =
      TimelineEntity.SYSTEM_INFO_KEY_PREFIX +  "FLOW_VERSION";
  public static final String FLOW_RUN_ID_INFO_KEY =
      TimelineEntity.SYSTEM_INFO_KEY_PREFIX +  "FLOW_RUN_ID";

  public FlowEntity() {
    super(TimelineEntityType.YARN_FLOW.toString());
  }

  public FlowEntity(TimelineEntity entity) {
    super(entity);
    if (!entity.getType().equals(TimelineEntityType.YARN_FLOW.toString())) {
      throw new IllegalArgumentException("Incompatible entity type: " + getId());
    }
  }

  @XmlElement(name = "id")
  @Override
  public String getId() {
    //Flow id schema: user@flow_name(or id)/version/run_id
    String id = super.getId();
    if (id == null) {
      StringBuilder sb = new StringBuilder();
      sb.append(getInfo().get(USER_INFO_KEY).toString());
      sb.append('@');
      sb.append(getInfo().get(FLOW_NAME_INFO_KEY).toString());
      sb.append('/');
      sb.append(getInfo().get(FLOW_VERSION_INFO_KEY).toString());
      sb.append('/');
      sb.append(getInfo().get(FLOW_RUN_ID_INFO_KEY).toString());
      id = sb.toString();
      setId(id);
    }
    return id;
  }

  public String getUser() {
    Object user = getInfo().get(USER_INFO_KEY);
    return user == null ? null : user.toString();
  }

  public void setUser(String user) {
    addInfo(USER_INFO_KEY, user);
  }

  public String getName() {
    Object name = getInfo().get(FLOW_NAME_INFO_KEY);
    return name == null ? null : name.toString();
  }

  public void setName(String name) {
    addInfo(FLOW_NAME_INFO_KEY, name);
  }

  public String getVersion() {
    Object version = getInfo().get(FLOW_VERSION_INFO_KEY);
    return version == null ? null : version.toString();
  }

  public void setVersion(String version) {
    addInfo(FLOW_VERSION_INFO_KEY, version);
  }

  public long getRunId() {
    Object runId = getInfo().get(FLOW_RUN_ID_INFO_KEY);
    return runId == null ? 0L : (Long) runId;
  }

  public void setRunId(long runId) {
    addInfo(FLOW_RUN_ID_INFO_KEY, runId);
  }
}
