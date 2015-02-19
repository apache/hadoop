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

@XmlRootElement(name = "flow")
@XmlAccessorType(XmlAccessType.NONE)
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class FlowEntity extends HierarchicalTimelineEntity {
  private String user;
  private String version;
  private String run;

  public FlowEntity() {
    super(TimelineEntityType.YARN_FLOW.toString());
  }

  @Override
  public String getId() {
    //Flow id schema: user@flow_name(or id)/version/run
    StringBuilder sb = new StringBuilder();
    sb.append(user);
    sb.append('@');
    sb.append(super.getId());
    sb.append('/');
    sb.append(version);
    sb.append('/');
    sb.append(run);
    return sb.toString();
  }

  @XmlElement(name = "user")
  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  @XmlElement(name = "version")
  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  @XmlElement(name = "run")
  public String getRun() {
    return run;
  }

  public void setRun(String run) {
    this.run = run;
  }
}
