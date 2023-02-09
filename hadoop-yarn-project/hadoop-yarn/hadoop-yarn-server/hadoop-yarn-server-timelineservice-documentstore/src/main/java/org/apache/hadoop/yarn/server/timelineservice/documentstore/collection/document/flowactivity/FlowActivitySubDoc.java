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

/**
 * This is a sub doc which represents each flow.
 */
public class FlowActivitySubDoc {
  private String flowName;
  private String flowVersion;
  private long flowRunId;

  public FlowActivitySubDoc() {
  }

  public FlowActivitySubDoc(String flowName, String flowVersion,
      long flowRunId) {
    this.flowName = flowName;
    this.flowVersion = flowVersion;
    this.flowRunId = flowRunId;
  }

  public String getFlowName() {
    return flowName;
  }

  public String getFlowVersion() {
    return flowVersion;
  }

  public long getFlowRunId() {
    return flowRunId;
  }

  @Override
  public int hashCode() {
    int result = flowVersion.hashCode();
    result = (int) (31 * result + flowRunId);
    return result;
  }

  // Only check if type and id are equal
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FlowActivitySubDoc)) {
      return false;
    }
    FlowActivitySubDoc m = (FlowActivitySubDoc) o;
    if (!flowVersion.equalsIgnoreCase(m.getFlowVersion())) {
      return false;
    }
    return flowRunId == m.getFlowRunId();
  }
}