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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;

/**
 * Response of {@code POST /scheduler-conf/validate/v2}. The {@code issues}
 * wrapper is always present; {@code explain} is present only when explain
 * was requested.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@XmlRootElement(name = "validationResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {"valid", "issues", "explain"})
public class ValidationResultInfo {
  private boolean valid;
  @XmlElementWrapper(name = "issues")
  @XmlElement(name = "issue")
  private List<ValidationIssueInfo> issues = new ArrayList<>();
  @XmlElementWrapper(name = "explain")
  @XmlElement(name = "queue")
  private List<ValidationExplainQueueInfo> explain;

  public ValidationResultInfo() {
    // JAXB needs this
  }

  public ValidationResultInfo(ValidationResult result) {
    valid = result.isValid();
    for (ValidationIssue issue : result.getIssues()) {
      issues.add(new ValidationIssueInfo(issue));
    }
    if (result.hasExplain()) {
      explain = new ArrayList<>();
      for (Map.Entry<String, List<ValidationResult.ExplainEntry>> queue
          : result.getExplain().entrySet()) {
        explain.add(
            new ValidationExplainQueueInfo(queue.getKey(), queue.getValue()));
      }
    }
  }

  public boolean isValid() {
    return valid;
  }

  public List<ValidationIssueInfo> getIssues() {
    return issues;
  }

  /** @return explained queues, or null when explain was not requested */
  public List<ValidationExplainQueueInfo> getExplain() {
    return explain;
  }
}
