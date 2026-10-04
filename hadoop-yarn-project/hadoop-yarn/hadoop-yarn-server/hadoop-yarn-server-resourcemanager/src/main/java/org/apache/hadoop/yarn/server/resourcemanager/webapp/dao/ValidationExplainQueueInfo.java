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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;

/**
 * The resolved properties of one explained queue.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {"queuePath", "properties"})
public class ValidationExplainQueueInfo {
  private String queuePath;
  @XmlElement(name = "property")
  private List<ValidationExplainPropertyInfo> properties = new ArrayList<>();

  public ValidationExplainQueueInfo() {
    // JAXB needs this
  }

  public ValidationExplainQueueInfo(String queuePath,
      List<ValidationResult.ExplainEntry> entries) {
    this.queuePath = queuePath;
    for (ValidationResult.ExplainEntry entry : entries) {
      properties.add(new ValidationExplainPropertyInfo(entry));
    }
  }

  public String getQueuePath() {
    return queuePath;
  }

  public List<ValidationExplainPropertyInfo> getProperties() {
    return properties;
  }
}
