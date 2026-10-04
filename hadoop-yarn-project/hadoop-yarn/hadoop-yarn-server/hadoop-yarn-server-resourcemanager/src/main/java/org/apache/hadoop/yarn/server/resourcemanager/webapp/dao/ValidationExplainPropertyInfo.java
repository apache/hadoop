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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;

/**
 * One resolved property of an explained queue: key, value, value source and
 * the key or parent path the value came from.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {"key", "value", "source", "sourceDetail"})
public class ValidationExplainPropertyInfo {
  private String key;
  private String value;
  private String source;
  private String sourceDetail;

  public ValidationExplainPropertyInfo() {
    // JAXB needs this
  }

  public ValidationExplainPropertyInfo(ValidationResult.ExplainEntry entry) {
    key = entry.getKey();
    value = entry.getValue();
    source = entry.getSource();
    sourceDetail = entry.getSourceDetail();
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public String getSource() {
    return source;
  }

  public String getSourceDetail() {
    return sourceDetail;
  }
}
