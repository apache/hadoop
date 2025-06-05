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
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@XmlRootElement(name = "issue")
@XmlAccessorType(XmlAccessType.FIELD)
public class IssueType {
  private String name;
  private List<String> parameters = new ArrayList<>();

  public IssueType() {}

  public IssueType(String name) {
    this.name = name;
    this.parameters = Collections.emptyList();
  }

  public IssueType(String name, List<String> parameters) {
    this.name = name;
    this.parameters = new ArrayList<>(parameters);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(List<String> parameters) {
    this.parameters = parameters;
  }
}