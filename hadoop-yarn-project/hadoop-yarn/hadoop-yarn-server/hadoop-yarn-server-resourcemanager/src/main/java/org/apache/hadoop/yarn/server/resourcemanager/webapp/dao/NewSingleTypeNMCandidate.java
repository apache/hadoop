/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;


import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "NewSingleTypeNMCandidate")
@XmlAccessorType(XmlAccessType.NONE)
public class NewSingleTypeNMCandidate {
  public String getModelName() {
    return modelName;
  }

  public int getCount() {
    return count;
  }

  @XmlElement
  protected String modelName;
  // instance count of this type
  @XmlElement
  protected int count;

  public CustomResourceInfo getPlanRemaining() {
    return planRemaining;
  }

  public void setPlanRemaining(
      CustomResourceInfo planRemaining) {
    this.planRemaining = planRemaining;
  }

  // total remaining
  protected CustomResourceInfo planRemaining;
  // total capacity
  protected CustomResourceInfo capacity;

  public CustomResourceInfo getPlanToUse() {
    return planToUse;
  }

  public void setPlanToUse(
      CustomResourceInfo planToUse) {
    this.planToUse = planToUse;
    this.planRemaining.setResource(
        Resources.subtract(capacity.getResource(), planToUse.getResource()));
  }

  public void addPlanToUse(Resource more) {
    Resource newRes = Resources.addTo(this.planToUse.getResource(), more);
    this.planToUse.setResource(newRes);
    this.planRemaining.setResource(
        Resources.subtract(capacity.getResource(), planToUse.getResource()));
  }

  // total plan to use
  protected CustomResourceInfo planToUse;

  public NewSingleTypeNMCandidate() { }

  public NewSingleTypeNMCandidate(String n, int c, CustomResourceInfo ca) {
    this.modelName = n;
    this.count = c;
    this.capacity = ca;
    this.planRemaining = ca;
  }
}
