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

package org.apache.hadoop.yarn.nodelabels;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Generic class capturing the information required commonly across Partitions
 * and Attributes.
 */
public abstract class AbstractLabel {

  private Resource resource;
  private int numActiveNMs;
  private String labelName;

  public AbstractLabel() {
    super();
  }

  public AbstractLabel(String labelName) {
    this(labelName, Resource.newInstance(0, 0), 0);
  }

  public AbstractLabel(String labelName, Resource resource, int numActiveNMs) {
    super();
    this.resource = resource;
    this.numActiveNMs = numActiveNMs;
    this.labelName = labelName;
  }

  public void addNode(Resource nodeRes) {
    Resources.addTo(resource, nodeRes);
    numActiveNMs++;
  }

  public void removeNode(Resource nodeRes) {
    Resources.subtractFrom(resource, nodeRes);
    numActiveNMs--;
  }

  public Resource getResource() {
    return Resource.newInstance(this.resource);
  }

  public int getNumActiveNMs() {
    return numActiveNMs;
  }

  public String getLabelName() {
    return labelName;
  }

}