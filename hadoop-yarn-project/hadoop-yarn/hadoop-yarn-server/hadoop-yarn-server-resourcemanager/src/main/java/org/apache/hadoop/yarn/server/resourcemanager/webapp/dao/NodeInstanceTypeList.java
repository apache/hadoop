/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;


import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement(name = "instanceTypes")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeInstanceTypeList {

  public NodeInstanceTypeList() {
    // JAXB needs this
  }

  public List<NodeInstanceType> getInstanceTypes() {
    return instanceTypes;
  }

  public List<NodeInstanceType> instanceTypes = new ArrayList<>();

  public NodeInstanceTypeList rebuild() {
    for (NodeInstanceType ni : instanceTypes) {
      ni.getCapacity().reBuild();
    }
    return this;
  }

}
