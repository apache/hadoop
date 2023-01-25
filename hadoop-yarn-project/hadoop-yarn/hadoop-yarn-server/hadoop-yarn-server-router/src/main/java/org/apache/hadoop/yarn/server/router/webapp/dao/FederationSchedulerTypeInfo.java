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
package org.apache.hadoop.yarn.server.router.webapp.dao;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class FederationSchedulerTypeInfo extends SchedulerTypeInfo {
  @XmlElement(name = "subCluster")
  private List<SchedulerTypeInfo> list = new ArrayList<>();

  public FederationSchedulerTypeInfo() {
  } // JAXB needs this

  public FederationSchedulerTypeInfo(ArrayList<SchedulerTypeInfo> list) {
    this.list = list;
  }

  public List<SchedulerTypeInfo> getList() {
    return list;
  }

  public void setList(List<SchedulerTypeInfo> list) {
    this.list = list;
  }
}
