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
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;


@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
public class ResourceInfo {

  @XmlElement
  long memory;
  @XmlElement
  int vCores;
  @XmlElement
  ResourceInformationsInfo resourceInformations =
      new ResourceInformationsInfo();

  private Resource resources;

  public ResourceInfo() {
  }

  public ResourceInfo(Resource res) {
    if (res != null) {
      memory = res.getMemorySize();
      vCores = res.getVirtualCores();
      resources = Resources.clone(res);
      resourceInformations.addAll(res.getAllResourcesListCopy());
    }
  }

  public long getMemorySize() {
    if (resources == null) {
      resources = Resource.newInstance(memory, vCores);
    }
    return resources.getMemorySize();
  }

  public int getvCores() {
    if (resources == null) {
      resources = Resource.newInstance(memory, vCores);
    }
    return resources.getVirtualCores();
  }

  @Override
  public String toString() {
    return getResource().toString();
  }

  public void setMemory(int memory) {
    if (resources == null) {
      resources = Resource.newInstance(memory, vCores);
    }
    this.memory = memory;
    resources.setMemorySize(memory);
  }

  public void setvCores(int vCores) {
    if (resources == null) {
      resources = Resource.newInstance(memory, vCores);
    }
    this.vCores = vCores;
    resources.setVirtualCores(vCores);
  }

  public Resource getResource() {
    if (resources == null) {
      resources = Resource.newInstance(memory, vCores);
    }
    return Resource.newInstance(resources);
  }

  public ResourceInformationsInfo getResourcesInformations() {
    return resourceInformations;
  }
}
