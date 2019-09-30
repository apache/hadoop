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
import org.apache.hadoop.yarn.api.records.ResourceInformation;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;

@XmlRootElement(name = "CustomResourceInfo")
@XmlAccessorType(XmlAccessType.NONE)
public class CustomResourceInfo {

  @XmlElement
  public long memMB;

  @XmlElement
  public long vcore;

  private Resource resource;

  @XmlElement
  public ArrayList<CustomResourceType> customResourceTypeList = new ArrayList();

  public CustomResourceInfo() {
    // JAXB needs this
  }

  // The object of CustomeResourceInfo built from json string by web service has
  // no resource object constructed. Provide this method to rebuild.
  public void reBuild() {
    if (this.resource == null) {
      this.resource = Resource.newInstance(this.getMemMB(),
          (int) this.getVcore());
    }
  }

  public CustomResourceInfo(Resource r) {
    this.memMB = r.getMemorySize();
    this.vcore = r.getVirtualCores();
    this.resource = Resource.newInstance(r);
    if (this.customResourceTypeList == null) {
      this.customResourceTypeList = new ArrayList<>();
    }
    for (ResourceInformation ri : r.getResources()) {
      if (ri.getName().equals(ResourceInformation.MEMORY_MB.getName()) ||
          ri.getName().equals(ResourceInformation.VCORES.getName())) {
        continue;
      }
      if (ri.getValue() != 0) {
        this.customResourceTypeList.add(new CustomResourceType(ri.getName(), ri.getValue()));
      }
    }
  }

  public ArrayList<CustomResourceType> getCustomResourceTypeList() {
    return customResourceTypeList;
  }

  public Resource getResource() {
    return resource;
  }

  public long getMemMB() {
    return memMB;
  }

  public long getVcore() {
    return vcore;
  }

  public void setResource(Resource resource) {
    this.resource = resource;
  }
}
