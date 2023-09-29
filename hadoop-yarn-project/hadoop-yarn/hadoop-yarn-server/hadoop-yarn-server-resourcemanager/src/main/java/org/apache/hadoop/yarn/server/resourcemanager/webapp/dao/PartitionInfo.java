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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * XML element uses to represent partitionInfo.
 */
@XmlRootElement(name = "partitionInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class PartitionInfo {

  @XmlElement(name = "resourceAvailable")
  private ResourceInfo resourceAvailable;

  public PartitionInfo() {
  }

  public PartitionInfo(ResourceInfo resourceAvailable) {
    this.resourceAvailable = resourceAvailable;
  }

  public ResourceInfo getResourceAvailable() {
    return resourceAvailable;
  }

  /**
   * This method will generate a new PartitionInfo object based on two PartitionInfo objects.
   * The combination process is mainly based on the Resources. Add method.
   *
   * @param left left PartitionInfo Object.
   * @param right right PartitionInfo Object.
   * @return new PartitionInfo Object.
   */
  public static PartitionInfo addTo(PartitionInfo left, PartitionInfo right) {
    Resource leftResource = Resource.newInstance(0, 0);
    if (left != null && left.getResourceAvailable() != null) {
      ResourceInfo leftResourceInfo = left.getResourceAvailable();
      leftResource = leftResourceInfo.getResource();
    }

    Resource rightResource = Resource.newInstance(0, 0);
    if (right != null && right.getResourceAvailable() != null) {
      ResourceInfo rightResourceInfo = right.getResourceAvailable();
      rightResource = rightResourceInfo.getResource();
    }

    Resource resource = Resources.addTo(leftResource, rightResource);
    return new PartitionInfo(new ResourceInfo(resource));
  }
}
