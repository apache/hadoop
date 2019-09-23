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

import org.apache.hadoop.yarn.api.records.ResourceOption;

/**
 * A JAXB representation of a {link ResourceOption}.
 */
@XmlRootElement(name = "resourceOption")
@XmlAccessorType(XmlAccessType.NONE)
public class ResourceOptionInfo {

  @XmlElement(name = "resource")
  private ResourceInfo resource = new ResourceInfo();
  @XmlElement(name = "overCommitTimeout")
  private int overCommitTimeout;

  /** Internal resource option for caching. */
  private ResourceOption resourceOption;


  public ResourceOptionInfo() {
  } // JAXB needs this

  public ResourceOptionInfo(ResourceOption resourceOption) {
    if (resourceOption != null) {
      this.resource = new ResourceInfo(resourceOption.getResource());
      this.overCommitTimeout = resourceOption.getOverCommitTimeout();
    }
  }

  public ResourceOption getResourceOption() {
    if (resourceOption == null) {
      resourceOption = ResourceOption.newInstance(
          resource.getResource(), overCommitTimeout);
    }
    return resourceOption;
  }

  @Override
  public String toString() {
    return getResourceOption().toString();
  }
}
