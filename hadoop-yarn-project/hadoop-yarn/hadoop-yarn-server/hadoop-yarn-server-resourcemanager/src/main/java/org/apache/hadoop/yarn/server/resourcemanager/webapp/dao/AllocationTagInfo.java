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

/**
 * DAO object to display node allocation tag.
 */
@XmlRootElement(name = "allocationTagInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class AllocationTagInfo {

  private String allocationTag;
  private long allocationsCount;

  public AllocationTagInfo() {
    // JAXB needs this
  }

  public AllocationTagInfo(String tag, long count) {
    this.allocationTag = tag;
    this.allocationsCount = count;
  }

  public String getAllocationTag() {
    return this.allocationTag;
  }

  public long getAllocationsCount() {
    return this.allocationsCount;
  }

  @Override
  public String toString() {
    return allocationTag + "(" + allocationsCount + ")";
  }
}
