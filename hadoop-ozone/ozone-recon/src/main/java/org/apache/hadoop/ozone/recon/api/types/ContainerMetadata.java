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
package org.apache.hadoop.ozone.recon.api.types;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

/**
 * Metadata object that represents a Container.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class ContainerMetadata {

  @XmlElement(name = "ContainerId")
  private long containerId;

  @XmlElement(name = "UserBytes")
  private long usedBytes;

  @XmlElement(name = "NumberOfKeys")
  private long numberOfKeys;

  @XmlElement(name = "Owner")
  private String owner;

  public long getContainerId() {
    return containerId;
  }

  public void setContainerId(long containerId) {
    this.containerId = containerId;
  }

  public long getUsedBytes() {
    return usedBytes;
  }

  public void setUsedBytes(long usedBytes) {
    this.usedBytes = usedBytes;
  }

  public long getNumberOfKeys() {
    return numberOfKeys;
  }

  public void setNumberOfKeys(long numberOfKeys) {
    this.numberOfKeys = numberOfKeys;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }
}
