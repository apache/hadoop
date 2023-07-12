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

package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * This class represent a sub-cluster identifier in the JSON representation
 * of the policy configuration.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@XmlRootElement(name = "federation-policy")
@XmlAccessorType(XmlAccessType.FIELD)
public class SubClusterIdInfo {

  private String id;

  public SubClusterIdInfo() {
    //JAXB needs this
  }

  public SubClusterIdInfo(String subClusterId) {
    this.id = subClusterId;
  }

  public SubClusterIdInfo(SubClusterId subClusterId) {
    this.id = subClusterId.getId();
  }

  /**
   * Get the sub-cluster identifier as {@link SubClusterId}.
   * @return the sub-cluster id.
   */
  public SubClusterId toId() {
    return SubClusterId.newInstance(id);
  }

  @Override
  public boolean equals(Object obj) {

    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (obj instanceof SubClusterIdInfo) {
      SubClusterIdInfo other = (SubClusterIdInfo) obj;
      return new EqualsBuilder()
          .append(this.id, other.id)
          .isEquals();
    }

    return false;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(this.id).toHashCode();
  }

  @Override
  public String toString() {
    return "SubClusterIdInfo{ id='" + id + '\'' + '}';
  }
}
