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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.util.Records;

/**
 * An object that encapsulates an updated container and the
 * type of Update.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class UpdatedContainer {

  /**
   * Static Factory method.
   *
   * @param updateType ContainerUpdateType
   * @param container Container
   * @return UpdatedContainer
   */
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public static UpdatedContainer newInstance(ContainerUpdateType updateType,
      Container container) {
    UpdatedContainer updatedContainer =
        Records.newRecord(UpdatedContainer.class);
    updatedContainer.setUpdateType(updateType);
    updatedContainer.setContainer(container);
    return updatedContainer;
  }

  /**
   * Get the <code>ContainerUpdateType</code>.
   * @return ContainerUpdateType
   */
  public abstract ContainerUpdateType getUpdateType();

  /**
   * Set the <code>ContainerUpdateType</code>.
   * @param updateType ContainerUpdateType
   */
  public abstract void setUpdateType(ContainerUpdateType updateType);

  /**
   * Get the <code>Container</code>.
   * @return Container
   */
  public abstract Container getContainer();

  /**
   * Set the <code>Container</code>.
   * @param container Container
   */
  public  abstract void setContainer(Container container);

  @Override
  public int hashCode() {
    final int prime = 2153;
    int result = 2459;
    ContainerUpdateType updateType = getUpdateType();
    Container container = getContainer();
    result = prime * result + ((updateType == null) ? 0 :
        updateType.hashCode());
    result = prime * result + ((container == null) ? 0 : container.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    UpdatedContainer other = (UpdatedContainer) obj;
    ContainerUpdateType updateType = getUpdateType();
    if (updateType == null) {
      if (other.getUpdateType() != null) {
        return false;
      }
    } else if (updateType != other.getUpdateType()) {
      return false;
    }
    Container container = getContainer();
    if (container == null) {
      if (other.getContainer() != null) {
        return false;
      }
    } else if (!container.equals(other.getContainer())) {
      return false;
    }
    return true;
  }

}
