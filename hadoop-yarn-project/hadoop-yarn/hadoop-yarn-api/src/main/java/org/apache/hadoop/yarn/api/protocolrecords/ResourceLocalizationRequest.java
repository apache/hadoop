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

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/**
 * The request sent by the ApplicationMaster to ask for localizing resources.
 */
@Public
@Unstable
public abstract class ResourceLocalizationRequest {

  @Public
  @Unstable
  public static ResourceLocalizationRequest newInstance(ContainerId containerId,
      Map<String, LocalResource> localResources) {
    ResourceLocalizationRequest record =
        Records.newRecord(ResourceLocalizationRequest.class);
    record.setContainerId(containerId);
    record.setLocalResources(localResources);
    return record;
  }

  /**
   * Get the <code>ContainerId</code> of the container to localize resources.
   *
   * @return <code>ContainerId</code> of the container to localize resources.
   */
  @Public
  @Unstable
  public abstract ContainerId getContainerId();

  /**
   * Set the <code>ContainerId</code> of the container to localize resources.
   * @param containerId the containerId of the container.
   */
  @Private
  @Unstable
  public abstract void setContainerId(ContainerId containerId);

  /**
   * Get <code>LocalResource</code> required by the container.
   *
   * @return all <code>LocalResource</code> required by the container
   */
  @Public
  @Unstable
  public abstract Map<String, LocalResource> getLocalResources();

  /**
   * Set <code>LocalResource</code> required by the container. All pre-existing
   * Map entries are cleared before adding the new Map
   *
   * @param localResources <code>LocalResource</code> required by the container
   */
  @Private
  @Unstable
  public abstract void setLocalResources(
      Map<String, LocalResource> localResources);
}
